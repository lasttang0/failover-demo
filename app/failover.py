import asyncio
import time
from typing import Optional

from loguru import logger
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine, async_sessionmaker, AsyncSession
from yarl import URL

from app.settings import settings


class AsyncFailoverManager:
    """Async failover manager that tries primary first, then secondary"""

    _instance: Optional["AsyncFailoverManager"] = None

    # Singleton
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            instance = super().__new__(cls)
            cls._instance = instance
        return cls._instance

    def __init__(self):
        if hasattr(self, "_initialized") and self._initialized:
            return
        self._initialized = True

        self.primary_engine: Optional[AsyncEngine] = None
        self.secondary_engine: Optional[AsyncEngine] = None
        self.current_engine: Optional[AsyncEngine] = None
        self.SessionLocal = None
        self._health_check_interval = settings.db_host_health_check_interval_in_seconds
        self._last_health_check = 0
        self._is_healthy = True
        self._split_brain_detected = False
        # protection from racing and flapping
        self._lock = asyncio.Lock()
        self._last_switch_time = 0.0
        self._switch_cooldown = settings.db_host_cooldown_interval_in_seconds

    @property
    def health_check_interval(self) -> int:
        return self._health_check_interval

    @staticmethod
    def _make_engine(url) -> AsyncEngine:
        pool_size = settings.db_pool_size  # starting pool size
        max_overflow = settings.db_max_overflow  # splashes
        pool_timeout = settings.db_pool_timeout_in_seconds  # waiting for a free connection (sec)
        pool_recycle = settings.db_pool_recycle_in_seconds  # compound recycling (sec)
        app_name = settings.app_name
        stmt_timeout = settings.postgres_statement_timeout_str  # limit the duration of one SQL

        return create_async_engine(
            str(url),
            echo=settings.db_echo,
            future=True,
            pool_pre_ping=True,
            pool_use_lifo=True,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_recycle=pool_recycle,
            connect_args={
                "timeout": settings.tcp_connect_timeout_in_seconds,  # connection establishment timeout (sec)
                "server_settings": {
                    "application_name": app_name,
                    "statement_timeout": stmt_timeout,
                }
            },
        )

    def initialize(self):
        """Initialize both primary and secondary engines"""
        logger.info("Initializing async failover manager")

        def _build_db_url(*, host: str, port: int) -> URL:
            return URL.build(
                scheme=settings.db_driver,
                host=host,
                port=port,
                user=settings.postgres_user,
                password=settings.postgres_password.get_secret_value(),
                path=f"/{settings.postgres_db}",
            )

        # Create primary engine
        primary_url = _build_db_url(
            host=settings.db_primary_host.get_secret_value(),
            port=settings.db_primary_port
        )
        logger.info(f"Creating primary async engine with URL: {primary_url}")
        self.primary_engine = self._make_engine(primary_url)

        # Create secondary engine
        secondary_url = _build_db_url(
            host=settings.db_secondary_host.get_secret_value(),
            port=settings.db_secondary_port
        )
        logger.info(f"Creating secondary async engine with URL: {secondary_url}")
        self.secondary_engine = self._make_engine(secondary_url)

        # Start with primary
        self.current_engine = self.primary_engine
        self.SessionLocal = async_sessionmaker(
            self.current_engine,
            expire_on_commit=False
        )

        logger.info(f"Primary DB: {primary_url}")
        logger.info(f"Secondary DB: {secondary_url}")
        logger.info("Async failover manager initialized")

    async def get_engine(self) -> AsyncEngine:
        if self.current_engine is None:
            raise RuntimeError("Failover manager not initialized")
        async with self._lock:
            if await self._is_current_healthy():
                return self.current_engine
            logger.warning("Current engine failed, switching to alternative")
            return await self._switch_engine()

    async def _is_current_healthy(self) -> bool:
        """Check if the current engine is healthy and is a master (not replica)"""
        current_time = time.time()
        if current_time - self._last_health_check < self._health_check_interval:
            return self._is_healthy

        try:
            async with self.current_engine.connect() as conn:
                # check connection
                await conn.execute(text("SELECT 1"))

                # check if replica
                result = await conn.execute(text("SELECT pg_is_in_recovery()"))
                in_recovery = result.scalar()
                if in_recovery:
                    raise Exception("Current DB is in recovery mode (replica)")

            await self.check_split_brain_scenario()

            self._is_healthy = True
        except Exception as e:
            logger.warning(f"Current engine health check failed: {e}")
            self._is_healthy = False

        self._last_health_check = current_time
        return self._is_healthy

    @staticmethod
    async def _engine_is_writable(engine: AsyncEngine) -> bool:
        # noinspection PyBroadException
        try:
            async with engine.connect() as conn:
                res = await conn.execute(text("SELECT pg_is_in_recovery()"))
                return not bool(res.scalar())
        except Exception:
            return False

    async def _switch_engine(self) -> AsyncEngine:
        """Switch to the alternative engine"""
        if self._split_brain_detected:
            logger.critical("Split-brain detected, refusing to switch engine!")
            raise RuntimeError("Cannot safely switch engine due to replication conflict")

        now = time.time()
        if now - self._last_switch_time < self._switch_cooldown:
            logger.info("Switch suppressed by cooldown")
            return self.current_engine

        target = self.secondary_engine if self.current_engine == self.primary_engine else self.primary_engine
        if not target:
            logger.warning("Alternative engine not configured. Staying on current.")
            self._is_healthy = False
            return self.current_engine

        # switch only to a truly writable instance
        if not await self._engine_is_writable(target):
            logger.warning("Alternative engine is not writable. Staying on current.")
            self._is_healthy = False
            return self.current_engine

        logger.info("Switching engine")
        self.current_engine = target

        self.SessionLocal = async_sessionmaker(
            self.current_engine,
            expire_on_commit=False
        )

        # Reset health check
        self._is_healthy = True
        self._last_health_check = time.time()
        self._last_switch_time = now

        return self.current_engine

    async def get_session(self) -> AsyncSession:
        """Get a database session with failover support"""
        engine = await self.get_engine()
        if self.SessionLocal is None or self.current_engine is not engine:
            self.SessionLocal = async_sessionmaker(bind=engine, expire_on_commit=False)

        return self.SessionLocal()

    async def check_split_brain_scenario(self):
        """Check for dangerous replication configuration (e.g., two masters or two replicas)"""

        async def get_role(name: str, engine: AsyncEngine) -> Optional[bool]:
            try:
                async with engine.connect() as conn:
                    result = await conn.execute(text("SELECT pg_is_in_recovery()"))
                    return result.scalar()
            except Exception as e:
                logger.warning(f"Failed to determine role of {name}: {e}")
                return None

        primary_in_recovery = await get_role("Primary", self.primary_engine)
        secondary_in_recovery = await get_role("Secondary", self.secondary_engine)

        if primary_in_recovery is None or secondary_in_recovery is None:
            logger.warning("Unable to determine full replication state")
            return

        if primary_in_recovery == secondary_in_recovery:
            if primary_in_recovery:
                logger.critical("x Both databases are in REPLICA mode — no master available!")
            else:
                logger.critical("x Both databases are in MASTER mode — potential split-brain!")
            self._split_brain_detected = True
        else:
            # logger.debug("Replication roles verified: one master, one replica.")
            self._split_brain_detected = False

    async def test_connections(self):
        """Test both connections and log status"""
        logger.info("Testing database connections...")

        # Helper to check connection and role
        async def check_connection(name: str, engine: AsyncEngine):
            try:
                async with engine.connect() as conn:
                    await conn.execute(text("SELECT 1"))
                    result = await conn.execute(text("SELECT pg_is_in_recovery()"))
                    in_recovery = result.scalar()
                    if in_recovery:
                        logger.info(f"✓ {name} database is REPLICA")
                    else:
                        logger.info(f"✓ {name} database is MASTER")
            except Exception as e:
                logger.warning(f"✗ {name} database connection failed: {e}")

        await check_connection("Primary", self.primary_engine)
        await check_connection("Secondary", self.secondary_engine)
        await self.check_split_brain_scenario()

    async def select_master_at_startup(self) -> None:
        """
        At the start, select a truly writable engine and rebuild SessionLocal.
        """
        async with self._lock:
            # if primary is master, use it
            if self.primary_engine and await self._engine_is_writable(self.primary_engine):
                chosen = self.primary_engine
            # otherwise try secondary
            elif self.secondary_engine and await self._engine_is_writable(self.secondary_engine):
                chosen = self.secondary_engine
            else:
                logger.warning("No writable database found at startup; staying on current (may be read-only).")
                return

            if chosen is not self.current_engine:
                self.current_engine = chosen
                self.SessionLocal = async_sessionmaker(self.current_engine, expire_on_commit=False)
                self._is_healthy = True
                self._last_health_check = time.time()
                self._last_switch_time = time.time()
                logger.info("Selected writable DB engine at startup.")

    async def wait_until_writable(self, timeout: float = None) -> None:
        """
        Blocking wait until the current engine becomes writable (not in recovery).
        """
        limit = time.monotonic() + (timeout or settings.db_host_writable_timeout_in_seconds)
        while True:
            async with self._lock:
                if await self._is_current_healthy():
                    return
                # try to switch
                await self._switch_engine()
            if time.monotonic() > limit:
                raise TimeoutError("Writable DB engine not available within startup timeout")
            await asyncio.sleep(0.5)

    async def close(self):
        """Close all database connections"""
        if self.primary_engine:
            await self.primary_engine.dispose()
        if self.secondary_engine:
            await self.secondary_engine.dispose()
        logger.info("Database connections closed")


# Global instance
failover_manager = AsyncFailoverManager()
