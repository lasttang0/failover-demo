from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy import text
from loguru import logger

from app.failover import failover_manager


@asynccontextmanager
async def lifespan(_app: FastAPI):
    failover_manager.initialize()
    await failover_manager.select_master_at_startup()
    await failover_manager.test_connections()
    logger.info("App started")

    try:
        yield
    finally:
        await failover_manager.close()
        logger.info("App stopped")


app = FastAPI(title="Mini Failover Demo", lifespan=lifespan)


@app.get("/db/ping")
async def db_ping() -> dict:
    async with await failover_manager.get_session() as session:
        res = await session.execute(text("SELECT 1"))
        return {"ok": True, "result": res.scalar()}


@app.get("/db/role")
async def db_role() -> dict:
    async with await failover_manager.get_session() as session:
        res = await session.execute(text("SELECT pg_is_in_recovery()"))
        in_recovery = bool(res.scalar())
        return {
            "ok": True,
            "role": "replica" if in_recovery else "master",
        }
