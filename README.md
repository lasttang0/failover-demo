# Mini Failover Demo

Небольшой demo‑проект для демонстрации failover между двумя PostgreSQL узлами.
API написано на FastAPI + SQLAlchemy (async).

Что делает проект:
- Проверяет доступность БД
- Определяет активный узел
- Переключается при недоступности primary

Запуск (одной командой):
1. docker compose up -d --build

После запуска:
- API: http://localhost:8000
- Swagger: http://localhost:8000/docs

Эндпоинты:

GET /db/ping - проверка подключения

GET /db/role - текущая активная БД
