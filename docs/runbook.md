# Эксплуатация CatalogForge

## Запуск

```bash
docker compose up --build -d --wait --wait-timeout 180
docker compose ps
curl -fsS http://localhost:8130/health
docker compose logs --tail=100 api worker
```

Публичный локальный порт — 8130, привязан к 127.0.0.1. PostgreSQL не публикует
порт на хост. `/health` показывает возраст heartbeat импортёра. `/metrics`
содержит HTTP-счётчики и задержки с шаблонами маршрутов. Логи включают request ID.

При холодном старте миграции и seed выполняются отдельным сервисом до API/worker.
Роли и пароли в Compose демонстрационные, перед внешним развёртыванием их нужно
заменить и настроить TLS, резервные копии и ограничение входящих запросов.

## Тесты и демонстрация

```bash
uv sync --extra dev --frozen
uv run ruff check .
uv run ruff format --check .
docker compose --profile test build test
docker compose --profile test run --rm test
docker compose exec -T api python scripts/smoke.py
uv run python scripts/recovery_smoke.py --rows 1000000
```

Тесты используют отдельный PostgreSQL `catalogforge_test` и отдельную
директорию `/tmp/catalogforge-tests`. Перед очисткой проверяются TESTING,
имя БД и точный путь. Обычная БД и загруженные через API исходники не затрагиваются.
После проверки тестовую БД можно остановить:

```bash
docker compose --profile test stop test-db
```

Recovery script временно останавливает и перезапускает только worker текущего
проекта. Его запуск нужно планировать как отдельную демонстрацию, поскольку
другие импорты этого локального стенда тоже будут ожидать восстановления worker.

## Состояния и действия

| Состояние | Действие |
|---|---|
| awaiting_upload | Загрузить исходник с SHA-256 |
| uploading | Дождаться завершения; при обрыве повторить после lease |
| ready / running | Читать прогресс; при необходимости pause/cancel |
| paused | Resume продолжает с checkpoint |
| failed | Посмотреть error и /errors; исправленный файл отправить новым импортом |
| succeeded | Проверить счётчики и каталог |
| cancelled | Каталог не был изменён этим импортом |

При аварийном падении worker обычный `docker compose up -d --no-deps worker`
возвращает процесс. Через LEASE_SECONDS задание снова станет доступным.
Не переводить состояния и счётчики вручную SQL-командами: это нарушает
связь checkpoint и staging.

## Данные и место на диске

Исходник, staging, ошибки и история сохраняются для проверки. Удалить
конкретный завершённый импорт можно через `DELETE /imports/{id}`.
Обычный `docker compose down` сохраняет тома. Команда с `--volumes` удаляет
все данные проекта и для повседневной остановки не нужна.

Для перехода на большие рабочие объёмы следующими задачами будут retention,
квоты на диск, объектное хранилище и оценка продолжительности финального merge.
Схема сейчас ориентирована на проверяемый локальный сервис и воспроизводимый
миллион строк, без заявления о коммерческом production throughput.
