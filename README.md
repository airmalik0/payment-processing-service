# Асинхронный сервис процессинга платежей

Микросервис принимает платежи по HTTP, обрабатывает их асинхронно через
эмуляцию платёжного шлюза и уведомляет клиента о результате через webhook.

Ключевое свойство: приём и обработка платежа разнесены во времени. HTTP-запрос
завершается в момент, когда платёж надёжно сохранён и поставлен в очередь
(`202 Accepted`), а не когда шлюз вернул результат.

**Стек:** FastAPI · Pydantic v2 · SQLAlchemy 2.0 (async) · PostgreSQL ·
RabbitMQ (FastStream) · Alembic · Docker Compose.

Проектное решение и обоснование архитектурных развилок — в
[`docs/design.md`](docs/design.md).

## Что реализовано

| Требование ТЗ | Где |
|---|---|
| Модели и миграции `payments`, `outbox` | `src/payments/db/`, Alembic |
| API создания и получения платежа | `src/payments/api/` |
| Один consumer, делающий всё | `src/payments/broker/consumer.py` |
| Outbox pattern — гарантированная доставка событий | `src/payments/outbox/relay.py` |
| Retry: 3 попытки с экспоненциальной задержкой (1с → 5с → 25с) | `src/payments/broker/` |
| Dead Letter Queue для окончательно упавших сообщений | `src/payments/broker/topology.py` |
| Идемпотентность (вход и обработка) | `payment_service.py`, `processor.py` |
| Docker Compose: postgres, rabbitmq, api, consumer, relay | `docker-compose.yml` |

Сверх минимума: идемпотентность самой обработки (шлюз не вызывается дважды),
publisher confirms, классификация ошибок transient/permanent, HMAC-подпись
webhook, аутентификация по `X-API-Key`, health/readiness, структурные JSON-логи,
92 теста и демонстрационный приёмник webhook.

## Архитектура

```
   POST /api/v1/payments
          │  одна транзакция: INSERT payment + INSERT outbox_message
          ▼
   ┌──────────────┐      ┌───────────────┐      ┌──────────────────────┐
   │  PostgreSQL  │◄─────│  outbox-relay │─────►│  RabbitMQ             │
   │ payments     │      │ SKIP LOCKED   │ pub  │  exchange: payments  │
   │ outbox       │      │ + confirms    │      │  queue: payments.new │
   └──────┬───────┘      └───────────────┘      └──────────┬───────────┘
          │                                                 │
          │                                    ┌────────────▼───────────┐
          │                                    │       consumer         │
          │   UPDATE status, webhook           │  эмуляция шлюза (2-5с)  │
          └────────────────────────────────────┤  webhook + retry/DLQ   │
                                                └────────────┬───────────┘
                                            успех/провал      │  webhook
                                                              ▼
                                                    ┌──────────────────┐
                                                    │  webhook_url      │
                                                    │  клиента          │
                                                    └──────────────────┘

   retry:  payments.new ──ошибка──► payments.retry ──TTL 1/5/25с──► payments.new
   DLQ:    после 3 попыток или неустранимой ошибки ──► payments.new.dlq
```

Три независимых процесса. Каждый переживает недоступность соседей: API
принимает платежи при мёртвом брокере (события копятся в outbox), consumer
обрабатывает при мёртвом webhook (сообщения уходят в retry).

## Запуск

Нужен только Docker с Compose v2.

```bash
cp .env.example .env
docker compose up -d --build
```

Поднимутся: PostgreSQL, RabbitMQ, миграции (one-shot), `api`, `consumer`,
`outbox-relay` и `webhook-sink` (демонстрационный приёмник webhook).

Проверить готовность:

```bash
curl localhost:8000/health          # {"status":"ok"}
curl localhost:8000/health/ready     # {"status":"ok","database":"up"}
```

* Swagger UI — <http://localhost:8000/docs>
* RabbitMQ Management — <http://localhost:15672> (payments / payments)

## Примеры

Все запросы к `/api/v1/*` требуют заголовок `X-API-Key` (по умолчанию
`local-dev-api-key`), создание платежа — ещё и `Idempotency-Key`.

### Создать платёж

```bash
curl -X POST localhost:8000/api/v1/payments \
  -H "X-API-Key: local-dev-api-key" \
  -H "Idempotency-Key: order-42" \
  -H "Content-Type: application/json" \
  -d '{
    "amount": "100.50",
    "currency": "RUB",
    "description": "Оплата заказа #42",
    "metadata": {"order_id": "42"},
    "webhook_url": "http://webhook-sink:9000/"
  }'
```

```json
{ "payment_id": "…", "status": "pending", "created_at": "…" }
```

Ответ — `202 Accepted`: платёж принят и поставлен в очередь. Через 2–5 секунд
consumer доведёт его до `succeeded` (90%) или `failed` (10%) и вызовет webhook.

### Получить платёж

```bash
curl localhost:8000/api/v1/payments/<payment_id> \
  -H "X-API-Key: local-dev-api-key"
```

Возвращает полное состояние, включая `status`, `failure_reason` и статус
доставки webhook (`webhook_delivered_at`, `webhook_attempts`,
`webhook_last_error`).

### Посмотреть доставленные webhook

`webhook-sink` печатает каждое уведомление в лог:

```bash
docker compose logs -f webhook-sink
```

Уведомление несёт заголовки `X-Webhook-Event-Id` (стабильный ID для
дедупликации на стороне клиента) и `X-Webhook-Signature`
(HMAC-SHA256 тела — для аутентификации отправителя).

### Идемпотентность

```bash
# Повтор с тем же ключом и телом → тот же платёж, заголовок Idempotent-Replay: true
curl -i -X POST localhost:8000/api/v1/payments \
  -H "X-API-Key: local-dev-api-key" -H "Idempotency-Key: order-42" \
  -H "Content-Type: application/json" \
  -d '{"amount":"100.50","currency":"RUB","description":"Оплата заказа #42","metadata":{"order_id":"42"},"webhook_url":"http://webhook-sink:9000/"}'

# Тот же ключ, другое тело → 409 Conflict
```

### Проверить retry и DLQ вручную

`webhook-sink` умеет отвечать заданным кодом через query-параметр:

```bash
# webhook всегда отвечает 500 → 3 попытки (1с, 5с, 25с) → DLQ
curl -X POST localhost:8000/api/v1/payments \
  -H "X-API-Key: local-dev-api-key" -H "Idempotency-Key: retry-demo" \
  -H "Content-Type: application/json" \
  -d '{"amount":"77.00","currency":"EUR","description":"retry demo","webhook_url":"http://webhook-sink:9000/?status=500"}'

# следим за очередями: сообщение проходит retry.1 → retry.2 → retry.3 → dlq
watch -n1 'docker compose exec -T rabbitmq rabbitmqctl list_queues name messages'

# webhook отвечает 404 (неустранимо) → сразу в DLQ, без повторов
#   …"webhook_url":"http://webhook-sink:9000/?status=404"
```

### Устойчивость к недоступности брокера (Outbox pattern)

```bash
docker compose stop rabbitmq
# Платёж всё равно принимается (202): событие оседает в outbox
curl -X POST localhost:8000/api/v1/payments \
  -H "X-API-Key: local-dev-api-key" -H "Idempotency-Key: resilience" \
  -H "Content-Type: application/json" \
  -d '{"amount":"55.00","currency":"EUR","description":"broker down"}'

docker compose start rabbitmq
# relay сам опубликует накопленное, consumer обработает — ничего не потеряно
```

## API

| Метод | Путь | Назначение | Коды |
|---|---|---|---|
| `POST` | `/api/v1/payments` | Создать платёж | 202, 400, 401, 409, 422 |
| `GET` | `/api/v1/payments/{id}` | Получить платёж | 200, 401, 404 |
| `GET` | `/health` | Liveness | 200 |
| `GET` | `/health/ready` | Readiness (нужна БД) | 200, 503 |

## Разработка

```bash
uv venv && uv pip install -e ".[dev]"      # окружение

docker compose up -d postgres rabbitmq      # зависимости для тестов
uv run ruff check . && uv run ruff format --check .
uv run mypy src tests                        # строгая типизация
uv run pytest                                # 91 тест (unit + integration; e2e исключён)
uv run pytest -m e2e                         # e2e на живом RabbitMQ (нужен весь стек)
```

Тесты по слоям:

* **unit** — отпечаток запроса, backoff, эмулятор шлюза, классификация ошибок,
  схемы. Без инфраструктуры.
* **integration** — API, идемпотентность (включая гонку ключей),
  атомарность outbox, идемпотентность обработки, relay с `SKIP LOCKED`,
  маршрутизация retry/DLQ. Нужен PostgreSQL.
* **e2e** — обработка события через настоящий RabbitMQ. Нужен полный стек.

БД в тестах изолируется внешней транзакцией на каждый тест (savepoint-режим):
изменения откатываются, схема не пересоздаётся.

## Структура

```
src/payments/
├── api/            HTTP-слой: роуты, схемы, зависимости, аутентификация
├── broker/         RabbitMQ: топология, consumer, маршрутизация retry/DLQ
├── outbox/         relay — публикация событий из БД в брокер
├── services/       бизнес-логика: приём платежа, обработка, webhook
├── domain/         модель предметной области: enums, ошибки, шлюз, события
├── db/             модели, сессии, репозитории, миграции Alembic
├── observability/  структурное логирование
├── entrypoints/    точки входа процессов: api, consumer, relay
└── config.py       конфигурация из окружения
```

## Конфигурация

Все параметры — в `.env` (см. `.env.example`). Основное:

| Переменная | Назначение | По умолчанию |
|---|---|---|
| `API_KEY` | Ключ для `X-API-Key` | `local-dev-api-key` |
| `DATABASE_URL` | Строка подключения (только `postgresql+asyncpg`) | — |
| `RABBITMQ_URL` | Строка подключения к брокеру | — |
| `GATEWAY_SUCCESS_RATE` | Доля успешных платежей у эмулятора | `0.9` |
| `MAX_RETRIES` | Число повторов до DLQ | `3` |
| `RETRY_BASE_DELAY_SECONDS` / `RETRY_MULTIPLIER` | База и множитель задержки | `1.0` / `5.0` |
| `WEBHOOK_SIGNING_SECRET` | Секрет HMAC-подписи webhook | — |
