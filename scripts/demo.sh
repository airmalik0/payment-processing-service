#!/usr/bin/env bash
# Сквозная демонстрация сервиса: проходит по всем пунктам ТЗ и проверяет их
# на живом стенде. Каждая проверка печатает результат, в конце — сводка.
#
#   ./scripts/demo.sh            обычный прогон (~2 минуты)
#   ./scripts/demo.sh --full     плюс долгий сценарий «3 попытки → DLQ» (+40 секунд)
#
# Требуется только curl и запущенный стек (`make up`).
set -uo pipefail

API="${API:-http://localhost:8000}"
SINK_HOST="${SINK_HOST:-http://localhost:9000}"
# Адрес приёмника изнутри docker-сети: его видит consumer, отправляя webhook.
SINK_INTERNAL="${SINK_INTERNAL:-http://webhook-sink:9000}"
API_KEY="${API_KEY:-local-dev-api-key}"
COMPOSE="${COMPOSE:-docker compose}"
FULL=false
if [ "${1:-}" = "--full" ]; then
  FULL=true
fi

if [ -t 1 ]; then
  BOLD=$'\033[1m'; DIM=$'\033[2m'; GREEN=$'\033[32m'
  RED=$'\033[31m'; CYAN=$'\033[36m'; RESET=$'\033[0m'
else
  BOLD=""; DIM=""; GREEN=""; RED=""; CYAN=""; RESET=""
fi

PASSED=0
FAILED=0
RUN_ID="demo-$(date +%s)-$$"

# ---------- вспомогательное ----------

section() {
  printf '\n%s━━━ %s ━━━%s\n' "$BOLD$CYAN" "$1" "$RESET"
}

ok() {
  PASSED=$((PASSED + 1))
  printf '  %s✓%s %s\n' "$GREEN" "$RESET" "$1"
}

fail() {
  FAILED=$((FAILED + 1))
  printf '  %s✗%s %s\n' "$RED" "$RESET" "$1"
}

note() {
  printf '    %s%s%s\n' "$DIM" "$1" "$RESET"
}

check() { # check "описание" ожидаемое фактическое
  if [ "$2" = "$3" ]; then
    ok "$1 → $3"
  else
    fail "$1 → ожидалось $2, получено $3"
  fi
}

# Разбор JSON. python3 есть почти везде; если нет — берём интерпретатор из
# контейнера api, чтобы у скрипта не было локальных зависимостей вовсе.
if command -v python3 >/dev/null 2>&1; then
  json() { python3 -c '
import json, sys
data = json.loads(sys.argv[1] or "{}")
for key in sys.argv[2].split("."):
    if isinstance(data, list):
        data = data[int(key)] if data else None
    elif isinstance(data, dict):
        data = data.get(key)
    if data is None:
        break
print("" if data is None else (json.dumps(data, ensure_ascii=False) if isinstance(data, (dict, list)) else data))
' "$1" "$2" 2>/dev/null; }
  json_len() { python3 -c 'import json,sys; print(len(json.loads(sys.argv[1] or "[]")))' "$1" 2>/dev/null || echo 0; }
else
  json() { $COMPOSE exec -T api python -c '
import json, sys
data = json.loads(sys.argv[1] or "{}")
for key in sys.argv[2].split("."):
    if isinstance(data, list):
        data = data[int(key)] if data else None
    elif isinstance(data, dict):
        data = data.get(key)
    if data is None:
        break
print("" if data is None else (json.dumps(data, ensure_ascii=False) if isinstance(data, (dict, list)) else data))
' "$1" "$2" 2>/dev/null; }
  json_len() { $COMPOSE exec -T api python -c 'import json,sys; print(len(json.loads(sys.argv[1] or "[]")))' "$1" 2>/dev/null || echo 0; }
fi

# Запрос к API. Код ответа кладёт в CODE, тело — в RESPONSE_BODY.
request() { # request METHOD path [json-body] [доп. заголовки...]
  local method="$1" path="$2" body="${3:-}"
  shift 2
  if [ $# -gt 0 ]; then
    shift
  fi
  local out; out=$(mktemp)
  local args=(-s -o "$out" -w '%{http_code}' -X "$method" "$API$path" -H "X-API-Key: $API_KEY")
  if [ -n "$body" ]; then
    args+=(-H "Content-Type: application/json" -d "$body")
  fi
  local header
  for header in "$@"; do args+=(-H "$header"); done
  CODE=$(curl "${args[@]}")
  RESPONSE_BODY=$(cat "$out")
  rm -f "$out"
}

create_payment() { # create_payment idempotency-key json-body
  request POST /api/v1/payments "$2" "Idempotency-Key: $1"
}

# Четвёртый аргумент — путь/query приёмника webhook. Не передан — платёж без
# webhook_url; пустая строка — уведомление на корень приёмника.
payment_body() { # payment_body amount currency description [webhook_suffix]
  local webhook=""
  if [ "$#" -ge 4 ]; then
    webhook=", \"webhook_url\": \"$SINK_INTERNAL/$4\""
  fi
  printf '{"amount":"%s","currency":"%s","description":"%s","metadata":{"demo":"%s"}%s}' \
    "$1" "$2" "$3" "$RUN_ID" "$webhook"
}

get_payment() { # get_payment id → печатает тело
  curl -s "$API/api/v1/payments/$1" -H "X-API-Key: $API_KEY"
}

wait_for() { # wait_for id timeout поле → ждёт непустое поле, печатает тело
  local id="$1" timeout="$2" field="$3" body="" value=""
  local deadline=$(( $(date +%s) + timeout ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    body=$(get_payment "$id")
    value=$(json "$body" "$field")
    if [ -n "$value" ] && [ "$value" != "pending" ] && [ "$value" != "null" ]; then
      printf '%s' "$body"
      return 0
    fi
    sleep 1
  done
  printf '%s' "$body"
  return 1
}

queue_depth() { # queue_depth имя-очереди
  $COMPOSE exec -T rabbitmq rabbitmqctl list_queues name messages -q --no-table-headers 2>/dev/null \
    | awk -v q="$1" '$1 == q {print $2}' | head -1
}

sink_records() { # sink_records payment_id
  curl -s "$SINK_HOST/received?payment_id=$1"
}

# ---------- 0. Готовность стека ----------

section "0/7 · Готовность стека"

request GET /health
check "GET /health" 200 "$CODE"
note "$RESPONSE_BODY"

request GET /health/ready
check "GET /health/ready (проверяет соединение с БД)" 200 "$CODE"
note "$RESPONSE_BODY"

if [ "$CODE" != "200" ]; then
  printf '\n%sСтенд не поднят. Запустите: make up%s\n\n' "$RED" "$RESET"
  exit 1
fi

curl -s -X DELETE "$SINK_HOST/received" >/dev/null 2>&1  # чистим журнал приёмника

# ---------- 1. Аутентификация ----------

section "1/7 · Аутентификация по X-API-Key"

CODE=$(curl -s -o /dev/null -w '%{http_code}' "$API/api/v1/payments/00000000-0000-0000-0000-000000000000")
check "Запрос без ключа" 401 "$CODE"

CODE=$(curl -s -o /dev/null -w '%{http_code}' "$API/api/v1/payments/00000000-0000-0000-0000-000000000000" \
  -H "X-API-Key: неверный-ключ")
check "Запрос с неверным ключом" 401 "$CODE"

request GET /api/v1/payments/00000000-0000-0000-0000-000000000000
check "Верный ключ, несуществующий платёж" 404 "$CODE"

# ---------- 2. Валидация тела ----------

section "2/7 · Валидация запроса"

request POST /api/v1/payments "$(payment_body 10.00 RUB 'без ключа идемпотентности')"
check "Создание без Idempotency-Key" 400 "$CODE"

create_payment "$RUN_ID-bad-amount" "$(payment_body -5.00 RUB 'отрицательная сумма')"
check "Отрицательная сумма" 422 "$CODE"

create_payment "$RUN_ID-bad-currency" "$(payment_body 10.00 GBP 'неизвестная валюта')"
check "Валюта вне списка RUB/USD/EUR" 422 "$CODE"

create_payment "$RUN_ID-subcent" "$(payment_body 10.005 RUB 'доля копейки')"
check "Сумма с точностью мельче копейки" 422 "$CODE"

# ---------- 3. Создание платежа и идемпотентность ----------

section "3/7 · Создание платежа и идемпотентность"

KEY="$RUN_ID-order-42"
BODY_MAIN=$(payment_body 100.50 RUB 'Оплата заказа #42' '')

REPLAY=$(curl -s -D /tmp/demo-headers.$$ -o /tmp/demo-body.$$ -w '%{http_code}' \
  -X POST "$API/api/v1/payments" -H "X-API-Key: $API_KEY" -H "Idempotency-Key: $KEY" \
  -H "Content-Type: application/json" -d "$BODY_MAIN")
FIRST_BODY=$(cat /tmp/demo-body.$$)
check "POST /api/v1/payments" 202 "$REPLAY"

PAYMENT_ID=$(json "$FIRST_BODY" payment_id)
STATUS=$(json "$FIRST_BODY" status)
check "Начальный статус" "pending" "$STATUS"
note "payment_id = $PAYMENT_ID"
note "$(grep -i 'idempotent-replay' /tmp/demo-headers.$$ | tr -d '\r')"

# Повтор с тем же ключом и телом — тот же платёж, без дубля.
curl -s -D /tmp/demo-headers2.$$ -o /tmp/demo-body2.$$ \
  -X POST "$API/api/v1/payments" -H "X-API-Key: $API_KEY" -H "Idempotency-Key: $KEY" \
  -H "Content-Type: application/json" -d "$BODY_MAIN" >/dev/null
REPEAT_ID=$(json "$(cat /tmp/demo-body2.$$)" payment_id)
check "Повтор с тем же ключом возвращает тот же платёж" "$PAYMENT_ID" "$REPEAT_ID"
if grep -qi 'idempotent-replay: true' /tmp/demo-headers2.$$; then
  ok "Заголовок Idempotent-Replay: true"
else
  fail "Ожидался заголовок Idempotent-Replay: true"
fi

# Тот же ключ, но другое тело — конфликт, а не молчаливая подмена.
create_payment "$KEY" "$(payment_body 999.00 USD 'другое тело с тем же ключом')"
check "Тот же ключ с другим телом" 409 "$CODE"

rm -f /tmp/demo-headers.$$ /tmp/demo-body.$$ /tmp/demo-headers2.$$ /tmp/demo-body2.$$

# ---------- 4. Асинхронная обработка и webhook ----------

section "4/7 · Асинхронная обработка и доставка webhook"

KEY_WH="$RUN_ID-webhook"
create_payment "$KEY_WH" "$(payment_body 250.00 EUR 'Платёж с webhook' '')"
WH_ID=$(json "$RESPONSE_BODY" payment_id)
printf '  %s…%s ждём обработчик (эмуляция шлюза занимает 2–5 секунд)\n' "$DIM" "$RESET"

PAYMENT=$(wait_for "$WH_ID" 40 status)
FINAL_STATUS=$(json "$PAYMENT" status)
if [ "$FINAL_STATUS" = "succeeded" ] || [ "$FINAL_STATUS" = "failed" ]; then
  ok "Платёж обработан асинхронно, статус: $FINAL_STATUS"
  if [ "$FINAL_STATUS" = "failed" ]; then
    note "failed — штатный исход: эмулятор шлюза отказывает в 10% случаев"
  fi
else
  fail "Платёж не дошёл до финального статуса за 40 секунд (статус: $FINAL_STATUS)"
fi

PROCESSED_AT=$(json "$PAYMENT" processed_at)
if [ -n "$PROCESSED_AT" ]; then
  ok "Проставлено время обработки: $PROCESSED_AT"
else
  fail "processed_at пуст"
fi

DELIVERED=$(json "$PAYMENT" webhook_delivered_at)
ATTEMPTS=$(json "$PAYMENT" webhook_attempts)
if [ -n "$DELIVERED" ]; then
  ok "Webhook доставлен с попытки №$ATTEMPTS"
else
  fail "Webhook не доставлен"
fi

RECORDS=$(sink_records "$WH_ID")
check "Приёмник получил уведомление" 1 "$(json_len "$RECORDS")"
check "HMAC-подпись webhook верна" "True" "$(json "$RECORDS" 0.signature_valid)"
check "Событие" "payment.$FINAL_STATUS" "$(json "$RECORDS" 0.event)"
note "event_id (ключ дедупликации на стороне клиента): $(json "$RECORDS" 0.event_id)"

# ---------- 5. Retry и DLQ ----------

section "5/7 · Повторные попытки и Dead Letter Queue"

DLQ_BEFORE=$(queue_depth payments.new.dlq)
DLQ_BEFORE=${DLQ_BEFORE:-0}

# 5a. Приёмник отвечает 503 на первые две доставки → сообщение уходит в
# retry-очереди с задержкой 1с и 5с и доставляется с третьей попытки.
create_payment "$RUN_ID-retry" "$(payment_body 77.00 EUR 'Проверка retry' '?fail=2')"
RETRY_ID=$(json "$RESPONSE_BODY" payment_id)
printf '  %s…%s webhook дважды отвечает 503; ждём повторы с задержкой 1с и 5с\n' "$DIM" "$RESET"

RETRY_PAYMENT=""
DEADLINE=$(( $(date +%s) + 70 ))
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  RETRY_PAYMENT=$(get_payment "$RETRY_ID")
  if [ -n "$(json "$RETRY_PAYMENT" webhook_delivered_at)" ]; then
    break
  fi
  sleep 2
done

RETRY_ATTEMPTS=$(json "$RETRY_PAYMENT" webhook_attempts)
if [ -n "$(json "$RETRY_PAYMENT" webhook_delivered_at)" ]; then
  ok "Webhook доставлен после повторов, всего попыток: $RETRY_ATTEMPTS"
  note "Задержка сделана очередями с x-message-ttl, а не sleep в обработчике"
else
  fail "Webhook так и не доставлен за 70 секунд (попыток: $RETRY_ATTEMPTS)"
fi
DELIVERIES=$(json_len "$(sink_records "$RETRY_ID")")
check "Приёмник видел все доставки (2 отказа + успех)" 3 "$DELIVERIES"

# 5b. Приёмник отвечает 404: повтор бессмысленен → сообщение сразу в DLQ.
create_payment "$RUN_ID-dlq" "$(payment_body 13.00 USD 'Неустранимая ошибка webhook' '?status=404')"
DLQ_ID=$(json "$RESPONSE_BODY" payment_id)
printf '  %s…%s webhook отвечает 404 (неустранимо) — ждём попадания в DLQ\n' "$DIM" "$RESET"

DEADLINE=$(( $(date +%s) + 40 ))
DLQ_AFTER="$DLQ_BEFORE"
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  DLQ_AFTER=$(queue_depth payments.new.dlq)
  DLQ_AFTER=${DLQ_AFTER:-0}
  if [ "$DLQ_AFTER" -gt "$DLQ_BEFORE" ]; then
    break
  fi
  sleep 2
done

if [ "$DLQ_AFTER" -gt "$DLQ_BEFORE" ]; then
  ok "Сообщение ушло в payments.new.dlq без лишних повторов (было $DLQ_BEFORE, стало $DLQ_AFTER)"
  note "Платёж при этом обработан: статус $(json "$(get_payment "$DLQ_ID")" status) — деньги не потеряны, не доставлено только уведомление"
else
  fail "Сообщение не появилось в DLQ за 40 секунд"
fi

if $FULL; then
  # 5c. Долгий сценарий: webhook всегда отвечает 500 → 3 попытки (1с, 5с, 25с) → DLQ.
  DLQ_BEFORE="$DLQ_AFTER"
  create_payment "$RUN_ID-exhausted" "$(payment_body 21.00 RUB 'Исчерпание попыток' '?status=500')"
  EXH_ID=$(json "$RESPONSE_BODY" payment_id)
  printf '  %s…%s webhook всегда отвечает 500; ждём 3 попытки (1с + 5с + 25с) и уход в DLQ\n' "$DIM" "$RESET"

  DEADLINE=$(( $(date +%s) + 120 ))
  while [ "$(date +%s)" -lt "$DEADLINE" ]; do
    DLQ_AFTER=$(queue_depth payments.new.dlq)
    DLQ_AFTER=${DLQ_AFTER:-0}
    if [ "$DLQ_AFTER" -gt "$DLQ_BEFORE" ]; then
      break
    fi
    sleep 3
  done
  EXH_ATTEMPTS=$(json "$(get_payment "$EXH_ID")" webhook_attempts)
  if [ "$DLQ_AFTER" -gt "$DLQ_BEFORE" ]; then
    ok "Попыток доставки: $EXH_ATTEMPTS (первичная + 3 повтора) — сообщение окончательно ушло в DLQ"
  else
    fail "Сообщение не дошло до DLQ за 120 секунд (попыток: $EXH_ATTEMPTS)"
  fi
fi

# ---------- 6. Outbox: платежи принимаются при мёртвом брокере ----------

section "6/7 · Outbox pattern: брокер лежит, платежи не теряются"

printf '  %s…%s останавливаю RabbitMQ\n' "$DIM" "$RESET"
$COMPOSE stop rabbitmq >/dev/null 2>&1

create_payment "$RUN_ID-outbox" "$(payment_body 55.00 EUR 'Платёж при мёртвом брокере' '')"
check "Платёж принят при недоступном брокере" 202 "$CODE"
OUTBOX_ID=$(json "$RESPONSE_BODY" payment_id)

UNPUBLISHED=$($COMPOSE exec -T postgres psql -U "${POSTGRES_USER:-payments}" -d "${POSTGRES_DB:-payments}" \
  -tAc "SELECT count(*) FROM outbox_messages WHERE published_at IS NULL" 2>/dev/null | tr -d '[:space:]')
if [ "${UNPUBLISHED:-0}" -ge 1 ]; then
  ok "Событие лежит в таблице outbox и ждёт публикации (неопубликованных: $UNPUBLISHED)"
  note "Платёж и событие записаны одной транзакцией — потерять одно без другого невозможно"
else
  fail "В outbox нет неопубликованного события"
fi

printf '  %s…%s поднимаю RabbitMQ обратно\n' "$DIM" "$RESET"
$COMPOSE start rabbitmq >/dev/null 2>&1

OUTBOX_PAYMENT=$(wait_for "$OUTBOX_ID" 90 status)
OUTBOX_STATUS=$(json "$OUTBOX_PAYMENT" status)
if [ "$OUTBOX_STATUS" = "succeeded" ] || [ "$OUTBOX_STATUS" = "failed" ]; then
  ok "После возвращения брокера relay опубликовал событие, платёж обработан: $OUTBOX_STATUS"
else
  fail "Платёж не обработался после возвращения брокера (статус: $OUTBOX_STATUS)"
fi

# ---------- 7. Итог ----------

section "7/7 · Итог"

printf '  Очереди RabbitMQ сейчас:\n'
$COMPOSE exec -T rabbitmq rabbitmqctl list_queues name messages -q --no-table-headers 2>/dev/null \
  | awk '{printf "    %-24s %s\n", $1, $2}'

printf '\n  %sПройдено проверок: %s%d%s' "$BOLD" "$GREEN" "$PASSED" "$RESET"
if [ "$FAILED" -gt 0 ]; then
  printf '  %sпровалено: %d%s\n' "$RED" "$FAILED" "$RESET"
else
  printf '  %sпровалов нет%s\n' "$DIM" "$RESET"
fi

printf '\n  %sЧто посмотреть дальше:%s\n' "$BOLD" "$RESET"
printf '    Swagger UI ............. %shttp://localhost:8000/docs%s\n' "$CYAN" "$RESET"
printf '    RabbitMQ Management .... %shttp://localhost:15672%s (payments / payments)\n' "$CYAN" "$RESET"
printf '    Полученные webhook ..... %shttp://localhost:9000/received%s\n' "$CYAN" "$RESET"
printf '    Логи обработчика ....... %smake logs-consumer%s\n' "$CYAN" "$RESET"
if ! $FULL; then
  printf '    Долгий сценарий «3 попытки → DLQ»: %s./scripts/demo.sh --full%s\n' "$CYAN" "$RESET"
fi
printf '\n'

[ "$FAILED" -eq 0 ]
