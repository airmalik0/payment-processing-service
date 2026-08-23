# Короткие команды для запуска и проверки сервиса.
# Нужен только Docker с Compose v2 — Python локально не требуется.

.DEFAULT_GOAL := help
.PHONY: help verify up demo test test-e2e lint logs logs-consumer ps queues down clean

COMPOSE := docker compose

help: ## Показать список команд
	@echo "Асинхронный сервис процессинга платежей"
	@echo
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo "Проверка одной командой:  make verify"

verify: up demo test ## Полная проверка с нуля: поднять стек, прогнать сквозной сценарий и тесты

up: ## Поднять весь стек (postgres, rabbitmq, миграции, api, consumer, relay, webhook-sink)
	$(COMPOSE) up -d --build --wait
	@echo
	@echo "  API ..................... http://localhost:8000"
	@echo "  Swagger UI .............. http://localhost:8000/docs"
	@echo "  RabbitMQ Management ..... http://localhost:15672  (payments / payments)"
	@echo "  Приёмник webhook ........ http://localhost:9000/received"
	@echo
	@echo "  Сквозная проверка:  make demo"

demo: ## Прогнать сквозной сценарий: платёж, идемпотентность, retry, DLQ, outbox
	@./scripts/demo.sh

test: ## Прогнать тесты (unit + integration) в контейнере
	$(COMPOSE) --profile test run --rm --build tests

test-e2e: ## Прогнать e2e-тесты на живом RabbitMQ (consumer на это время останавливается)
	@$(COMPOSE) stop consumer >/dev/null
	-@$(COMPOSE) --profile test run --rm --build tests test -m e2e
	@$(COMPOSE) start consumer >/dev/null

lint: ## Проверить стиль и типы (ruff + mypy strict)
	$(COMPOSE) --profile test run --rm --build tests lint

logs: ## Логи всех сервисов
	$(COMPOSE) logs -f

logs-consumer: ## Логи одного обработчика платежей
	$(COMPOSE) logs -f consumer

ps: ## Состояние контейнеров
	$(COMPOSE) ps

queues: ## Очереди RabbitMQ: сколько сообщений где лежит
	@$(COMPOSE) exec -T rabbitmq rabbitmqctl list_queues name messages messages_unacknowledged

down: ## Остановить стек (данные сохраняются)
	$(COMPOSE) --profile test down

clean: ## Остановить стек и удалить данные (тома postgres и rabbitmq)
	$(COMPOSE) --profile test down -v --remove-orphans
