"""Демонстрационный приёмник webhook.

Печатает каждое полученное уведомление вместе с заголовком подписи (саму
подпись НЕ проверяет — это приёмник для демонстрации). Полезен, чтобы глазами
увидеть сквозной путь: POST платежа → обработка → webhook. К самому сервису
отношения не имеет, поэтому написан на голой стандартной библиотеке без
зависимостей.

Поведение можно менять query-параметром `?status=<код>` в webhook_url:
  * без параметра        → 200, уведомление принято;
  * ?status=500          → 500, сервис уйдёт в retry;
  * ?status=404          → 404, сервис сразу отправит сообщение в DLQ.
Это удобно для проверки retry/DLQ вручную.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


class Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802 — имя задано базовым классом
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)

        query = parse_qs(urlparse(self.path).query)
        status = int(query.get("status", ["200"])[0])

        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            body = {"_raw": raw.decode("utf-8", "replace")}

        stamp = datetime.now(UTC).isoformat()
        print(
            json.dumps(
                {
                    "received_at": stamp,
                    "event": body.get("event"),
                    "event_id": self.headers.get("X-Webhook-Event-Id"),
                    "signature": self.headers.get("X-Webhook-Signature"),
                    "payment_id": body.get("payment_id"),
                    "status": body.get("status"),
                    "amount": body.get("amount"),
                    "currency": body.get("currency"),
                    "response_code": status,
                },
                ensure_ascii=False,
            ),
            flush=True,
        )

        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"ok": status < 400}).encode())

    def do_GET(self) -> None:  # noqa: N802
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"webhook-sink alive"}')

    def log_message(self, *_: object) -> None:
        """Глушим стандартный access-log: полезная строка печатается в do_POST."""


def main() -> None:
    server = ThreadingHTTPServer(("0.0.0.0", 9000), Handler)  # noqa: S104
    print(json.dumps({"webhook_sink": "listening", "port": 9000}), flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
