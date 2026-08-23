"""Демонстрационный приёмник webhook.

К самому сервису отношения не имеет: это стенд, чтобы глазами и скриптом
увидеть сквозной путь «POST платежа → обработка → webhook». Написан на голой
стандартной библиотеке, без зависимостей.

Что умеет:

* принимает уведомление, проверяет HMAC-подпись и печатает строку в лог;
* держит журнал последних уведомлений в памяти и отдаёт его по
  `GET /received` (`?payment_id=<id>` — фильтр по платежу);
* `DELETE /received` — очистить журнал;
* управление ответом через query-параметры в `webhook_url`:
    * без параметров   → 200, уведомление принято;
    * `?status=500`    → 500, сервис уйдёт в retry;
    * `?status=404`    → 404 (неустранимо), сервис сразу отправит в DLQ;
    * `?fail=2`        → первые 2 доставки этого события отвечают 503,
                         третья — 200. Так видно успешный retry, а не только DLQ.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import threading
from collections import deque
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

PORT = 9000
JOURNAL_LIMIT = 500
SIGNING_SECRET = os.environ.get("WEBHOOK_SIGNING_SECRET", "")

_lock = threading.Lock()
_journal: deque[dict[str, Any]] = deque(maxlen=JOURNAL_LIMIT)
_delivery_counts: dict[str, int] = {}


def _verify_signature(body: bytes, header: str | None) -> bool | None:
    """Проверяет `X-Webhook-Signature`. None — проверка невозможна (нет секрета/заголовка)."""
    if not SIGNING_SECRET or not header:
        return None
    expected = "sha256=" + hmac.new(SIGNING_SECRET.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, header)


def _decide_status(query: dict[str, list[str]], event_id: str) -> int:
    """Определяет код ответа: фиксированный `status` или «упасть первые N раз»."""
    if "status" in query:
        return int(query["status"][0])

    if "fail" in query:
        fail_times = int(query["fail"][0])
        with _lock:
            seen = _delivery_counts.get(event_id, 0) + 1
            _delivery_counts[event_id] = seen
        # Первые fail_times доставок отвечают 503 → сообщение уходит в retry.
        return 503 if seen <= fail_times else 200

    return 200


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _respond(self, status: int, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # Имена do_POST/do_GET/do_DELETE задаёт BaseHTTPRequestHandler.
    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)

        query = parse_qs(urlparse(self.path).query)
        event_id = self.headers.get("X-Webhook-Event-Id", "")
        status = _decide_status(query, event_id)

        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            body = {"_raw": raw.decode("utf-8", "replace")}

        record = {
            "received_at": datetime.now(UTC).isoformat(),
            "event": body.get("event"),
            "event_id": event_id,
            "payment_id": body.get("payment_id"),
            "status": body.get("status"),
            "amount": body.get("amount"),
            "currency": body.get("currency"),
            "failure_reason": body.get("failure_reason"),
            "signature": self.headers.get("X-Webhook-Signature"),
            "signature_valid": _verify_signature(raw, self.headers.get("X-Webhook-Signature")),
            "responded_with": status,
        }

        with _lock:
            _journal.append(record)

        print(json.dumps(record, ensure_ascii=False), flush=True)
        self._respond(status, {"ok": status < 400})

    def do_GET(self) -> None:
        parsed = urlparse(self.path)

        if parsed.path.rstrip("/") == "/received":
            payment_id = parse_qs(parsed.query).get("payment_id", [None])[0]
            with _lock:
                items = list(_journal)
            if payment_id:
                items = [item for item in items if item["payment_id"] == payment_id]
            self._respond(200, items)
            return

        self._respond(200, {"status": "webhook-sink alive", "received": len(_journal)})

    def do_DELETE(self) -> None:
        with _lock:
            _journal.clear()
            _delivery_counts.clear()
        self._respond(200, {"cleared": True})

    def log_message(self, *_: object) -> None:
        """Глушим стандартный access-log: полезная строка печатается в do_POST."""


def main() -> None:
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)  # noqa: S104
    print(
        json.dumps(
            {
                "webhook_sink": "listening",
                "port": PORT,
                "signature_check": bool(SIGNING_SECRET),
            }
        ),
        flush=True,
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
