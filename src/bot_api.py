import asyncio
import itertools
import json
from logging import Logger
from typing import Any

import httpx

from src.config import TelegramConfig

_BASE_PAYLOAD: dict[str, Any] = {
    "parse_mode": "HTML",
    "link_preview_options": {"is_disabled": True},
}
_MIN_ATTEMPTS = 3
_TOO_MANY_REQUESTS = 429


class BotApiError(Exception):
    def __init__(self, code: int, description: str) -> None:
        super().__init__(f"[{code}] {description}")

        self.code = code
        self.description = description


class BotApiClient:
    def __init__(self, cfg: TelegramConfig, logger: Logger) -> None:
        self._tokens = list(cfg.bot_tokens)
        self._cycle = itertools.cycle(self._tokens)
        self._logger = logger
        self._http = httpx.AsyncClient(
            base_url=cfg.bot_api_base_url,
            timeout=cfg.request_timeout,
            limits=httpx.Limits(max_connections=len(self._tokens) * 2),
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def send_message(
        self,
        chat_id: int,
        text: str,
        reply_to_message_id: int | None = None,
    ) -> int:
        payload: dict[str, Any] = {"chat_id": chat_id, "text": text}

        if reply_to_message_id is not None:
            payload["reply_parameters"] = {
                "message_id": reply_to_message_id,
                "allow_sending_without_reply": True,
            }

        result = await self._request("sendMessage", payload)
        message_id = result.get("message_id") if isinstance(result, dict) else None

        if not isinstance(message_id, int):
            raise BotApiError(-1, f"sendMessage returned no message_id: {result!r}")

        return message_id

    async def edit_message_text(self, chat_id: int, message_id: int, text: str) -> bool:
        try:
            await self._request(
                "editMessageText",
                {"chat_id": chat_id, "message_id": message_id, "text": text},
            )

        except BotApiError as ex:
            if ex.code == 400 and "message is not modified" in ex.description.lower():
                return False

            raise

        return True

    async def _request(self, method: str, payload: dict[str, Any]) -> object:
        body = payload | _BASE_PAYLOAD
        attempts = max(_MIN_ATTEMPTS, len(self._tokens))

        self._logger.debug("Bot API %s payload: %s", method, payload)

        for attempt in range(1, attempts + 1):
            token = next(self._cycle)

            try:
                # `httpx.TimeoutException` is a `TransportError` subclass.
                response = (
                    await self._http.post(f"/bot{token}/{method}", json=body)
                ).json()

            except (httpx.TransportError, json.JSONDecodeError):
                self._logger.warning(
                    "Bot API %s failed at transport level (attempt %d/%d)",
                    method,
                    attempt,
                    attempts,
                )

                continue

            if response.get("ok"):
                return response.get("result")

            code = int(response.get("error_code") or -1)
            description = str(response.get("description") or "")

            if code == _TOO_MANY_REQUESTS:
                retry_after = (response.get("parameters") or {}).get("retry_after", 1)

                self._logger.warning(
                    "Bot API %s rate limited, retrying in %ss (attempt %d/%d)",
                    method,
                    retry_after,
                    attempt,
                    attempts,
                )

                await asyncio.sleep(retry_after)

                continue

            # Deterministic errors must not burn every token.
            raise BotApiError(code, description)

        raise BotApiError(-1, f"all bot tokens failed for {method}")
