import asyncio
import itertools
from logging import Logger
import re
from typing import Any

import certifi
import niquests
from niquests import exceptions as niquests_errors

from src.config import BotApiConfig

_BASE_PAYLOAD: dict[str, Any] = {
    "parse_mode": "HTML",
    "link_preview_options": {"is_disabled": True},
}
_MIN_ATTEMPTS = 3
_BAD_REQUEST = 400
_TOO_MANY_REQUESTS = 429
# niquests verifies against the OS trust store, which on Windows is populated on
# demand and can lack the root that signs api.telegram.org, failing with
# CERTIFICATE_VERIFY_FAILED; certifi carries the public roots on every machine.
# It must be passed per request: `AsyncSession(verify=...)` and `session.verify`
# are both overridden by the `verify=True` default of every request method.
_CA_BUNDLE = certifi.where()
# A niquests exception stringifies the request path, and the path carries the
# bot token; nothing derived from one may reach a log record un-redacted.
_TOKEN_PATH = re.compile(r"/bot[^/\s]+/")


def _redact(text: str) -> str:
    return _TOKEN_PATH.sub("/bot<redacted>/", text)


class BotApiError(Exception):
    def __init__(self, code: int, description: str) -> None:
        super().__init__(f"[{code}] {description}")

        self.code = code
        self.description = description


class BotApiClient:
    def __init__(self, cfg: BotApiConfig, logger: Logger) -> None:
        self._tokens = list(cfg.tokens)
        self._cycle = itertools.cycle(self._tokens)
        self._logger = logger
        self._http = niquests.AsyncSession(
            base_url=cfg.base_url,
            timeout=cfg.request_timeout,
            pool_maxsize=len(self._tokens) * 2,
            retries=0,  # token rotation below is the only retry mechanism
        )

    async def aclose(self) -> None:
        await self._http.close()

    async def send_message(
        self,
        chat_id: int,
        text: str,
        reply_to_message_id: int | None = None,
        *,
        disable_notification: bool = False,
    ) -> int:
        payload: dict[str, Any] = {"chat_id": chat_id, "text": text}

        if reply_to_message_id is not None:
            payload["reply_parameters"] = {
                "message_id": reply_to_message_id,
                "allow_sending_without_reply": True,
            }

        if disable_notification:
            payload["disable_notification"] = True

        result = await self._request("sendMessage", payload)
        message_id = result.get("message_id") if isinstance(result, dict) else None

        if not isinstance(message_id, int):
            raise BotApiError(-1, f"sendMessage returned no message_id: {result!r}")

        return message_id

    async def edit_message_text(self, chat_id: int, message_id: int, text: str) -> None:
        """Raise `BotApiError` on failure.

        "message is not modified" means the message already shows `text`, which
        is exactly what the caller wanted, so it counts as success.
        """
        try:
            await self._request(
                "editMessageText",
                {"chat_id": chat_id, "message_id": message_id, "text": text},
            )

        except BotApiError as ex:
            if (
                ex.code == _BAD_REQUEST
                and "message is not modified" in ex.description.lower()
            ):
                return

            raise

    async def _request(self, method: str, payload: dict[str, Any]) -> object:
        """Post `payload`, rotating to the next token on any non-400 failure.

        401 (revoked token), 403 (bot removed from the chat) and 5xx are scoped
        to one token or one moment, so another token may well succeed; only a
        400 is a property of the request itself and fails at once.
        """
        body = payload | _BASE_PAYLOAD
        attempts = max(_MIN_ATTEMPTS, len(self._tokens))
        last_error: BotApiError | None = None

        self._logger.debug("Bot API %s payload: %s", method, payload)

        for attempt in range(1, attempts + 1):
            token = next(self._cycle)

            try:
                response = (
                    await self._http.post(
                        f"/bot{token}/{method}", json=body, verify=_CA_BUNDLE
                    )
                ).json()

            except niquests_errors.RequestException as ex:
                self._logger.warning(
                    "Bot API %s failed at transport level (attempt %d/%d): %s: %s",
                    method,
                    attempt,
                    attempts,
                    type(ex).__name__,
                    _redact(str(ex)),
                )

                continue

            if not isinstance(response, dict):
                self._logger.warning(
                    "Bot API %s returned a non-object body (attempt %d/%d): %r",
                    method,
                    attempt,
                    attempts,
                    response,
                )

                continue

            if response.get("ok"):
                return response.get("result")

            code = int(response.get("error_code") or -1)
            description = str(response.get("description") or "")
            last_error = BotApiError(code, description)

            if code == _TOO_MANY_REQUESTS:
                # Sleeping after the final attempt only delays the inevitable raise.
                if attempt == attempts:
                    raise last_error

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

            # A bad request fails the same way with every token.
            if code == _BAD_REQUEST:
                raise last_error

            self._logger.warning(
                "Bot API %s failed with [%d] %s (attempt %d/%d)",
                method,
                code,
                description,
                attempt,
                attempts,
            )

        # The caller's log must say why, not just that every token failed.
        if last_error is not None:
            raise last_error

        raise BotApiError(-1, f"all bot tokens failed for {method}")
