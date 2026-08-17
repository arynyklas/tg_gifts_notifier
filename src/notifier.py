import asyncio
from logging import Logger
import math
from zoneinfo import ZoneInfo

from telethon import TelegramClient, errors
from telethon.tl import types

from src import constants, utils
from src.bot_api import BotApiClient, BotApiError
from src.config import Config
from src.models import StarGiftData
from src.store import StarGiftsStore
from src.userbot import AUTH_ERRORS, send_gift_sticker


class Notifier:
    def __init__(
        self,
        *,
        client: TelegramClient,
        bot_api: BotApiClient,
        store: StarGiftsStore,
        cfg: Config,
        notify_peer: types.TypeInputPeer,
        upgrades_peer: types.TypeInputPeer | None,
        logger: Logger,
    ) -> None:
        self._client = client
        self._bot_api = bot_api
        self._store = store
        self._texts = cfg.texts
        self._notifications = cfg.notifications
        self._notify_peer = notify_peer
        self._upgrades_peer = upgrades_peer
        self._timezone = ZoneInfo(cfg.notifications.timezone)
        self._logger = logger

    def render_gift_text(self, gift: StarGiftData) -> str:
        texts = self._texts
        is_limited = gift.is_limited

        available_percentage_str = constants.NULL_STR
        available_percentage_is_same = False

        if is_limited and gift.total_amount > 0:
            available_percentage_str, available_percentage_is_same = utils.pretty_float(
                math.ceil(gift.available_amount / gift.total_amount * 100 * 100) / 100
            )

        return texts.notify.format(
            title=texts.title_limited if is_limited else texts.title_regular,
            number=gift.number,
            id=gift.id,
            total_amount=(
                texts.total_amount.format(
                    total_amount=utils.pretty_int(gift.total_amount)
                )
                if is_limited
                else constants.NULL_STR
            ),
            available_amount=(
                texts.available_amount.format(
                    available_amount=utils.pretty_int(gift.available_amount),
                    same_str=(
                        constants.NULL_STR if available_percentage_is_same else "~"
                    ),
                    available_percentage=available_percentage_str,
                    updated_datetime=utils.get_current_datetime(self._timezone),
                )
                if is_limited
                else constants.NULL_STR
            ),
            sold_out=(
                texts.sold_out.format(
                    sold_out=utils.format_seconds_to_human_readable(
                        gift.last_sale_timestamp - gift.first_appearance_timestamp
                    )
                )
                if gift.last_sale_timestamp and gift.first_appearance_timestamp
                else constants.NULL_STR
            ),
            price=utils.pretty_int(gift.price),
            convert_price=utils.pretty_int(gift.convert_price),
            require_premium_or_user_limited=(
                texts.require_premium_or_user_limited.format(
                    emoji=texts.require_premium_or_user_limited_emoji,
                    require_premium=(
                        texts.require_premium
                        if gift.require_premium
                        else constants.NULL_STR
                    ),
                    user_limited=(
                        texts.user_limited.format(
                            user_limited=utils.pretty_int(gift.user_limited)
                        )
                        if gift.user_limited is not None
                        else constants.NULL_STR
                    ),
                    separator=(
                        texts.require_premium_and_user_limited_separator
                        if gift.require_premium and gift.user_limited is not None
                        else constants.NULL_STR
                    ),
                )
                if gift.require_premium or gift.user_limited is not None
                else constants.NULL_STR
            ),
        )

    def validate_texts(self) -> None:
        """Render every template once so a bad placeholder aborts startup."""
        probe = StarGiftData(
            id=0,
            number=1,
            price=1,
            convert_price=1,
            available_amount=1,
            total_amount=2,
            require_premium=True,
            user_limited=1,
            is_limited=True,
            first_appearance_timestamp=1,
            last_sale_timestamp=2,
        )

        self.render_gift_text(probe)
        self._texts.upgrades.format(id=probe.id)

    async def announce_new_gift(
        self,
        gift: StarGiftData,
        sticker: types.Document | None,
    ) -> bool:
        """Post sticker + text for a new gift; safe to retry.

        The sticker message id is persisted before the text is sent, so a retry
        never double-posts the sticker.
        """
        try:
            if gift.sticker_message_id is None and sticker is not None:
                message = await send_gift_sticker(
                    self._client, self._notify_peer, sticker
                )

                await self._store.set_message_ids(
                    gift.id, sticker_message_id=message.id
                )
                await asyncio.sleep(self._notifications.after_sticker_delay)

            message_id = await self._bot_api.send_message(
                self._notifications.chat_id,
                self.render_gift_text(gift),
                reply_to_message_id=gift.sticker_message_id,
            )

            await self._store.set_message_ids(gift.id, message_id=message_id)

        except errors.FileReferenceExpiredError:
            self._logger.warning(
                "Sticker reference of gift %d expired, retrying next cycle", gift.id
            )

            return False

        except AUTH_ERRORS:
            raise

        except (BotApiError, errors.RPCError, OSError):
            self._logger.exception("Failed to announce new gift %d", gift.id)

            return False

        self._logger.info("Announced new gift %d in message %d", gift.id, message_id)

        await asyncio.sleep(self._notifications.after_text_delay)

        return True

    async def edit_gift_text(self, gift: StarGiftData) -> bool:
        if gift.message_id is None:
            self._logger.warning("Cannot edit gift %d: no message id", gift.id)

            return False

        try:
            return await self._bot_api.edit_message_text(
                self._notifications.chat_id,
                gift.message_id,
                self.render_gift_text(gift),
            )

        except (BotApiError, OSError):
            self._logger.exception(
                "Failed to edit gift %d message %d", gift.id, gift.message_id
            )

            return False

    async def announce_upgrade(
        self,
        gift: StarGiftData,
        sticker: types.Document | None,
    ) -> bool:
        """Post sticker + text for an upgraded gift; safe to retry.

        The sticker message id is persisted before the text is sent, so a retry
        never double-posts the sticker.
        """
        peer = self._upgrades_peer
        chat_id = self._notifications.upgrades_chat_id

        if peer is None or chat_id is None:
            self._logger.error(
                "Upgrades chat is not configured, cannot announce %d", gift.id
            )

            return False

        sticker_message_id = gift.upgrade_sticker_message_id

        try:
            if sticker_message_id is None:
                if sticker is None:
                    self._logger.warning(
                        "Gift %d has no sticker for its upgrade notification", gift.id
                    )

                else:
                    message = await send_gift_sticker(self._client, peer, sticker)
                    sticker_message_id = message.id

                    await self._store.set_message_ids(
                        gift.id, upgrade_sticker_message_id=sticker_message_id
                    )
                    await asyncio.sleep(self._notifications.after_sticker_delay)

            await self._bot_api.send_message(
                chat_id,
                self._texts.upgrades.format(id=gift.id),
                reply_to_message_id=sticker_message_id,
            )

        except errors.FileReferenceExpiredError:
            self._logger.warning(
                "Sticker reference of gift %d expired, retrying next cycle", gift.id
            )

            return False

        except AUTH_ERRORS:
            raise

        except (BotApiError, errors.RPCError, OSError):
            self._logger.exception("Failed to announce upgrade of gift %d", gift.id)

            return False

        self._logger.info("Announced upgrade of gift %d", gift.id)

        await asyncio.sleep(self._notifications.after_text_delay)

        return True
