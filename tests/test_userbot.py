import asyncio
from datetime import UTC, datetime
from io import BytesIO
from types import SimpleNamespace

import pytest
from telethon import TelegramClient
from telethon.client.telegrambaseclient import _ExportState
from telethon.sessions import MemorySession
from telethon.tl import types

from src.userbot import _revive_dead_file_senders, send_gift_sticker

FILE_DC = 4


class FakeExportedSender:
    """The part of `MTProtoSender` that `_borrow_exported_sender` touches."""

    def __init__(self, *, connected: bool) -> None:
        self.dc_id = FILE_DC
        self.connected = connected
        self.connections: list[object] = []

    def is_connected(self) -> bool:
        return self.connected

    async def connect(self, connection: object) -> bool:
        self.connections.append(connection)
        self.connected = True

        return True


def _client_with(
    sender: FakeExportedSender, monkeypatch: pytest.MonkeyPatch
) -> tuple[TelegramClient, _ExportState]:
    client = TelegramClient(MemorySession(), 1, "0" * 32, receive_updates=False)
    # A download that failed never returned its borrow: `_n` stays 1, so
    # Telethon's idle cleanup never resets `_connected` for this sender.
    state = _ExportState()
    state.add_borrow()
    client._borrowed_senders[FILE_DC] = (state, sender)

    async def get_dc(dc_id: int) -> types.DcOption:
        return types.DcOption(id=dc_id, ip_address="127.0.0.1", port=443)

    monkeypatch.setattr(client, "_get_dc", get_dc)

    return client, state


async def test_a_dead_file_sender_is_reconnected_by_telethons_own_borrow(
    logger, monkeypatch
):
    """Telethon hands a cached per-DC sender out without checking that it is
    connected, so one that gave up made every later download from its DC fail.

    This drives the real `_borrow_exported_sender`, so a Telethon upgrade that
    changes the contract the fix relies on fails here.
    """
    sender = FakeExportedSender(connected=False)
    client, state = _client_with(sender, monkeypatch)

    _revive_dead_file_senders(client, logger)

    assert state.need_connect() is True

    borrowed = await client._borrow_exported_sender(FILE_DC)

    assert borrowed is sender
    assert len(sender.connections) == 1
    assert sender.is_connected() is True


async def test_a_sender_telethon_is_still_retrying_is_left_alone(logger, monkeypatch):
    sender = FakeExportedSender(connected=True)
    client, state = _client_with(sender, monkeypatch)

    _revive_dead_file_senders(client, logger)

    assert state.need_connect() is False

    await client._borrow_exported_sender(FILE_DC)

    assert sender.connections == []


async def test_a_sticker_on_a_dc_whose_sender_gave_up_is_still_posted(
    logger, monkeypatch
):
    """`send_gift_sticker` must revive the dead sender before it downloads.

    Only the file transfer is faked: the borrow is Telethon's own, so the dead
    sender is handed out unless the revive ran first, and a dead sender fails
    the transfer the way `MTProtoSender.send` does.
    """
    sender = FakeExportedSender(connected=False)
    client, _ = _client_with(sender, monkeypatch)
    document = types.Document(
        id=7,
        access_hash=7,
        file_reference=b"ref",
        date=datetime(2026, 1, 1, tzinfo=UTC),
        mime_type="application/x-tgsticker",
        size=3,
        dc_id=FILE_DC,
        attributes=[types.DocumentAttributeFilename(file_name="7.tgs")],
    )
    uploads: list[bytes] = []

    async def download_media(doc: types.Document, file: type[bytes]) -> bytes:
        borrowed = await client._borrow_exported_sender(doc.dc_id)

        if not borrowed.is_connected():
            raise ConnectionError("Cannot send requests while disconnected")

        return b"tgs"

    async def send_file(peer: object, stream: BytesIO, **_: object) -> object:
        uploads.append(stream.read())

        return SimpleNamespace(id=1)

    monkeypatch.setattr(client, "download_media", download_media)
    monkeypatch.setattr(client, "send_file", send_file)

    message = await asyncio.wait_for(
        send_gift_sticker(client, types.InputPeerEmpty(), document, logger),
        timeout=5,
    )

    assert message.id == 1
    assert uploads == [b"tgs"]
    assert len(sender.connections) == 1
