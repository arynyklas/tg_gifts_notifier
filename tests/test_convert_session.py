import importlib.util
from pathlib import Path
import sqlite3
import sys

import pytest
from telethon.sessions import SQLiteSession

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "convert_pyrogram_session.py"
AUTH_KEY = bytes(range(256))


def _load_script():
    """The converter is a standalone script, not an importable package member."""
    spec = importlib.util.spec_from_file_location(
        "convert_pyrogram_session", SCRIPT_PATH
    )

    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)

    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    return module


convert_script = _load_script()


def _pyrogram_session(path: Path, *, dc_id: int = 2, test_mode: int = 0) -> Path:
    connection = sqlite3.connect(path)

    try:
        connection.execute(
            "CREATE TABLE sessions("
            "dc_id, api_id, test_mode, auth_key, date, user_id, is_bot)"
        )
        connection.execute(
            "INSERT INTO sessions VALUES (?, ?, ?, ?, ?, ?, ?)",
            (dc_id, 1234, test_mode, AUTH_KEY, 0, 777, 0),
        )
        connection.commit()

    finally:
        connection.close()

    return path


def _auth_key_of(path: Path) -> bytes:
    session = SQLiteSession(str(path))

    try:
        assert session.auth_key is not None

        return session.auth_key.key

    finally:
        session.close()


def test_conversion_carries_the_auth_key_over(tmp_path, capsys):
    src = _pyrogram_session(tmp_path / "account.session")
    dst = tmp_path / "sessions" / "userbot.session"

    assert convert_script.convert(src, dst) == 0

    capsys.readouterr()

    assert _auth_key_of(dst) == AUTH_KEY

    session = SQLiteSession(str(dst))

    try:
        assert session.server_address == convert_script.DC_IPV4[2]
        assert session.port == convert_script.DEFAULT_PORT
        assert session.dc_id == 2

    finally:
        session.close()


def test_a_test_mode_session_uses_the_test_servers(tmp_path, capsys):
    src = _pyrogram_session(tmp_path / "account.session", dc_id=1, test_mode=1)
    dst = tmp_path / "test.session"

    assert convert_script.convert(src, dst) == 0

    capsys.readouterr()

    session = SQLiteSession(str(dst))

    try:
        assert session.server_address == convert_script.TEST_IPV4[1]

    finally:
        session.close()


def test_a_missing_source_is_refused(tmp_path, capsys):
    assert convert_script.convert(tmp_path / "absent.session", tmp_path / "out.session")

    assert "Source session not found" in capsys.readouterr().err


def test_a_source_without_a_session_row_is_refused(tmp_path, capsys):
    src = tmp_path / "empty.session"
    connection = sqlite3.connect(src)

    try:
        connection.execute(
            "CREATE TABLE sessions("
            "dc_id, api_id, test_mode, auth_key, date, user_id, is_bot)"
        )
        connection.commit()

    finally:
        connection.close()

    assert convert_script.convert(src, tmp_path / "out.session") == 1

    assert "No session row found" in capsys.readouterr().err


def test_an_unknown_dc_is_refused(tmp_path, capsys):
    src = _pyrogram_session(tmp_path / "account.session", dc_id=9)

    assert convert_script.convert(src, tmp_path / "out.session") == 1

    assert "Unknown DC 9" in capsys.readouterr().err
    assert not (tmp_path / "out.session").exists()


def test_an_existing_destination_is_never_overwritten(tmp_path, capsys):
    src = _pyrogram_session(tmp_path / "account.session")
    dst = tmp_path / "userbot.session"

    dst.write_bytes(b"authorized session, do not touch")

    assert convert_script.convert(src, dst) == 1

    assert "refusing to overwrite" in capsys.readouterr().err
    assert dst.read_bytes() == b"authorized session, do not touch"


def test_a_destination_without_the_suffix_cannot_clobber_the_real_session(
    tmp_path, capsys
):
    """`SQLiteSession` appends `.session` when the argument lacks it.

    Checking the path as given therefore passed the guard and then destroyed the
    authorized `<dst>.session` the script exists to preserve.
    """
    src = _pyrogram_session(tmp_path / "account.session")
    existing = tmp_path / "userbot.session"

    existing.write_bytes(b"authorized session, do not touch")

    assert convert_script.convert(src, tmp_path / "userbot") == 1

    assert "refusing to overwrite" in capsys.readouterr().err
    assert existing.read_bytes() == b"authorized session, do not touch"


def test_a_destination_without_the_suffix_still_converts_when_free(tmp_path, capsys):
    src = _pyrogram_session(tmp_path / "account.session")

    assert convert_script.convert(src, tmp_path / "userbot") == 0

    capsys.readouterr()

    assert _auth_key_of(tmp_path / "userbot.session") == AUTH_KEY


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX mode bits only")
def test_the_written_session_is_owner_only(tmp_path, capsys):
    src = _pyrogram_session(tmp_path / "account.session")
    dst = tmp_path / "sessions" / "userbot.session"

    assert convert_script.convert(src, dst) == 0

    capsys.readouterr()

    assert dst.stat().st_mode & 0o777 == 0o600
    assert dst.parent.stat().st_mode & 0o777 == 0o700
