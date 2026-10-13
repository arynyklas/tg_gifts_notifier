"""Reuse a pyrofork/pyrogram auth key in a Telethon session file.

Read-only on the source session; refuses to overwrite the destination.

    python scripts/convert_pyrogram_session.py account.session sessions/userbot.session
"""

import argparse
from pathlib import Path
import sqlite3
import sys

from telethon.crypto import AuthKey
from telethon.sessions import SQLiteSession

DC_IPV4 = {
    1: "149.154.175.53",
    2: "149.154.167.51",
    3: "149.154.175.100",
    4: "149.154.167.91",
    5: "91.108.56.130",
}
TEST_IPV4 = {
    1: "149.154.175.10",
    2: "149.154.167.40",
    3: "149.154.175.117",
}
DEFAULT_PORT = 443


def convert(src: Path, dst: Path) -> int:
    if not dst.name.endswith(".session"):
        # `SQLiteSession` appends the suffix to a name not ending in it (the
        # same `endswith` test), so the guard below has to check the path it
        # will actually write, not the one given.
        dst = dst.with_name(dst.name + ".session")

    if not src.is_file():
        print(f"Source session not found: {src}", file=sys.stderr)

        return 1

    if dst.exists():
        print(
            f"Destination already exists, refusing to overwrite: {dst}", file=sys.stderr
        )

        return 1

    connection = sqlite3.connect(f"file:{src}?mode=ro", uri=True)

    try:
        row = connection.execute(
            "SELECT dc_id, test_mode, auth_key FROM sessions"
        ).fetchone()

    finally:
        connection.close()

    if row is None:
        print(f"No session row found in {src}", file=sys.stderr)

        return 1

    dc_id, test_mode, auth_key = int(row[0]), bool(row[1]), bytes(row[2])
    servers = TEST_IPV4 if test_mode else DC_IPV4

    if dc_id not in servers:
        print(f"Unknown DC {dc_id} (test_mode={test_mode})", file=sys.stderr)

        return 1

    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.parent.chmod(0o700)

    session = SQLiteSession(str(dst))

    try:
        session.set_dc(dc_id, servers[dc_id], DEFAULT_PORT)
        session.auth_key = AuthKey(auth_key)
        session.save()

    finally:
        session.close()

    # Full account access with no 2FA prompt; a no-op for group/other on Windows.
    dst.chmod(0o600)

    print(f"Converted {src} -> {dst} (DC {dc_id}, test_mode={test_mode})")

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument("source", type=Path, help="pyrogram/pyrofork .session file")
    parser.add_argument(
        "destination", type=Path, help="Telethon .session file to create"
    )

    args = parser.parse_args()

    return convert(args.source, args.destination)


if __name__ == "__main__":
    raise SystemExit(main())
