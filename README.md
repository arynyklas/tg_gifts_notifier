# TG Gifts Notifier

## Overview

TG Gifts Notifier is a tool designed to notify users about new gifts available on the Telegram platform. It helps users stay updated and never miss out on any gifts.

Our Main Telegram Channel — [Gifts Detector](https://t.me/gifts_detector)

Our Upgrades Telegram Channel — [Gifts Upgrades Detector](https://t.me/gifts_upgrades_detector)

## Features

**Note:** This project requires Python 3.13 or higher and [uv](https://docs.astral.sh/uv/).

- Real-time notifications, posted as a sticker plus a bot-authored message
- Stock updates edited into the original message as gifts sell out
- Separate upgrade notifications channel
- Customizable notification texts

A userbot (Telethon, MTProto layer 227) polls `payments.getStarGifts`; the configured bots send and edit the texts over the Bot API.

## Installation

1. Clone the repository:

    ```sh
    git clone https://github.com/arynyklas/tg_gifts_notifier.git
    ```

2. Navigate to the project directory:

    ```sh
    cd tg_gifts_notifier
    ```

3. Create the environment and install the dependencies:

    ```sh
    uv venv
    uv sync
    ```

4. Create the config:

    ```sh
    cp config.example.yaml config.yaml
    ```

5. Provide an authorized userbot session, either by converting an existing pyrogram/pyrofork one:

    ```sh
    uv run python scripts/convert_pyrogram_session.py account.session sessions/userbot.session
    ```

    by scanning a QR code from an already authorized device (Telegram → Settings → Devices → Link Desktop Device):

    ```sh
    uv run python -m src.main --login qr
    ```

    or by entering a phone number and login code:

    ```sh
    uv run python -m src.main --login
    ```

    Both flows ask for the two-step verification password when the account has one, and allow three attempts. Once a QR code has been scanned, the code stays accepted even if the password is then mistyped: rerunning `--login qr` asks only for the password again, without a new code.

    The userbot must already be a member of both notification chats — Telethon cannot resolve a `-100…` id it has never seen.

## Usage

Start the notifier:

```sh
uv run python -m src.main
```

Re-baseline the stored gift data (fetches the current catalog and notifies nothing):

```sh
uv run python -m src.main --save-only
```

It first dumps the existing data file to `star_gifts_dump_<timestamp>.json`; if that dump fails, it aborts with exit code `1`. It then fetches the catalog and probes which of those gifts are already upgradable, so the first upgrade cycle reports only gifts that become upgradable afterwards. Only after the catalog and every gift's upgradability were fetched is the data file replaced: any failure or Ctrl+C before that point leaves it untouched. The probe is one request per gift, which is why `--save-only` takes a few seconds on a full catalog.

The first start without a data file announces the whole current catalog as new gifts (the legacy notifier did the same). Because those gifts are stored as not upgradable, the first upgrades cycle then also posts every already-upgradable gift to the upgrades chat. Run `--save-only` once before the first start to avoid both.

Use `--config path/to/config.yaml` to run against a different config file.

Exit codes: `0` clean shutdown, including Ctrl+C at any phase once the notifier has started (a Ctrl+C while Python is still importing it prints a traceback); `1` configuration or startup failure, including network, RPC or flood-wait errors during startup; `2` session not authorized or authorization lost, including during the startup upgradability baseline and `--save-only`. After a network outage the notifier reconnects by itself, checking the connection every few seconds even while a request is stuck, and exits with `2` if Telegram no longer knows the session.

The session file is full access to the Telegram account, and `config.yaml` holds the API hash and bot tokens. The notifier narrows the session file and its directory to the owner with `chmod`, which works on POSIX only (on POSIX, also run `chmod 600 config.yaml`). On Windows the files inherit the directory's ACL — which may grant e.g. Authenticated Users: Modify — so restrict them to the current user:

```bat
icacls sessions /inheritance:r /grant:r "%USERNAME%:(OI)(CI)F"
icacls config.yaml /inheritance:r /grant:r "%USERNAME%:F"
```

An announcement interrupted by a crash or a Ctrl+C — the gift was stored but its message was never sent — is re-sent on the next start; the log reports `Re-announcing N gifts interrupted by an earlier run`.

## Development

```sh
uv sync --group dev --group tests
uv run ruff check src tests scripts
uv run ruff format src tests scripts --check
uv run ty check src tests scripts
uv run pytest
uv lock --check
```

CI runs the same commands, except that its `uv sync --locked` stands in for `uv lock --check`. The type-check job is advisory: it is marked `continue-on-error` while `ty` is pre-1.0, so a green CI run does not mean the type check passed.

## Configuration

`config.yaml` is validated on startup; unknown keys are rejected.

| Key | Type | Description |
| --- | --- | --- |
| `telegram.api_id` | Integer | Telegram API ID from my.telegram.org |
| `telegram.api_hash` | String | Telegram API hash matching the API ID |
| `telegram.session_path` | String | Path of the Telethon session file (default `sessions/userbot.session`) |
| `telegram.flood_sleep_threshold` | Integer | Flood waits shorter than this are slept through by Telethon (seconds) |
| `telegram.bot_tokens` | [String] | Bot tokens from [BotFather](https://t.me/BotFather) used to send and edit messages; at least one is required |
| `telegram.bot_api_base_url` | String | Bot API base URL; must be `https`, or `http` only for a local Bot API server (`localhost`, `127.0.0.1`, `[::1]`) |
| `telegram.request_timeout` | Float | Bot API request timeout in seconds |
| `detector.check_interval` | Float | Delay between gift catalog polls (seconds) |
| `detector.updates_batch_delay` | Float | Delay between batches of stock-update edits (seconds) |
| `detector.check_upgrades_interval` | Float | Delay between upgradability check cycles (seconds) |
| `detector.upgrades_check_concurrency` | Integer | Concurrent upgradability probes (≥ 1) |
| `detector.announce_retry_max_delay` | Float | Upper bound of the backoff for retrying a failed announcement (seconds) |
| `storage.data_filepath` | String | Path of the gift data file |
| `storage.save_debounce` | Float | Debounce before the data file is rewritten (seconds) |
| `notifications.chat_id` | Integer | Chat where new gift messages are posted |
| `notifications.upgrades_chat_id` | Integer or `null` | Chat where upgrade messages are posted; `null` disables upgrade checks, so new gifts are never probed and enabling the chat later announces every gift that became upgradable in the meantime |
| `notifications.after_sticker_delay` | Float | Delay after sending a sticker, before sending its text (seconds) |
| `notifications.after_text_delay` | Float | Delay after sending a text (seconds) |
| `notifications.timezone` | String | Timezone of the rendered timestamps (e.g. `Europe/Moscow`) |
| `logging.console_level` | String or Integer | Console log level (e.g. `DEBUG`) |
| `logging.file_level` | String or Integer | File log level |
| `logging.telethon_level` | String or Integer | Level for Telethon's own loggers, kept separate from ours (default `WARNING`; set to `DEBUG` to debug MTProto) |
| `logging.filepath` | String | Log file path |
| `logging.max_bytes` | Integer | Size at which the log file is rotated |
| `logging.backup_count` | Integer | Number of rotated log files kept |
| `texts.*` | String | Notification templates; see `config.example.yaml` for every placeholder |

Bots cannot check gift upgradability — Telegram answers `400 BOT_METHOD_INVALID` — which is why the userbot performs those probes.

### Data file migration

An existing `star_gifts.json` is migrated on the first start: `version` is set to `2`, the dead `sticker_file_id` / `sticker_file_name` keys are dropped (sticker references are now refreshed from every poll instead of being persisted), and four fields are added. `sticker_message_id` and `upgrade_sticker_message_id` ensure a retried new-gift or upgrade announcement never posts a second sticker. `upgrade_stars` holds Telegram's advertised upgrade price; a gift without one is probed at most every 5 minutes. `announce_pending` makes an announcement interrupted by a crash or Ctrl+C be resent on the next start. The file is marked for rewriting as version 2 once the session is authorized and, when needed, the upgradability baseline has completed; a later startup failure, such as an unresolvable chat, still leaves a version 2 file. The legacy code cannot read a version 2 file, so back up `star_gifts.json` before the first start.

When the legacy file marks no gift as upgradable — the legacy notifier ran without upgrade checks — the migrating run baselines upgradability before it announces anything: it probes every stored gift once and records the result, logging `Marked N migrated gifts as already upgradable`. Without it the first upgrades cycle would announce the entire already-upgradable catalog. Otherwise the legacy `is_upgradable` values are trusted, so gifts that became upgradable during the cutover are still announced. Under `--save-only` this baseline is skipped, since `--save-only` re-baselines every gift anyway.

## Contact

For any questions or feedback, please contact me at [my socials](https://aryn.sek.su/).
