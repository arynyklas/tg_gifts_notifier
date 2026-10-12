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

    Its first line binds `config.schema.json`, so VS Code with the [YAML extension](https://marketplace.visualstudio.com/items?itemName=redhat.vscode-yaml) (`redhat.vscode-yaml`) completes keys, shows each key's description on hover, and flags unknown keys, wrong types and out-of-range values as you type. Timezones and template placeholders are checked when the notifier starts.

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

`config.schema.json` and the key table under [Configuration](#configuration) are generated from `src/config.py`; run `uv run python -m scripts.generate_config_schema` after changing it. The pre-commit hook does this, and the tests fail while either file is stale.

## Configuration

`config.yaml` is validated on startup: unknown keys, wrong types and broken templates are rejected, and every error names its key and line without echoing the value. This table and `config.schema.json` are generated from `src/config.py`.

<!-- config-table:start -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `userbot.api_id` | Integer | required | Telegram API ID from my.telegram.org. |
| `userbot.api_hash` | String | required | Telegram API hash matching `api_id`. |
| `userbot.session_path` | Path | `"sessions/userbot.session"` | Telethon session file. |
| `userbot.flood_sleep_threshold` | Integer | `60` | Flood waits shorter than this many seconds are slept through by Telethon. |
| `bot_api.tokens` | List of strings | required | Bot tokens from @BotFather; requests rotate through them. At least one. |
| `bot_api.base_url` | String | `"https://api.telegram.org"` | Bot API server: `https`, or `http` only for a local server (`localhost`, `127.0.0.1`, `[::1]`). |
| `bot_api.request_timeout` | Number | `20.0` | Bot API request timeout, in seconds. |
| `detector.check_interval` | Number | `1.0` | Delay between gift catalog polls, in seconds. |
| `detector.updates_batch_delay` | Number | `1.0` | Delay between batches of stock-update edits, in seconds. |
| `detector.announce_retry_max_delay` | Number | `60.0` | Upper bound of the backoff for retrying a failed announcement, in seconds. |
| `storage.data_filepath` | Path | `"star_gifts.json"` | Path of the gift data file. |
| `storage.save_debounce` | Number | `2.0` | Debounce before the data file is rewritten, in seconds. |
| `notifications.chat_id` | Integer | required | Chat where new gift messages are posted. |
| `notifications.after_sticker_delay` | Number | `1.0` | Delay after posting a sticker, before its text, in seconds. Paces upgrade posts too. |
| `notifications.after_text_delay` | Number | `2.0` | Delay after posting a text, in seconds. Paces upgrade posts too. |
| `notifications.timezone` | String | `"UTC"` | IANA timezone of the rendered timestamps and of `--save-only` dump names (e.g. `Europe/Moscow`). |
| `upgrades.chat_id` | Integer or `null` | `null` | Chat where upgrade messages are posted; `null` disables upgrade checks. |
| `upgrades.check_interval` | Number | `2.0` | Delay between upgradability check cycles, in seconds. |
| `upgrades.check_concurrency` | Integer | `4` | Concurrent upgradability probes; `--save-only` and a data file migration use it even with upgrades off. |
| `upgrades.text` | String | `"Gift is upgradable! (<code>{id}</code>)"` | Upgrade message (HTML). Placeholders: `{id}`. |
| `logging.telethon_level` | One of `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` | `"WARNING"` | Level of Telethon's own loggers, kept apart from ours; `DEBUG` to debug MTProto. |
| `logging.console.level` | One of `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` | `"INFO"` | Console log level. |
| `logging.file.level` | One of `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` | `"INFO"` | File log level. |
| `logging.file.path` | Path | `"logs/main.log"` | Log file path. |
| `logging.file.max_bytes` | Integer | `10485760` | Size at which the log file is rotated, in bytes. |
| `logging.file.backup_count` | Integer | `5` | Number of rotated log files kept. |
| `texts.new_gift` | String | required | New gift message. Placeholders: `{title}`, `{number}`, `{id}`, `{total_amount}`, `{available_amount}`, `{sold_out}`, `{price}`, `{convert_price}`, `{require_premium_or_user_limited}`. |
| `texts.title.limited` | String | required | Inserted as `{title}` for a limited gift. |
| `texts.title.regular` | String | required | Inserted as `{title}` for an unlimited gift. |
| `texts.total_amount` | String | required | Inserted as `{total_amount}` for a limited gift. Placeholders: `{total_amount}`. |
| `texts.available_amount` | String | required | Inserted as `{available_amount}` for a limited gift. Placeholders: `{available_amount}`, `{same_str}`, `{available_percentage}`, `{updated_datetime}`. |
| `texts.sold_out` | String | required | Inserted as `{sold_out}` once a gift is sold out. Placeholders: `{sold_out}`. |
| `texts.requirements.template` | String | required | Layout of the line. Placeholders: `{emoji}`, `{require_premium}`, `{separator}`, `{user_limited}`. |
| `texts.requirements.emoji` | String | required | Inserted as `{emoji}`. |
| `texts.requirements.premium` | String | required | Inserted as `{require_premium}` when the gift needs Telegram Premium. |
| `texts.requirements.user_limited` | String | required | Inserted as `{user_limited}` when the gift is limited per user. Placeholders: `{user_limited}`. |
| `texts.requirements.separator` | String | required | Inserted as `{separator}` when both parts are present. |
<!-- config-table:end -->

Bots cannot check gift upgradability — Telegram answers `400 BOT_METHOD_INVALID` — which is why the userbot performs those probes.

### Data file migration

An existing `star_gifts.json` is migrated on the first start: `version` is set to `2`, the dead `sticker_file_id` / `sticker_file_name` keys are dropped (sticker references are now refreshed from every poll instead of being persisted), and four fields are added. `sticker_message_id` and `upgrade_sticker_message_id` ensure a retried new-gift or upgrade announcement never posts a second sticker. `upgrade_stars` holds Telegram's advertised upgrade price; a gift without one is probed at most every 5 minutes. `announce_pending` makes an announcement interrupted by a crash or Ctrl+C be resent on the next start. The file is marked for rewriting as version 2 once the session is authorized and, when needed, the upgradability baseline has completed; a later startup failure, such as an unresolvable chat, still leaves a version 2 file. The legacy code cannot read a version 2 file, so back up `star_gifts.json` before the first start.

When the legacy file marks no gift as upgradable — the legacy notifier ran without upgrade checks — the migrating run baselines upgradability before it announces anything: it probes every stored gift once and records the result, logging `Marked N migrated gifts as already upgradable`. Without it the first upgrades cycle would announce the entire already-upgradable catalog. Otherwise the legacy `is_upgradable` values are trusted, so gifts that became upgradable during the cutover are still announced. Under `--save-only` this baseline is skipped, since `--save-only` re-baselines every gift anyway.

## Contact

For any questions or feedback, please contact me at [my socials](https://aryn.sek.su/).
