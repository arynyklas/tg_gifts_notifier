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

Re-baseline the stored gift data (fetches the current catalog, notifies nothing, and dumps the previous file to `star_gifts_dump_<timestamp>.json` first):

```sh
uv run python -m src.main --save-only
```

Use `--config path/to/config.yaml` to run against a different config file.

Exit codes: `0` clean shutdown, `1` configuration or startup failure, `2` session not authorized.

## Development

```sh
uv sync --group dev --group tests
uv run ruff check src tests
uv run ruff format src tests --check
uv run ty check src tests
uv run pytest
```

## Configuration

`config.yaml` is validated on startup; unknown keys are rejected.

| Key | Type | Description |
| --- | --- | --- |
| `telegram.api_id` | Integer | Telegram API ID from my.telegram.org |
| `telegram.api_hash` | String | Telegram API hash matching the API ID |
| `telegram.session_path` | String | Path of the Telethon session file (default `sessions/userbot.session`) |
| `telegram.flood_sleep_threshold` | Integer | Flood waits shorter than this are slept through by Telethon (seconds) |
| `telegram.bot_tokens` | [String] | Bot tokens from [BotFather](https://t.me/BotFather) used to send and edit messages; at least one is required |
| `telegram.bot_api_base_url` | String | Bot API base URL (override for a local Bot API server) |
| `telegram.request_timeout` | Float | Bot API request timeout in seconds |
| `detector.check_interval` | Float | Delay between gift catalog polls (seconds) |
| `detector.updates_batch_delay` | Float | Delay between batches of stock-update edits (seconds) |
| `detector.check_upgrades_interval` | Float | Delay between upgradability check cycles (seconds) |
| `detector.upgrades_check_concurrency` | Integer | Concurrent upgradability probes (≥ 1) |
| `detector.announce_retry_max_delay` | Float | Upper bound of the backoff for retrying a failed announcement (seconds) |
| `storage.data_filepath` | String | Path of the gift data file |
| `storage.save_debounce` | Float | Debounce before the data file is rewritten (seconds) |
| `notifications.chat_id` | Integer | Chat where new gift messages are posted |
| `notifications.upgrades_chat_id` | Integer or `null` | Chat where upgrade messages are posted; `null` disables upgrade checks |
| `notifications.after_sticker_delay` | Float | Delay after sending a sticker, before sending its text (seconds) |
| `notifications.after_text_delay` | Float | Delay after sending a text (seconds) |
| `notifications.timezone` | String | Timezone of the rendered timestamps (e.g. `Europe/Moscow`) |
| `logging.console_level` | String or Integer | Console log level (e.g. `DEBUG`) |
| `logging.file_level` | String or Integer | File log level |
| `logging.telethon_level` | String or Integer | Level for Telethon's own loggers, kept separate from ours (default `WARNING`; raise to `DEBUG` to debug MTProto) |
| `logging.filepath` | String | Log file path |
| `logging.max_bytes` | Integer | Size at which the log file is rotated |
| `logging.backup_count` | Integer | Number of rotated log files kept |
| `texts.*` | String | Notification templates; see `config.example.yaml` for every placeholder |

Bots cannot check gift upgradability — Telegram answers `400 BOT_METHOD_INVALID` — which is why the userbot performs those probes.

### Data file migration

An existing `star_gifts.json` is migrated in place on the first start: `version` is set to `2`, the dead `sticker_file_id` / `sticker_file_name` keys are dropped (sticker references are now refreshed from every poll instead of being persisted) and `sticker_message_id` is added so a retried announcement never posts a second sticker.

## Contact

For any questions or feedback, please contact me at [my socials](https://aryn.sek.su/).

---

If you find TG Gifts Notifier useful and would like to support its development, consider making a donation. Your contributions help in maintaining and improving the project.

You can donate via the following methods listed on this [site](https://aryn.sek.su/donates).

Thank you for your support!
