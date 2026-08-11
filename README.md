

# TG Gifts Notifier

## Overview

TG Gifts Notifier is a tool designed to notify users about new gifts available on the Telegram platform. It helps users stay updated and never miss out on any gifts.

Our Main Telegram Channel — [Gifts Detector](https://t.me/gifts_detector)

Our Upgrades Telegram Channel — [Gifts Upgrades Detector](https://t.me/gifts_upgrades_detector)

## Features

**Note:** This project requires Python 3.10 or higher to run.

- Real-time notifications
- Easy setup and configuration
- Customizable notification settings

## Installation

To install TG Gifts Notifier, follow these steps:

1. Clone the repository:

    ```sh
    git clone https://github.com/arynyklas/tg_gifts_notifier.git
    ```

2. Navigate to the project directory:

    ```sh
    cd tg_gifts_notifier
    ```

3. Install the required dependencies:

    ```sh
    pip install -r requirements.txt
    ```

## Usage

To start the notifier, run:

```sh
python detector.py
```

If you want to just fetch the gifts data with removing current data, use the `--save-only`/`-S` flag:

```sh
python detector.py --save-only
```

## Configuration

At first rename `config.example.py` to `config.py`.

Then, you must configure the notifier by editing the `config.py` file.

| Field                      | Type              | Description                                                                                               |
|----------------------------|-------------------|-----------------------------------------------------------------------------------------------------------|
| SESSION_NAME               | String            | Name of the session file where the userbot's session will be stored                                       |
| API_ID                     | Integer           | Your Telegram API ID obtained from my.telegram.org                                                        |
| API_HASH                   | String            | Your Telegram API Hash corresponding to your API ID                                                       |
| BOT_TOKENS                 | [String]          | Bot tokens provided by [BotFather](https://t.me/BotFather) of your Telegram bot to send and edit messages |
| CHECK_INTERVAL             | Float             | Time interval (in seconds) between checks for new gifts                                                   |
| CHECK_UPGRADES_PER_CYCLE   | Float             | Time interval (in seconds) to check upgradability of gifts per cycle                                      |
| DATA_FILEPATH              | String            | Path to the file where the gift data is stored                                                            |
| DATA_SAVER_DELAY           | Float             | Delay (in seconds) to save data to the file                                                               |
| NOTIFY_CHAT_ID             | Integer           | Chat ID where new gifts' messages will be sent                                                            |
| NOTIFY_UPGRADES_CHAT_ID    | Integer or `None` | Chat ID where gifts' upgradability messages will be sent                                                  |
| NOTIFY_AFTER_STICKER_DELAY | Float             | Delay (in seconds) after sending a sticker before sending a message                                       |
| NOTIFY_AFTER_TEXT_DELAY    | Float             | Delay (in seconds) after sending a message                                                                |
| TIMEZONE                   | String            | Timezone for the messages' date & time (e.g., "Europe/Moscow")                                            |
| HTTP_REQUEST_TIMEOUT       | Float             | Timeout for Bot API requests (in seconds)                                                                 |
| CONSOLE_LOG_LEVEL          | Integer           | Logging level for console output (e.g., `logging.DEBUG`)                                                  |
| FILE_LOG_LEVEL             | Integer           | Logging level for file output (e.g., `logging.INFO`)                                                      |

## Contact

For any questions or feedback, please contact me at [my socials](https://aryn.sek.su/).

---

If you find TG Gifts Notifier useful and would like to support its development, consider making a donation. Your contributions help in maintaining and improving the project.

You can donate via the following methods listed on this [site](https://aryn.sek.su/donates).

Thank you for your support!
