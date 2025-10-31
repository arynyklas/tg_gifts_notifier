# TG Gifts Notifier 🎁

<div align="center">

![Python](https://img.shields.io/badge/python-3.10+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)

**A powerful production-ready Telegram Star Gifts monitoring system**  
with real-time notifications, smart analytics, and enterprise features

[**🏆 Demo Channel**](#-channels) • [**✨ Features**](#-features) • [**🚀 Quick Start**](#-quick-start) • [**📚 API Docs**](#-api-documentation)

---

</div>

---

## 📢 Live Demo Channels

Check out our live monitoring channels to see the system in action:

- **🎁 Main Channel**: [@gifts_detector](https://t.me/gifts_detector)
- **⬆️ Upgrades Channel**: [@gifts_upgrades_detector](https://t.me/gifts_upgrades_detector)

---

## ✨ Features

### 🔔 Core Features
- ✅ **Real-time Monitoring**: Automatic detection of new Telegram star gifts
- ✅ **Smart Notifications**: Configure when and how to be notified
- ✅ **Upgrade Alerts**: Get notified when gifts become upgradable
- ✅ **Update Tracking**: Live updates on gift availability changes

### 🚀 Advanced Features
- 🔍 **Smart Filters**: Price, type, premium status, blacklist support
- 🚨 **Priority System**: Critical/High/Normal/Low priority levels
- 📊 **History Tracking**: Complete change history with timestamps
- 📈 **Analytics**: Sale rate, sold-out prediction, critical gift detection
- 🌐 **REST API**: Full-featured FastAPI with Swagger docs
- 💾 **Auto Backups**: Scheduled backups with retention management
- 🛠️ **CLI Tools**: Command-line interface for data management
- ❤️ **Health Monitoring**: Built-in healthcheck and metrics

---

## 📋 Table of Contents

- [Quick Start](#-quick-start)
- [Installation](#-installation)
- [Configuration](#-configuration)
- [Usage](#-usage)
- [API Documentation](#-api-documentation)
- [CLI Reference](#-cli-reference)
- [Architecture](#-architecture)
- [Testing](#-testing)
- [Statistics & Analytics](#-statistics--analytics)
- [Advanced Usage](#-advanced-usage)
- [Contributing](#-contributing)

---

📖 **[Full Architecture Documentation](ARCHITECTURE.md)** • **[Recent Improvements](IMPROVEMENTS.md)**

---

## 🚀 Quick Start

### 1️⃣ Clone & Install

```bash
git clone https://github.com/arynyklas/tg_gifts_notifier.git
cd tg_gifts_notifier
pip install -r requirements.txt
```

### 2️⃣ Configure

```bash
cp config.example.py config.py
# Edit config.py with your credentials
```

### 3️⃣ Run

```bash
# Start monitoring
python main.py

# Save data only (no monitoring)
python main.py --save-only

# (Optional) Start API server
python api/api_server.py

# (Optional) Use CLI tools
python api/cli.py stats
```

**🎉 You're all set!**

---

## 📦 Installation

### Requirements

- **Python**: 3.10 or higher
- **Telegram Account**: API credentials from [my.telegram.org](https://my.telegram.org)
- **Telegram Bot**: Bot token from [@BotFather](https://t.me/BotFather)

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Get API Credentials

1. Visit [my.telegram.org](https://my.telegram.org/apps)
2. Create an application
3. Note your `API ID` and `API Hash`
4. Get bot token from [@BotFather](https://t.me/BotFather)

---

## ⚙️ Configuration

### Basic Setup

1. Copy example config:
   ```bash
   cp config.example.py config.py
   ```

2. Edit `config.py`:

```python
API_ID = 12345678  # Your API ID
API_HASH = "your_api_hash_here"  # Your API Hash

BOT_TOKENS = [
    "123456789:ABCdefGHIjklMNOpqrsTUVwxyz"  # Bot token from @BotFather
]

NOTIFY_CHAT_ID = -1001234567890  # Your channel/group ID
```

### Filter Configuration

```python
# Price filters
FILTER_MIN_PRICE = 100  # Minimum price in stars
FILTER_MAX_PRICE = 10000  # Maximum price in stars

# Type filters
FILTER_LIMITED_ONLY = True  # Only limited gifts
FILTER_PREMIUM_ONLY = False  # Only premium gifts

# Advanced filters
FILTER_MAX_PERCENT_SOLD = 50.0  # Max sold percentage
FILTER_BLACKLIST_IDS = [12345, 67890]  # IDs to ignore
```

### Priority Configuration

```python
NOTIFY_MIN_PRIORITY = "normal"  # "critical", "high", "normal", "low"
CRITICAL_CHAT_ID = -1001234567890  # Separate chat for critical alerts
```

**📖 Full configuration reference**: See [config.example.py](config.example.py)

---

## 💻 Usage

### Starting the Monitor

```bash
# Normal mode
python main.py

# Save data only (no monitoring)
python main.py --save-only
```

### API Server

```bash
# Start server
python api/api_server.py

# Server runs on http://127.0.0.1:8000
# Interactive docs: http://127.0.0.1:8000/docs
```

### CLI Tools

```bash
# Search for a gift
python api/cli.py search 12345 --detailed

# List gifts with filters
python api/cli.py list --limited --available

# Show history
python api/cli.py history 12345 --limit 20

# Export to CSV
python api/cli.py export gifts.csv

# View statistics
python api/cli.py stats

# Backup management
python api/cli.py backup
python api/cli.py backups
python api/cli.py restore backups/star_gifts_20240101_120000.json
```

---

## 📚 API Documentation

### Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | API information |
| `GET` | `/health` | Healthcheck |
| `GET` | `/gifts` | List all gifts |
| `GET` | `/gifts/{id}` | Get gift by ID |
| `GET` | `/gifts/{id}/history` | Get gift history |
| `GET` | `/gifts/{id}/statistics` | Get gift statistics |
| `GET` | `/statistics` | Global statistics |

### Quick Examples

```bash
# Health check
curl http://localhost:8000/health

# Get all limited gifts
curl "http://localhost:8000/gifts?limited_only=true"

# Get gift statistics
curl http://localhost:8000/gifts/12345/statistics

# Global stats
curl http://localhost:8000/statistics
```

**📖 Interactive API Docs**: Visit `http://localhost:8000/docs`

---

## 🏗️ Architecture

### Project Structure

```
tg_gifts_notifier/
├── 📁 core/                      # Core monitoring engine
│   ├── __init__.py
│   ├── detector.py              # Main monitoring logic
│   ├── star_gifts_data.py       # Data models & persistence
│   ├── parse_data.py            # Telegram API integration
│   └── userbot_helpers.py       # Media download helpers
│
├── 📁 features/                  # Advanced functionality
│   ├── __init__.py
│   ├── history_manager.py       # History tracking & analytics
│   ├── filters_and_priorities.py # Filtering & priority system
│   └── backup_manager.py        # Backup management
│
├── 📁 api/                       # REST API & CLI
│   ├── __init__.py
│   ├── api_server.py            # FastAPI REST server
│   └── cli.py                   # Command-line interface
│
├── 📁 utils/                     # Utilities
│   ├── __init__.py
│   ├── utils.py                 # Common utilities
│   ├── config_validator.py      # Configuration validation
│   └── constants.py             # Constants
│
├── 📄 main.py                    # Main entry point
├── 📄 config.example.py          # Example configuration
├── 📄 requirements.txt           # Dependencies
│
└── 📁 Documentation
    ├── README.md                # Main documentation
    ├── IMPROVEMENTS.md          # Recent improvements
    └── ARCHITECTURE.md          # System architecture
```

### Data Flow

```
┌─────────────┐
│  Telegram   │
│  API        │
└──────┬──────┘
       │
       ▼
┌─────────────┐      ┌─────────────┐
│   Parser    │─────▶│   Data      │
│   (MTProto) │      │   Models    │
└──────┬──────┘      └──────┬──────┘
       │                    │
       ▼                    ▼
┌─────────────┐      ┌─────────────┐
│  Detector   │─────▶│   History   │
│  (Monitor)  │      │   Manager   │
└──────┬──────┘      └─────────────┘
       │
       ▼
┌─────────────────────────────────┐
│  Filters & Priorities           │
│  ┌─────────┐  ┌──────────────┐ │
│  │ Filters │  │  Priorities  │ │
│  └────┬────┘  └──────┬───────┘ │
└───────┼──────────────┼─────────┘
        │              │
        ▼              ▼
┌─────────────┐  ┌─────────────┐
│  Telegram   │  │  REST API   │
│  Channels   │  │  Server     │
└─────────────┘  └─────────────┘
```

---

## 🎯 Configuration Reference

### Core Settings

| Setting | Type | Description | Default |
|---------|------|-------------|---------|
| `SESSION_NAME` | String | Pyrogram session name | `"account"` |
| `API_ID` | Integer | Telegram API ID | Required |
| `API_HASH` | String | Telegram API hash | Required |
| `BOT_TOKENS` | List | Bot tokens for notifications | Required |
| `CHECK_INTERVAL` | Float | Check interval (seconds) | `1.0` |
| `CHECK_UPGRADES_PER_CYCLE` | Float | Upgrade check interval | `2.0` |
| `DATA_FILEPATH` | Path | Data storage path | `star_gifts.json` |
| `NOTIFY_CHAT_ID` | Integer | Main notification channel | Required |
| `TIMEZONE` | String | Timezone for timestamps | `"UTC"` |

### Filter Settings

| Setting | Type | Description | Default |
|---------|------|-------------|---------|
| `FILTER_MIN_PRICE` | Integer/None | Minimum price filter | `None` |
| `FILTER_MAX_PRICE` | Integer/None | Maximum price filter | `None` |
| `FILTER_LIMITED_ONLY` | Boolean | Only limited gifts | `False` |
| `FILTER_PREMIUM_ONLY` | Boolean | Only premium gifts | `False` |
| `FILTER_MAX_PERCENT_SOLD` | Float | Max sold percentage | `100.0` |
| `FILTER_BLACKLIST_IDS` | List | Blacklisted gift IDs | `[]` |
| `NOTIFY_MIN_PRIORITY` | String | Min priority level | `"normal"` |
| `CRITICAL_CHAT_ID` | Integer/None | Critical alerts channel | `None` |

---

## 🧪 Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov

# Verbose output
pytest -v
```

---

## 📊 Statistics & Analytics

### Available Metrics

- **Sale Rate**: Units sold per hour
- **Sold-out Prediction**: Estimated time until complete sellout
- **Critical Detection**: Automatic flagging of fast-selling gifts
- **Per-Gift Stats**: Individual analytics for each gift
- **Global Stats**: System-wide statistics

### Example Output

```
Global Statistics:
Total Gifts: 150
Limited Gifts: 100
Available Gifts: 75
Critical Gifts: 5
Average Price: 450.5 ⭐
```

---

## 🔧 Advanced Usage

### Custom Priority Rules

```python
# In features/filters_and_priorities.py
from features.filters_and_priorities import calculate_priority, NotificationPriority

if gift.total_amount <= 100:
    return NotificationPriority.CRITICAL
```

### Automated Backups

```python
from features.backup_manager import auto_backup_task

# Hourly backups, keep last 24
auto_backup_task(
    interval_seconds=3600,
    max_backups=24
)
```

### API Integration

```python
import requests

# Get critical gifts only
response = requests.get(
    "http://localhost:8000/gifts",
    params={"limited_only": True}
)
critical_gifts = [g for g in response.json() if g["statistics"]["is_critical"]]
```

---

## 🛠️ Development

### Running in Development

```bash
# Install dev dependencies
pip install -r requirements.txt
pip install pytest pytest-asyncio

# Run tests
pytest

# Check code quality
flake8 .
mypy .
```

### Project Scripts

```bash
# Format code
black .

# Type checking
mypy .

# Linting
flake8 .
```

---

## 📝 CLI Reference

### Commands

| Command | Arguments | Description |
|---------|-----------|-------------|
| `search` | `<id> [--detailed]` | Search gift by ID |
| `list` | `[--limited] [--available]` | List all gifts |
| `history` | `<id> [--limit N]` | Show gift history |
| `export` | `<file>` | Export to CSV |
| `stats` | - | Global statistics |
| `backup` | `[--dir PATH]` | Create backup |
| `restore` | `<file>` | Restore backup |
| `backups` | - | List backups |

### Examples

```bash
# Quick commands
python api/cli.py search 12345 --detailed
python api/cli.py list --limited
python api/cli.py stats

# Data management
python api/cli.py export data.csv
python api/cli.py backup
python api/cli.py backups
```

---

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

### Development Setup

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `pytest`
5. Submit a pull request

---

## 📄 License

This project is licensed under the MIT License.

---

## 🙏 Acknowledgments

- Built with [Pyrogram](https://docs.pyrogram.org/)
- Powered by [FastAPI](https://fastapi.tiangolo.com/)
- Inspired by the Telegram community

---

## 📞 Contact

- **Website**: [aryn.sek.su](https://aryn.sek.su)
- **Channels**: [@gifts_detector](https://t.me/gifts_detector)
- **Support**: [Donation Links](https://aryn.sek.su/donates)

---

<div align="center">

**Made with ❤️ for the Telegram community**

⭐ Star this repo if you find it useful!

</div>
