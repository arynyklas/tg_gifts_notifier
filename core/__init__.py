"""
Core package - Main monitoring engine and Telegram integration.
"""

from core.detector import *
from core.parse_data import *
from core.star_gifts_data import *
from core.userbot_helpers import *

__all__ = [
    # Detector
    "detector",
    # Parse data
    "get_all_star_gifts",
    "check_is_star_gift_upgradable",
    # Data models
    "StarGiftHistoryEntry",
    "StarGiftData",
    "StarGiftsData",
    "BaseConfigModel",
    # Userbot helpers
    "download_sticker",
]

