"""
Module for filtering notifications and determining priorities.
"""
from enum import Enum
import typing

from core import star_gifts_data
import features.history_manager as history_manager


class NotificationPriority(Enum):
    """Notification priorities."""
    CRITICAL = "critical"
    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"


class FilterConfig(typing.TypedDict, total=False):
    """Filter configuration."""
    min_price: int  # Minimum price
    max_price: int  # Maximum price
    is_limited_only: bool  # Only limited gifts
    require_premium_only: bool  # Only Premium gifts
    max_percent_sold: float  # Maximum sold percentage for notification
    blacklist_ids: list[int]  # Blacklist of gift IDs


def check_filter(gift: star_gifts_data.StarGiftData, filter_config: FilterConfig | None = None) -> bool:
    """
    Checks if gift passes through filter.
    
    Args:
        gift: Gift data
        filter_config: Filter configuration
    
    Returns:
        True if gift passes filter
    """
    if filter_config is None:
        return True
    
    # Check minimum price
    if "min_price" in filter_config:
        if gift.price < filter_config["min_price"]:
            return False
    
    # Check maximum price
    if "max_price" in filter_config:
        if gift.price > filter_config["max_price"]:
            return False
    
    # Check limited only
    if filter_config.get("is_limited_only", False):
        if not gift.is_limited:
            return False
    
    # Check premium only
    if filter_config.get("require_premium_only", False):
        if not gift.require_premium:
            return False
    
    # Check sold percentage
    if "max_percent_sold" in filter_config and gift.is_limited and gift.total_amount > 0:
        percent_sold = ((gift.total_amount - gift.available_amount) / gift.total_amount) * 100
        if percent_sold > filter_config["max_percent_sold"]:
            return False
    
    # Check blacklist
    if "blacklist_ids" in filter_config:
        if gift.id in filter_config["blacklist_ids"]:
            return False
    
    return True


def calculate_priority(gift: star_gifts_data.StarGiftData) -> NotificationPriority:
    """
    Calculates notification priority for gift.
    
    Args:
        gift: Gift data
    
    Returns:
        Notification priority
    """
    # Critical priority - very fast sale
    if history_manager.is_critical_sale_speed(gift, threshold_percent=50.0):
        return NotificationPriority.CRITICAL
    
    # High priority - rare gift (small limit) or fast sale
    if gift.is_limited and gift.total_amount > 0:
        if gift.total_amount <= 1000:
            return NotificationPriority.HIGH
        
        if history_manager.is_critical_sale_speed(gift, threshold_percent=25.0):
            return NotificationPriority.HIGH
    
    # Low priority - not limited and not upgradable
    if not gift.is_limited and not gift.is_upgradable:
        return NotificationPriority.LOW
    
    # Normal priority by default
    return NotificationPriority.NORMAL


def should_send_notification(
    gift: star_gifts_data.StarGiftData,
    filter_config: FilterConfig | None = None,
    min_priority: NotificationPriority = NotificationPriority.NORMAL
) -> tuple[bool, NotificationPriority]:
    """
    Determines if notification should be sent for gift.
    
    Args:
        gift: Gift data
        filter_config: Filter configuration
        min_priority: Minimum priority for sending
    
    Returns:
        Tuple (should_send, priority)
    """
    # Check filter
    if not check_filter(gift, filter_config):
        return (False, NotificationPriority.NORMAL)
    
    # Calculate priority
    priority = calculate_priority(gift)
    
    # Compare with minimum priority
    priority_values = {
        NotificationPriority.CRITICAL: 4,
        NotificationPriority.HIGH: 3,
        NotificationPriority.NORMAL: 2,
        NotificationPriority.LOW: 1
    }
    
    should_send = priority_values[priority] >= priority_values[min_priority]
    
    return (should_send, priority)
