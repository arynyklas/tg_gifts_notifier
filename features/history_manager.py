"""
Module for managing gift change history and calculating statistics.
"""
from pydantic import BaseModel
import typing

from core import star_gifts_data
import utils.utils as utils

logger = typing.cast(typing.Any, None)  # Will be set during initialization


def init_logger(logger_instance: typing.Any) -> None:
    """Initializes logger for this module."""
    global logger
    logger = logger_instance


def add_history_entry(
    star_gift: star_gifts_data.StarGiftData,
    timestamp: int | None = None
) -> None:
    """
    Adds entry to gift change history.
    
    Args:
        star_gift: Gift data to update history for
        timestamp: Timestamp (if None, uses current time)
    """
    if timestamp is None:
        timestamp = utils.get_current_timestamp()
    
    entry = star_gifts_data.StarGiftHistoryEntry(
        timestamp=timestamp,
        available_amount=star_gift.available_amount,
        price=star_gift.price,
        convert_price=star_gift.convert_price,
        is_upgradable=star_gift.is_upgradable
    )
    
    star_gift.history.append(entry)
    
    # Limit history size (keep max 1000 entries)
    if len(star_gift.history) > 1000:
        star_gift.history = star_gift.history[-1000:]


def get_sale_rate(gift: star_gifts_data.StarGiftData, hours: float = 1.0) -> float | None:
    """
    Calculates sale rate of gift for specified period.
    
    Args:
        gift: Gift data
        hours: Number of hours for analysis
    
    Returns:
        Sale rate (units per hour) or None if insufficient data
    """
    if not gift.is_limited or not gift.history:
        return None
    
    if len(gift.history) < 2:
        return None
    
    cutoff_time = utils.get_current_timestamp() - int(hours * 3600)
    
    # Find first state within specified period
    first_entry = None
    for entry in gift.history:
        if entry.timestamp >= cutoff_time:
            first_entry = entry
            break
    
    if first_entry is None:
        # Take oldest entry
        first_entry = gift.history[0]
    
    last_entry = gift.history[-1]
    
    # Check that change makes sense (decrease in amount)
    if first_entry.available_amount < last_entry.available_amount:
        return None
    
    sold = first_entry.available_amount - last_entry.available_amount
    time_diff = last_entry.timestamp - first_entry.timestamp
    
    if time_diff <= 0:
        return None
    
    hours_diff = time_diff / 3600.0
    
    return sold / hours_diff if hours_diff > 0 else 0.0


def get_estimated_sold_out_time(gift: star_gifts_data.StarGiftData) -> int | None:
    """
    Calculates estimated time until gift is sold out.
    
    Args:
        gift: Gift data
    
    Returns:
        Estimated time in seconds until sold out or None if cannot calculate
    """
    if not gift.is_limited or gift.available_amount <= 0:
        return None
    
    sale_rate = get_sale_rate(gift, hours=1.0)
    
    if sale_rate is None or sale_rate <= 0:
        return None
    
    remaining_time_seconds = int((gift.available_amount / sale_rate) * 3600)
    
    return remaining_time_seconds


def is_critical_sale_speed(gift: star_gifts_data.StarGiftData, threshold_percent: float = 50.0) -> bool:
    """
    Checks if sale speed is critical.
    
    Args:
        gift: Gift data
        threshold_percent: Percentage of sale per hour for critical status
    
    Returns:
        True if sale is critical
    """
    if not gift.is_limited or gift.total_amount <= 0:
        return False
    
    sale_rate = get_sale_rate(gift, hours=1.0)
    
    if sale_rate is None:
        return False
    
    hourly_percent = (sale_rate / gift.total_amount) * 100
    
    return hourly_percent >= threshold_percent


class GiftStatistics(BaseModel):
    """Statistics for a gift."""
    gift_id: int
    total_sold: int
    current_available: int
    sale_rate_per_hour: float | None
    estimated_sold_out_seconds: int | None
    is_critical: bool
    history_entries_count: int


def calculate_statistics(gift: star_gifts_data.StarGiftData) -> GiftStatistics:
    """
    Calculates statistics for a gift.
    
    Args:
        gift: Gift data
    
    Returns:
        Gift statistics
    """
    total_sold = (gift.total_amount - gift.available_amount) if gift.is_limited else 0
    sale_rate = get_sale_rate(gift)
    sold_out_time = get_estimated_sold_out_time(gift)
    is_critical = is_critical_sale_speed(gift)
    
    return GiftStatistics(
        gift_id=gift.id,
        total_sold=total_sold,
        current_available=gift.available_amount,
        sale_rate_per_hour=sale_rate,
        estimated_sold_out_seconds=sold_out_time,
        is_critical=is_critical,
        history_entries_count=len(gift.history)
    )


class GlobalStatistics(BaseModel):
    """Global statistics for all gifts."""
    total_gifts: int
    limited_gifts: int
    available_gifts: int
    premium_only_gifts: int
    upgradable_gifts: int
    critical_gifts: int
    total_unique_gifts: int
    average_price: float
    average_convert_price: float


def calculate_global_statistics(
    gifts: list[star_gifts_data.StarGiftData]
) -> GlobalStatistics:
    """
    Calculates global statistics for all gifts.
    
    Args:
        gifts: List of all gifts
    
    Returns:
        Global statistics
    """
    total = len(gifts)
    limited = sum(1 for g in gifts if g.is_limited)
    available = sum(1 for g in gifts if g.is_limited and g.available_amount > 0)
    premium = sum(1 for g in gifts if g.require_premium)
    upgradable = sum(1 for g in gifts if g.is_upgradable)
    critical = sum(1 for g in gifts if is_critical_sale_speed(g))
    
    unique_ids = len(set(g.id for g in gifts))
    
    prices = [g.price for g in gifts if g.price > 0]
    convert_prices = [g.convert_price for g in gifts if g.convert_price > 0]
    
    avg_price = sum(prices) / len(prices) if prices else 0.0
    avg_convert_price = sum(convert_prices) / len(convert_prices) if convert_prices else 0.0
    
    return GlobalStatistics(
        total_gifts=total,
        limited_gifts=limited,
        available_gifts=available,
        premium_only_gifts=premium,
        upgradable_gifts=upgradable,
        critical_gifts=critical,
        total_unique_gifts=unique_ids,
        average_price=avg_price,
        average_convert_price=avg_convert_price
    )
