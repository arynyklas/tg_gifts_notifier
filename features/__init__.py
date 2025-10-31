"""
Features package - Advanced functionality modules.
"""

from features.history_manager import *
from features.filters_and_priorities import *
from features.backup_manager import *

__all__ = [
    # History manager
    "add_history_entry",
    "get_sale_rate",
    "get_estimated_sold_out_time",
    "is_critical_sale_speed",
    "calculate_statistics",
    "calculate_global_statistics",
    "GiftStatistics",
    "GlobalStatistics",
    # Filters and priorities
    "NotificationPriority",
    "FilterConfig",
    "check_filter",
    "calculate_priority",
    "should_send_notification",
    # Backup manager
    "create_backup",
    "restore_from_backup",
    "list_backups",
    "cleanup_old_backups",
    "auto_backup_task",
]

