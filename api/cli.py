"""
CLI tool for working with gift data.
"""
import argparse
import csv
import json
from pathlib import Path
import sys
from datetime import datetime

from core import star_gifts_data
import features.history_manager as history_manager
import features.backup_manager as backup_manager
import utils.utils as utils
import utils.constants as constants
import config


def print_gift(gift: star_gifts_data.StarGiftData, detailed: bool = False):
    """Prints information about gift."""
    print(f"ID: {gift.id}")
    print(f"Number: {gift.number}")
    print(f"Price: {gift.price} ⭐")
    print(f"Convert Price: {gift.convert_price} ⭐")
    
    if gift.is_limited:
        print(f"Total: {gift.total_amount:,}")
        print(f"Available: {gift.available_amount:,}")
        print(f"Sold: {(gift.total_amount - gift.available_amount):,}")
        if gift.total_amount > 0:
            sold_percent = ((gift.total_amount - gift.available_amount) / gift.total_amount) * 100
            print(f"Sold Percent: {sold_percent:.2f}%")
    
    if gift.require_premium:
        print("Premium: Yes")
    
    if gift.user_limited:
        print(f"User Limited: {gift.user_limited}")
    
    if gift.is_upgradable:
        print("Upgradable: Yes")
    
    if gift.first_appearance_timestamp:
        dt = datetime.fromtimestamp(gift.first_appearance_timestamp)
        print(f"First Appearance: {dt.strftime('%Y-%m-%d %H:%M:%S')}")
    
    if detailed:
        stats = history_manager.calculate_statistics(gift)
        print(f"\nStatistics:")
        print(f"  Total Sold: {stats.total_sold:,}")
        print(f"  Sale Rate: {stats.sale_rate_per_hour:.2f} gifts/hour" if stats.sale_rate_per_hour else "  Sale Rate: N/A")
        
        if stats.estimated_sold_out_seconds:
            days = stats.estimated_sold_out_seconds // 86400
            hours = (stats.estimated_sold_out_seconds % 86400) // 3600
            print(f"  Estimated Sold Out: ~{days}d {hours}h")
        
        print(f"  Critical: {'Yes' if stats.is_critical else 'No'}")
        print(f"  History Entries: {stats.history_entries_count}")
    
    print()


def search_gift(gift_id: int, detailed: bool = False):
    """Search for gift by ID."""
    try:
        data = star_gifts_data.StarGiftsData.load(config.DATA_FILEPATH)
        gift = next((g for g in data.star_gifts if g.id == gift_id), None)
        
        if gift is None:
            print(f"Gift with ID {gift_id} not found")
            return False
        
        print_gift(gift, detailed)
        return True
    
    except Exception as e:
        print(f"Error: {e}")
        return False


def list_gifts(limited_only: bool = False, available_only: bool = False):
    """List all gifts."""
    try:
        data = star_gifts_data.StarGiftsData.load(config.DATA_FILEPATH)
        gifts = data.star_gifts
        
        if limited_only:
            gifts = [g for g in gifts if g.is_limited]
        
        if available_only:
            gifts = [g for g in gifts if g.is_limited and g.available_amount > 0]
        
        print(f"Found {len(gifts)} gifts")
        print()
        
        for gift in gifts:
            print(f"ID {gift.id}: {gift.price}⭐ - Available: {gift.available_amount if gift.is_limited else '∞'}")
    
    except Exception as e:
        print(f"Error: {e}")


def show_history(gift_id: int, limit: int = 10):
    """Show gift history."""
    try:
        data = star_gifts_data.StarGiftsData.load(config.DATA_FILEPATH)
        gift = next((g for g in data.star_gifts if g.id == gift_id), None)
        
        if gift is None:
            print(f"Gift with ID {gift_id} not found")
            return False
        
        if not gift.history:
            print(f"No history for gift {gift_id}")
            return True
        
        history = gift.history[-limit:] if len(gift.history) > limit else gift.history
        
        print(f"History for gift {gift_id} (last {len(history)} entries):")
        print()
        
        for entry in history:
            dt = datetime.fromtimestamp(entry.timestamp)
            print(f"{dt.strftime('%Y-%m-%d %H:%M:%S')}: Available={entry.available_amount}, Price={entry.price}⭐")
        
        return True
    
    except Exception as e:
        print(f"Error: {e}")
        return False


def export_csv(output_file: str):
    """Export data to CSV."""
    try:
        data = star_gifts_data.StarGiftsData.load(config.DATA_FILEPATH)
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Headers
            writer.writerow([
                'ID', 'Number', 'Price', 'Convert Price', 'Available', 'Total',
                'Is Limited', 'Require Premium', 'User Limited', 'Is Upgradable',
                'First Appearance', 'Last Sale', 'History Size'
            ])
            
            # Data
            for gift in data.star_gifts:
                first_appearance = ''
                if gift.first_appearance_timestamp:
                    first_appearance = datetime.fromtimestamp(gift.first_appearance_timestamp).isoformat()
                
                last_sale = ''
                if gift.last_sale_timestamp:
                    last_sale = datetime.fromtimestamp(gift.last_sale_timestamp).isoformat()
                
                writer.writerow([
                    gift.id, gift.number, gift.price, gift.convert_price,
                    gift.available_amount, gift.total_amount,
                    gift.is_limited, gift.require_premium, gift.user_limited, gift.is_upgradable,
                    first_appearance, last_sale, len(gift.history)
                ])
        
        print(f"Exported {len(data.star_gifts)} gifts to {output_file}")
        return True
    
    except Exception as e:
        print(f"Error: {e}")
        return False


def show_statistics():
    """Show global statistics."""
    try:
        data = star_gifts_data.StarGiftsData.load(config.DATA_FILEPATH)
        stats = history_manager.calculate_global_statistics(data.star_gifts)
        
        print("Global Statistics:")
        print()
        print(f"Total Gifts: {stats.total_gifts}")
        print(f"Limited Gifts: {stats.limited_gifts}")
        print(f"Available Gifts: {stats.available_gifts}")
        print(f"Premium Only: {stats.premium_only_gifts}")
        print(f"Upgradable: {stats.upgradable_gifts}")
        print(f"Critical: {stats.critical_gifts}")
        print(f"Unique IDs: {stats.total_unique_gifts}")
        print(f"Average Price: {stats.average_price:.2f} ⭐")
        print(f"Average Convert Price: {stats.average_convert_price:.2f} ⭐")
    
    except Exception as e:
        print(f"Error: {e}")


def backup_data(output_dir: str | None = None):
    """Create a backup of data."""
    try:
        backup_dir = Path(output_dir) if output_dir else None
        backup_file = backup_manager.create_backup(config.DATA_FILEPATH, backup_dir)
        
        if backup_file:
            print(f"Backup created: {backup_file}")
            return True
        else:
            print("Failed to create backup")
            return False
    
    except Exception as e:
        print(f"Error: {e}")
        return False


def restore_data(backup_file: str):
    """Restore data from backup."""
    try:
        if backup_manager.restore_from_backup(Path(backup_file), config.DATA_FILEPATH):
            print(f"Restored from backup: {backup_file}")
            return True
        else:
            print("Failed to restore from backup")
            return False
    
    except Exception as e:
        print(f"Error: {e}")
        return False


def list_backups_cmd():
    """List all backups."""
    try:
        backups = backup_manager.list_backups()
        
        if not backups:
            print("No backups found")
            return
        
        print(f"Found {len(backups)} backups:")
        print()
        
        for backup in backups:
            dt = datetime.fromtimestamp(backup.stat().st_mtime)
            size_kb = backup.stat().st_size / 1024
            print(f"{backup.name}: {dt.strftime('%Y-%m-%d %H:%M:%S')}, {size_kb:.2f} KB")
    
    except Exception as e:
        print(f"Error: {e}")


def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(description="TG Gifts Notifier CLI")
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Search command
    search_parser = subparsers.add_parser('search', help='Search for a gift by ID')
    search_parser.add_argument('gift_id', type=int, help='Gift ID')
    search_parser.add_argument('-d', '--detailed', action='store_true', help='Show detailed information')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List all gifts')
    list_parser.add_argument('-l', '--limited', action='store_true', help='Only limited gifts')
    list_parser.add_argument('-a', '--available', action='store_true', help='Only available gifts')
    
    # History command
    history_parser = subparsers.add_parser('history', help='Show gift history')
    history_parser.add_argument('gift_id', type=int, help='Gift ID')
    history_parser.add_argument('-n', '--limit', type=int, default=10, help='Number of entries to show')
    
    # Export command
    export_parser = subparsers.add_parser('export', help='Export data to CSV')
    export_parser.add_argument('output', help='Output file path')
    
    # Statistics command
    stats_parser = subparsers.add_parser('stats', help='Show global statistics')
    
    # Backup command
    backup_parser = subparsers.add_parser('backup', help='Create a backup')
    backup_parser.add_argument('-d', '--dir', help='Backup directory')
    
    # Restore command
    restore_parser = subparsers.add_parser('restore', help='Restore from backup')
    restore_parser.add_argument('backup_file', help='Backup file to restore from')
    
    # List backups command
    list_backups_parser = subparsers.add_parser('backups', help='List all backups')
    
    args = parser.parse_args()
    
    if args.command == 'search':
        sys.exit(0 if search_gift(args.gift_id, args.detailed) else 1)
    elif args.command == 'list':
        list_gifts(args.limited, args.available)
    elif args.command == 'history':
        sys.exit(0 if show_history(args.gift_id, args.limit) else 1)
    elif args.command == 'export':
        sys.exit(0 if export_csv(args.output) else 1)
    elif args.command == 'stats':
        show_statistics()
    elif args.command == 'backup':
        sys.exit(0 if backup_data(args.dir) else 1)
    elif args.command == 'restore':
        sys.exit(0 if restore_data(args.backup_file) else 1)
    elif args.command == 'backups':
        list_backups_cmd()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

