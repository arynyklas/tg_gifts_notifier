"""
Module for managing data backup.
"""
import shutil
import glob
from pathlib import Path
import typing
from datetime import datetime

from core import star_gifts_data
import utils.constants as constants
import config

logger = typing.cast(typing.Any, None)  # Will be set during initialization


def init_logger(logger_instance: typing.Any) -> None:
    """Initializes logger for this module."""
    global logger
    logger = logger_instance


def create_backup(
    source_filepath: Path,
    backup_dir: Path | None = None,
    max_backups: int = 10
) -> Path | None:
    """
    Creates a backup of data file.
    
    Args:
        source_filepath: Path to source file
        backup_dir: Directory for backups (default backup/)
        max_backups: Maximum number of backups to store
    
    Returns:
        Path to created backup or None on error
    """
    try:
        if backup_dir is None:
            backup_dir = constants.WORK_DIRPATH / "backups"
        
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        # Create backup name with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"{source_filepath.stem}_{timestamp}.json"
        backup_filepath = backup_dir / backup_filename
        
        # Copy file
        shutil.copy2(source_filepath, backup_filepath)
        
        if logger:
            logger.info(f"Created backup: {backup_filepath}")
        
        # Clean up old backups
        cleanup_old_backups(backup_dir, max_backups)
        
        return backup_filepath
    
    except Exception as e:
        if logger:
            logger.error(f"Failed to create backup: {e}", exc_info=True)
        return None


def cleanup_old_backups(backup_dir: Path, max_backups: int) -> None:
    """
    Removes old backups, keeping only last max_backups.
    
    Args:
        backup_dir: Directory with backups
        max_backups: Maximum number of backups to store
    """
    try:
        backup_pattern = str(backup_dir / "*.json")
        backups = sorted(glob.glob(backup_pattern), key=Path.stat, reverse=False)
        
        if len(backups) > max_backups:
            to_delete = backups[:len(backups) - max_backups]
            for backup_path in to_delete:
                Path(backup_path).unlink()
                if logger:
                    logger.debug(f"Deleted old backup: {backup_path}")
    except Exception as e:
        if logger:
            logger.error(f"Failed to cleanup old backups: {e}", exc_info=True)


def restore_from_backup(backup_filepath: Path, target_filepath: Path) -> bool:
    """
    Restores data from backup.
    
    Args:
        backup_filepath: Path to backup file
        target_filepath: Path to target file for restoration
    
    Returns:
        True if restoration successful
    """
    try:
        if not backup_filepath.exists():
            if logger:
                logger.error(f"Backup file not found: {backup_filepath}")
            return False
        
        # Create backup of current file before restoring
        if target_filepath.exists():
            create_backup(target_filepath, max_backups=1)
        
        # Restore from backup
        shutil.copy2(backup_filepath, target_filepath)
        
        if logger:
            logger.info(f"Restored from backup: {backup_filepath}")
        
        return True
    
    except Exception as e:
        if logger:
            logger.error(f"Failed to restore from backup: {e}", exc_info=True)
        return False


def list_backups(backup_dir: Path | None = None) -> list[Path]:
    """
    Gets list of all available backups.
    
    Args:
        backup_dir: Directory with backups (default backup/)
    
    Returns:
        List of paths to backups sorted by creation date
    """
    try:
        if backup_dir is None:
            backup_dir = constants.WORK_DIRPATH / "backups"
        
        if not backup_dir.exists():
            return []
        
        backup_pattern = str(backup_dir / "*.json")
        backups = sorted(glob.glob(backup_pattern), key=Path.stat, reverse=True)
        
        return [Path(b) for b in backups]
    
    except Exception as e:
        if logger:
            logger.error(f"Failed to list backups: {e}", exc_info=True)
        return []


def auto_backup_task(
    source_filepath: Path,
    interval_seconds: float = 3600.0,
    backup_dir: Path | None = None,
    max_backups: int = 24
) -> None:
    """
    Periodically creates automatic backups.
    
    Args:
        source_filepath: Path to source file
        interval_seconds: Interval between backups in seconds
        backup_dir: Directory for backups
        max_backups: Maximum number of backups to store
    """
    import asyncio
    import time
    
    if logger:
        logger.info(f"Starting auto-backup task with interval {interval_seconds}s")
    
    while True:
        try:
            asyncio.sleep(interval_seconds)
            
            if source_filepath.exists():
                create_backup(source_filepath, backup_dir, max_backups)
        except KeyboardInterrupt:
            if logger:
                logger.info("Auto-backup task stopped by user")
            break
        except Exception as e:
            if logger:
                logger.error(f"Auto-backup error: {e}", exc_info=True)
                asyncio.sleep(60)  # Wait one minute before next attempt
