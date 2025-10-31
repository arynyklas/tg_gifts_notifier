"""
Module for validating configuration at application startup.

Checks correctness of all configuration parameters before launch.
"""
import logging
from pathlib import Path
from pytz import all_timezones

import config


class ConfigValidationError(Exception):
    """Exception for configuration validation errors."""
    pass


def validate_config() -> None:
    """
    Validates configuration before application startup.

    Raises:
        ConfigValidationError: If configuration errors are detected
    """
    errors: list[str] = []

    # Check API credentials
    if not isinstance(config.API_ID, int) or config.API_ID <= 0:
        errors.append("API_ID must be a positive integer")

    if not isinstance(config.API_HASH, str) or len(config.API_HASH) < 10:
        errors.append("API_HASH must be a non-empty string")

    # Check bot tokens
    if not isinstance(config.BOT_TOKENS, list) or len(config.BOT_TOKENS) == 0:
        errors.append("BOT_TOKENS must be a non-empty list")
    else:
        for i, token in enumerate(config.BOT_TOKENS):
            if not isinstance(token, str):
                errors.append(f"BOT_TOKENS[{i}] must be a string")
            elif ":" not in token:
                errors.append(f"BOT_TOKENS[{i}] must be in format 'token_id:token_hash'")

    # Check intervals
    if not isinstance(config.CHECK_INTERVAL, (int, float)) or config.CHECK_INTERVAL <= 0:
        errors.append("CHECK_INTERVAL must be a positive number")

    if not isinstance(config.CHECK_UPGRADES_PER_CYCLE, (int, float)) or config.CHECK_UPGRADES_PER_CYCLE <= 0:
        errors.append("CHECK_UPGRADES_PER_CYCLE must be a positive number")

    if not isinstance(config.DATA_SAVER_DELAY, (int, float)) or config.DATA_SAVER_DELAY <= 0:
        errors.append("DATA_SAVER_DELAY must be a positive number")

    # Check paths
    if not isinstance(config.DATA_FILEPATH, Path):
        errors.append("DATA_FILEPATH must be a Path object")
    else:
        # Check that directory exists or can be created
        try:
            config.DATA_FILEPATH.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            errors.append(f"Cannot create directory for DATA_FILEPATH: {e}")

    # Check chat IDs
    if not isinstance(config.NOTIFY_CHAT_ID, int):
        errors.append("NOTIFY_CHAT_ID must be an integer")

    if config.NOTIFY_UPGRADES_CHAT_ID is not None:
        if not isinstance(config.NOTIFY_UPGRADES_CHAT_ID, int):
            errors.append("NOTIFY_UPGRADES_CHAT_ID must be an integer or None")

    # Check delays
    if not isinstance(config.NOTIFY_AFTER_STICKER_DELAY, (int, float)) or config.NOTIFY_AFTER_STICKER_DELAY < 0:
        errors.append("NOTIFY_AFTER_STICKER_DELAY must be a non-negative number")

    if not isinstance(config.NOTIFY_AFTER_TEXT_DELAY, (int, float)) or config.NOTIFY_AFTER_TEXT_DELAY < 0:
        errors.append("NOTIFY_AFTER_TEXT_DELAY must be a non-negative number")

    # Check timezone
    if not isinstance(config.TIMEZONE, str):
        errors.append("TIMEZONE must be a string")
    elif config.TIMEZONE not in all_timezones:
        errors.append(f"TIMEZONE '{config.TIMEZONE}' is not a valid timezone. "
                     f"Use one from pytz.all_timezones")

    # Check log levels
    valid_log_levels = {
        logging.DEBUG, logging.INFO, logging.WARNING,
        logging.ERROR, logging.CRITICAL
    }
    if config.CONSOLE_LOG_LEVEL not in valid_log_levels:
        errors.append(f"CONSOLE_LOG_LEVEL must be one of {valid_log_levels}")

    if config.FILE_LOG_LEVEL not in valid_log_levels:
        errors.append(f"FILE_LOG_LEVEL must be one of {valid_log_levels}")

    # Check timeout
    if not isinstance(config.HTTP_REQUEST_TIMEOUT, (int, float)) or config.HTTP_REQUEST_TIMEOUT <= 0:
        errors.append("HTTP_REQUEST_TIMEOUT must be a positive number")

    # Check text formats
    required_format_keys = {
        "title", "number", "id", "total_amount", "available_amount",
        "sold_out", "price", "convert_price", "require_premium_or_user_limited"
    }
    
    try:
        format_keys = set(config.NOTIFY_TEXT.split("{")[1:])
        format_keys = {key.split("}")[0] for key in format_keys}
        missing_keys = required_format_keys - format_keys
        if missing_keys:
            errors.append(f"NOTIFY_TEXT missing required format keys: {missing_keys}")
    except Exception as e:
        errors.append(f"Cannot parse NOTIFY_TEXT format: {e}")

    if errors:
        error_message = "Configuration validation failed:\n" + "\n".join(f"  - {error}" for error in errors)
        raise ConfigValidationError(error_message)

