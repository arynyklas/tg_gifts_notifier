import logging
from logging.handlers import RotatingFileHandler

from src import constants
from src.config import LoggingConfig


class StrippingFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        record.msg = record.msg.strip() if isinstance(record.msg, str) else record.msg

        return super().format(record)


def get_logger(name: str, cfg: LoggingConfig) -> logging.Logger:
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(min(cfg.console_level, cfg.file_level))

    formatter = StrippingFormatter(
        "%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(cfg.console_level)
    console_handler.setFormatter(formatter)

    cfg.filepath.parent.mkdir(parents=True, exist_ok=True)

    file_handler = RotatingFileHandler(
        filename=str(cfg.filepath),
        mode="a",
        maxBytes=cfg.max_bytes,
        backupCount=cfg.backup_count,
        encoding=constants.ENCODING,
    )

    file_handler.setLevel(cfg.file_level)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    # Telethon logs through children of this logger (see `build_client`); clamping
    # the shared parent keeps its DEBUG firehose out of ours without losing it.
    logging.getLogger(f"{name}.{constants.TELETHON_LOGGER_CHILD}").setLevel(
        cfg.telethon_level
    )

    return logger
