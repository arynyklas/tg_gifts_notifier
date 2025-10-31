from pathlib import Path


ENCODING = "utf-8"
NULL_STR = ""

WORK_DIRPATH = Path(__file__).parent
LOGS_DIRPATH = WORK_DIRPATH / "logs"

LOG_FILEPATH = LOGS_DIRPATH / "main.log"


if not LOGS_DIRPATH.exists():
    LOGS_DIRPATH.mkdir()
