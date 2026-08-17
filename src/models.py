from pydantic import BaseModel, ConfigDict, Field


class StarGiftData(BaseModel):
    # `extra="ignore"` drops the legacy v1 `sticker_file_id` / `sticker_file_name` keys.
    model_config = ConfigDict(extra="ignore")

    id: int
    number: int
    price: int
    convert_price: int
    available_amount: int
    total_amount: int
    require_premium: bool = False
    user_limited: int | None = None
    is_limited: bool
    first_appearance_timestamp: int | None = None
    last_sale_timestamp: int | None = None
    sticker_message_id: int | None = None
    upgrade_sticker_message_id: int | None = None
    message_id: int | None = None
    is_upgradable: bool = False
    # `None` means Telegram advertises no upgrade price for this gift.
    upgrade_stars: int | None = None
    # Set when an announcement was intended, cleared when its text is posted, so
    # a run interrupted mid-announcement resumes instead of losing the gift.
    announce_pending: bool = False


class StarGiftsFile(BaseModel):
    model_config = ConfigDict(extra="ignore")

    version: int = 1  # legacy files have no "version" key
    star_gifts: list[StarGiftData] = Field(default_factory=list)
