from pydantic import BaseModel, Field
from pathlib import Path

import simplejson as json
import typing

import utils.constants as constants


class BaseConfigModel(BaseModel, extra="ignore"):
    """Base class for configuration models with extra fields ignored."""
    pass


class StarGiftHistoryEntry(BaseConfigModel):
    """Entry in the gift change history."""
    timestamp: int
    available_amount: int
    price: int
    convert_price: int
    is_upgradable: bool


class StarGiftData(BaseConfigModel):
    """Model for star gift data from Telegram."""
    id: int
    number: int
    sticker_file_id: str
    sticker_file_name: str
    price: int
    convert_price: int
    available_amount: int
    total_amount: int
    require_premium: bool = Field(default=False)
    user_limited: int | None = Field(default=None)
    is_limited: bool
    first_appearance_timestamp: int | None = Field(default=None)  # None if posted before this update
    message_id: int | None = Field(default=None)
    last_sale_timestamp: int | None = Field(default=None)
    is_upgradable: bool = Field(default=False)
    history: list[StarGiftHistoryEntry] = Field(default_factory=list)


class StarGiftsData(BaseConfigModel):
    """
    Model for storing a collection of star gifts.

    Attributes:
        DATA_FILEPATH: Path to data file (excluded from serialization)
        star_gifts: List of all loaded star gifts
    """
    DATA_FILEPATH: Path = Field(exclude=True)
    star_gifts: list[StarGiftData] = Field(default_factory=list)

    @classmethod
    def load(cls, data_filepath: Path, new: bool=False) -> "StarGiftsData":
        """
        Loads star gift data from a JSON file.

        Args:
            data_filepath: Path to the data file
            new: If True, creates a new empty collection instead of loading

        Returns:
            Loaded or new StarGiftsData instance
        """
        if new:
            return cls(
                DATA_FILEPATH = data_filepath
            )

        try:
            with data_filepath.open("r", encoding=constants.ENCODING) as file:
                return cls.model_validate({
                    **json.load(file),
                    "DATA_FILEPATH": data_filepath
                })

        except FileNotFoundError:
            return cls(
                DATA_FILEPATH = data_filepath
            )

    def save(self) -> None:
        """
        Saves star gift data to a JSON file.

        Raises:
            IOError: If failed to write the file
        """
        with self.DATA_FILEPATH.open("w", encoding=constants.ENCODING) as file:
            json.dump(
                obj = self.model_dump(),
                fp = file,
                indent = 4,
                ensure_ascii = True,
                sort_keys = False
            )
