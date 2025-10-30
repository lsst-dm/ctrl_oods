import sys
from datetime import timedelta
from typing import ClassVar

import yaml
from pydantic import BaseModel, model_validator


class TimeInterval(BaseModel):
    """Represents a time interval with days, hours, minutes, and seconds."""

    days: int = 0
    hours: int = 0
    minutes: int = 0
    seconds: int = 0

    SECONDS_PER_DAY: ClassVar = 86400
    SECONDS_PER_HOUR: ClassVar = 3600
    SECONDS_PER_MINUTE: ClassVar = 60

    @staticmethod
    def calculate_total_seconds(config):
        """Calculate the number of seconds represented by this configuration

        Parameters
        ----------
        config: `dict`
            interval information

        Returns
        -------
        total: `int`
            Interval representation in seconds
        """
        days = config.days
        hours = config.hours
        minutes = config.minutes
        seconds = config.seconds

        total = days * TimeInterval.SECONDS_PER_DAY
        total = total + (hours * TimeInterval.SECONDS_PER_HOUR)
        total = total + (minutes * TimeInterval.SECONDS_PER_MINUTE)
        total = total + seconds
        return total

    def to_timedelta(self) -> timedelta:
        """Convert to Python timedelta object."""
        return timedelta(days=self.days, hours=self.hours, minutes=self.minutes, seconds=self.seconds)


class CollectionCleanupRule(BaseModel):
    """Rule for cleaning up a specific collection."""

    collection: str
    files_older_than: TimeInterval


class CollectionCleanerConfig(BaseModel):
    """Configuration for collection cleanup."""

    collections_to_clean: list[CollectionCleanupRule]
    cleaning_interval: TimeInterval


class ButlerConfig(BaseModel):
    """Configuration for Butler data management."""

    instrument: str
    repo_directory: str
    collections: list[str]
    collection_cleaner: CollectionCleanerConfig
    guider_max_age_seconds: int = 30


class S3ButlerConfig(ButlerConfig):
    """Configuration for Butler data management, with S3profile"""

    s3profile: str | None = None


class CacheCleanerConfig(BaseModel):
    """Configuration for cache cleanup."""

    clear_empty_directories_and_old_files: list[str]
    cleaning_interval: TimeInterval
    files_older_than: TimeInterval
    directories_empty_for_more_than: TimeInterval


class FileIngesterConfig(BaseModel):
    """Configuration for file ingestion."""

    image_staging_directory: str
    bad_file_directory: str
    staging_directory: str
    batch_size: int
    new_file_scan_interval: TimeInterval
    butler: ButlerConfig
    cache_cleaner: CacheCleanerConfig


class KafkaConfig(BaseModel):
    """Configuration for Kafka message consumption."""

    brokers: list[str]
    topics: list[str]
    group_id: str
    max_messages: int
    max_wait_time: float = 1.0


class MessageIngesterConfig(BaseModel):
    """Configuration for message ingestion."""

    kafka: KafkaConfig
    butler: S3ButlerConfig


class OODSConfig(BaseModel):
    """
    Main configuration model for OODS (Observatory Operations Data Service).

    This model can handle both file-based and message-based OODS configurations
    Must contain exactly one of file_ingester or message_ingester, but not both
    """

    FILE_INGESTER: ClassVar = "file_ingester"
    MESSAGE_INGESTER: ClassVar = "message_ingester"

    default_interval: TimeInterval

    # Exactly one of these must be present
    file_ingester: FileIngesterConfig | None = None
    message_ingester: MessageIngesterConfig | None = None

    # Optional - typically only present with file_ingester
    cache_cleaner: CacheCleanerConfig | None = None

    def ingester_config_type(self):
        if self.file_ingester is not None:
            return OODSConfig.FILE_INGESTER
        return OODSConfig.MESSAGE_INGESTER

    @model_validator(mode="after")
    def validate_ingester_config(self):
        """Ensure only one of file_ingester or message_ingester is present."""
        file_ingester = self.file_ingester
        message_ingester = self.message_ingester

        if file_ingester is not None and message_ingester is not None:
            raise ValueError("Cannot have both file_ingester and message_ingester configured")

        if file_ingester is None and message_ingester is None:
            raise ValueError("Must have either file_ingester or message_ingester configured")

        return self

    @classmethod
    def load(cls, config_file: str) -> "OODSConfig":
        try:
            with open(config_file) as file:
                config_dict = yaml.load(file, Loader=yaml.FullLoader)
            return cls.model_validate(config_dict)
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing {config_file}: {e}") from e
        except Exception as e:
            raise RuntimeError(f"Unexpected error loading {config_file}: {e}") from e


if __name__ == "__main__":

    print(f"opening {sys.argv[1]}")
    config = OODSConfig.load(sys.argv[1])
