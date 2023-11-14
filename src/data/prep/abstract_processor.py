import datetime
import os
import urllib
import os.path
import hashlib
import os.path as path
import time
from abc import ABC, abstractmethod
from typing import List

from haversine import Unit

from config.data_source_info import DataSourceInfo
from config.logs import get_logger
from utils.exceptions import InvalidOperation

logger = get_logger(__name__)


class DataProcessor(ABC):
    """
    Abstract base class for data processing.
    """

    def __init__(
        self,
        grid_distance: int,
        grid_units: Unit,
        grid_generic_filepath: str,
        processed_root_dir: str = None,
        processed_file_path: str = None,
    ):
        """
        Initialize the DataProcessor.

        Args:
            .
        """
        self.root_dir: str = processed_root_dir
        self._working_dir: str = processed_root_dir
        self.processed_file_path: str = processed_file_path
        self.grid_distance = grid_distance
        self.grid_units = grid_units
        self.grid_generic_filepath = grid_generic_filepath

    @property
    @abstractmethod
    def dataset_name(self):
        pass

    @property
    def working_dir(self):
        return self._working_dir

    @working_dir.setter
    def working_dir(self, state_id: str):
        if self.root_dir:
            self._working_dir = os.path.join(self.root_dir, state_id)

    def to_local_file_path(self, filename: str):
        if not self.root_dir:
            raise InvalidOperation("No valid root file was set up")

        filepath_tmp = self.processed_file_path.format(
            dataset_name=filename,
            grid_distance=str(self.grid_distance),
            grid_units=str(self.grid_units.value),
        )

        return os.path.join(self.working_dir, filepath_tmp)

    @property
    def grid_local_path(self):
        return self.grid_generic_filepath.format(
            distance=str(self.grid_distance), units=str(self.grid_units.value)
        )

    @abstractmethod
    def data_load(self):
        """
        Abstract method to load the data.
        """
        pass

    @abstractmethod
    def data_transform(self):
        """
        Abstract method to transform the data.
        """
        pass

    @abstractmethod
    def data_aggregate(self):
        """
        Abstract method to aggregate the data.
        """
        pass

    @abstractmethod
    def data_validate(self):
        """
        Abstract method to aggregate the data.
        """
        pass

    @staticmethod
    def validate_settings_batch(data_settings: List[DataSourceInfo]):
        logger.debug(f"Request to validate ({len(data_settings)}) datasets...")

        for db_setting in data_settings:
            # get all local paths
            local_paths = db_setting.get_db_local_paths()

            for key, value in db_setting.remote.items():
                if value:
                    DataProcessor.validate_file(
                        remote_url=value, local_path=local_paths[key]
                    )
                else:
                    logger.debug(f"Ignoring empty key {key}...")

    @staticmethod
    def validate_file(remote_url: str, local_path: str):
        """
        Validate the local file against the remote file.

        Args:
            remote_url: Remote URL of the file.
            local_path: Local path of the file.
        """
        local_hash = None

        logger.debug(f"Validating local file {local_path}...")

        # Check if the file exists and hash its contents
        if os.path.exists(local_path):
            if not DataProcessor._is_update_required(file_path=local_path):
                return

            with open(local_path, "rb") as file:
                data = file.read()
            local_hash = hashlib.md5(data).hexdigest()

        logger.info(f"Reading remote URL {remote_url}...")
        web_url = urllib.request.Request(remote_url)
        web_url.add_header("User-Agent", "Mozilla/5.0")

        with urllib.request.urlopen(web_url) as downloaded_file:
            remote_data = downloaded_file.read()
            remote_hash = hashlib.md5(remote_data).hexdigest()

            if local_hash != remote_hash:
                DataProcessor._save_remote_file(data=remote_data, file_path=local_path)
                logger.info("File updated...")
            else:
                date = datetime.datetime.now()
                mod_time = time.mktime(date.timetuple())
                os.utime(local_path, (mod_time, mod_time))
                logger.info("File is already up-to-date...")

    @staticmethod
    def _is_update_required(file_path: str, days_since_last_update: int = 7) -> bool:
        """
        Check if the file needs to be updated based on the specified number of days since the last modification.

        Args:
            file_path: Path of the file.
            days_since_last_update: Number of days since the last modification (default: 7).

        Returns:
            True if the file needs to be updated, False otherwise.
        """
        last_modified = path.getmtime(file_path)
        if (time.time() - last_modified) / 3600 > 24 * days_since_last_update:
            logger.info(
                f"File ({file_path}) is older than ({days_since_last_update}) days, updating..."
            )
            return True
        else:
            logger.debug(
                f"File '{file_path}' is not older enough to update. Skipping..."
            )
            return False

    @staticmethod
    def _save_remote_file(data, file_path: str):
        """
        Save the data to a file.

        Args:
            data: Data to be saved.
            file_path: Path of the file.
        """
        root_dir = os.path.dirname(file_path)

        if not os.path.exists(root_dir):
            os.makedirs(root_dir)
            logger.debug(f"Directory ({root_dir}) created...")

        with open(file_path, "wb") as file:
            file.write(data)

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_path} was not found!")

        with open(file_path, "r") as file:
            if file.readable():
                logger.info(f"File '{file_path}' downloaded and ready...")
            else:
                raise IOError(f"Unable to read downloaded file '{file_path}'")
