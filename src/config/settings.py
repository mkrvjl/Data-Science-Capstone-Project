import json
import os
import time
import pathlib

from haversine import Unit

from config import logs
from config.data_source_info import DataSourceInfo
from utils.custom_file_io import get_absolute_path
from utils.enums.databases import ExternalDatabases

logger = logs.get_logger(__name__)

# directory where to find dataframe files
root_dir = "resources/config"


class ProjectSettings(object):
    _instance = None

    @staticmethod
    def _read_settings(filepath: str) -> DataSourceInfo:
        """
        Read settings from a JSON file.

        Args:
            filepath (str): The path to the JSON file.

        Returns:
            The parsed settings as a dictionary.
        Raises:
            FileNotFoundError: If the specified file does not exist.
        """
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File {filepath} was not found!")

        with open(filepath, "r") as json_file:
            data = json.loads(json_file.read())

        return DataSourceInfo.from_dict(data)

    @staticmethod
    def _get_databases(root_dir: str):
        """
        Recursively get the database settings from a root directory.

        Args:
            root_dir (str): The root directory path.

        Returns:
            A dictionary of database settings, where the keys are the database names and
            the values are the corresponding settings.

        """
        database_settings = {}
        current_dict = database_settings

        for path, sub_dirs, files in os.walk(root_dir):
            for file in files:
                file_path = os.path.join(path, file)
                file_name = pathlib.Path(file_path).stem
                current_dict[
                    ExternalDatabases(file_name)
                ] = ProjectSettings._read_settings(file_path)

            for sub_dir in sub_dirs:
                current_dict = current_dict.setdefault(ExternalDatabases(sub_dir), {})

        return database_settings

    def __new__(cls):
        if cls._instance is None:
            logger.debug("Getting dataframe settings...")
            start = time.time()

            cls._instance = super(ProjectSettings, cls).__new__(cls)

            # ----------------------------------------------------------------------------------------------------------
            # prep settings initialization
            # ----------------------------------------------------------------------------------------------------------
            cls.databases = cls._get_databases(root_dir)
            # ----------------------------------------------------------------------------------------------------------
            # directories
            # ----------------------------------------------------------------------------------------------------------
            cls.out_dir = "./out"
            cls.processed_root_dir = "resources/data/processed/"
            cls.processed_file_path = "{dataset_name}_{grid_distance}_{grid_units}.csv"

            # ----------------------------------------------------------------------------------------------------------
            # other parameters
            # ----------------------------------------------------------------------------------------------------------
            cls.log_file = get_absolute_path(
                parent_dir_path=cls.out_dir, sub_dir_path="logs.log"
            )
            cls.DAYS_SINCE_LAST_UPDATE = 7
            cls.grid_shp_filepath = (
                "resources/data/raw/grids/grid_{distance}_{units}.shp"
            )
            cls.grid_distance = 500
            cls.grid_units = Unit.METERS
            # ----------------------------------------------------------------------------------------------------------
            # initialization timing
            # ----------------------------------------------------------------------------------------------------------
            logger.debug(
                "Settings fetched in {0: 0.2f} seconds".format(time.time() - start)
            )
        return cls._instance

    def validate_data_integrity(self):
        logger.info("Setting up required folders")
        os.makedirs(self.out_dir, exist_ok=True)
