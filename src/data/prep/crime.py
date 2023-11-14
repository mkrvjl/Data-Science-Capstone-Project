import os.path
import pandas as pd

from config.data_source_info import DataSourceInfo
from config.logs import get_logger
from haversine import Unit
from data.prep.abstract_processor import DataProcessor
from data.types.geospatial_dataset import GeoSpatialDataset
from data.types.tabular_dataset import TabularDataset
from utils.exceptions import InvalidOperation

NOT_RELEVANT_FEATURES = ["X", "Y", "PDQ", "LONGITUDE", "LATITUDE"]
logger = get_logger(__name__)


class DatasetCrime(DataProcessor):
    """
    Class for processing crime dataset.
    """

    def __init__(
        self,
        dataset_settings: DataSourceInfo,
        grid_distance: int,
        grid_units: Unit,
        grid_generic_filepath: str,
        processed_root_dir: str,
        processed_file_path: str,
        remove_not_relevant: bool = True,
        drop_na_values: bool = True,
    ):
        """
        Initialize the DatasetCrime.

        Args:
            dataset_settings: DataSourceInfo object containing dataset information.
            grid_distance: Distance for grid creation.
            grid_units: Units for grid creation.
            remove_not_relevant: Whether to remove not relevant features (default: True).
            drop_na_values: Whether to drop rows with NaN values (default: True).
        """
        super().__init__(
            grid_distance=grid_distance,
            grid_units=grid_units,
            grid_generic_filepath=grid_generic_filepath,
            processed_root_dir=processed_root_dir,
            processed_file_path=processed_file_path,
        )

        self._settings = dataset_settings
        self.dataset = GeoSpatialDataset()
        self.curated_dataset = TabularDataset()
        self.aggregated_dataset = TabularDataset()
        self.grid = GeoSpatialDataset()
        self.remove_not_relevant: bool = remove_not_relevant
        self.drop_na_values: bool = drop_na_values

    @property
    def settings(self):
        return self._settings

    @property
    def dataset_local_path(self):
        return self._settings.get_local_working_file_path()

    @property
    def dataset_name(self):
        return self._settings.name

    def data_validate(self):
        """
        Validate all the data files.
        """
        self.validate_settings_batch([self.settings])

    def data_load(self):
        """
        Loads the dataset.
        """
        logger.debug(f"Reading file ({self.dataset_local_path})")

        if not os.path.exists(self.dataset_local_path):
            raise FileNotFoundError(f"Unable to find ({self.dataset_local_path})")

        self.dataset.load_from_path(self.dataset_local_path)

        # Drop row that has all NaN values
        len_crimes_data_raw = len(self.dataset.data)
        self.dataset.data = self.dataset.data.dropna(how="all")
        logger.debug(
            f"Dropped ({len_crimes_data_raw - len(self.dataset.data)}) INVALID data records!!"
        )

        if self.drop_na_values:
            len_crimes_data = len(self.dataset.data)
            self.dataset.data = self.dataset.data.dropna(
                axis=0, subset=["LONGITUDE", "LATITUDE"]
            )
            logger.debug(
                f"Dropped ({len_crimes_data - len(self.dataset.data)}) INCOMPLETE data records!!"
            )

        if self.remove_not_relevant:
            self.dataset.data.drop(columns=NOT_RELEVANT_FEATURES, axis=1, inplace=True)

        self.dataset.data = self.dataset.data.reset_index()

    def data_transform(self):
        """
        Transform the dataset.
        """

        # validate that data is loaded
        if self.dataset.data is None:
            raise InvalidOperation(
                "No data has been loaded prior to data transformation."
            )

        logger.debug(
            f"Initial dataset ({self.dataset_name}) length is : '{len(self.dataset.data)}'"
        )

        self.grid.load_from_path(local_path=self.grid_local_path)

        # make sure they're using the same projection reference and merge
        self.dataset.data.crs = self.grid.data.crs

        self.curated_dataset.data = self.dataset.data.sjoin(self.grid.data, how="left")

        # drop data not required
        self.curated_dataset.data = self.curated_dataset.data.drop_duplicates(
            subset=["index"], keep="first"
        )
        self.curated_dataset.data = self.curated_dataset.data.drop(
            labels=["geometry", "index_right"], axis=1
        )

        # clean and save data
        len_crimes_data = len(self.curated_dataset.data)
        self.curated_dataset.data = self.curated_dataset.data.dropna(
            axis=0, subset=["grid_id"]
        )
        logger.info(
            f"Dropped ({len_crimes_data - len(self.dataset.data)}) INCOMPLETE data records!!"
        )

        self.curated_dataset.data["DATE_DT"] = pd.to_datetime(
            self.curated_dataset.data["DATE"], format="%Y-%m-%d"
        )
        self.curated_dataset.data["YEAR"] = self.curated_dataset.data["DATE_DT"].dt.year
        self.curated_dataset.data["MONTH"] = self.curated_dataset.data[
            "DATE_DT"
        ].dt.month
        self.curated_dataset.data["QUARTER"] = self.curated_dataset.data[
            "DATE_DT"
        ].dt.quarter
        self.curated_dataset.data["DAY"] = self.curated_dataset.data["DATE_DT"].dt.day

        self.curated_dataset.save_data(
            filepath=self.to_local_file_path(self.dataset_name)
        )

        logger.info(f"Transformed data for ({self.dataset_name}) successfully saved")

    def data_aggregate(self):
        """
        Aggregate the data.
        """
        logger.info(f"Aggregating data ({self.dataset_name})...")

        grouped_data = (
            self.curated_dataset.data.groupby(["grid_id", "DATE", "CATEGORIE"])["index"]
            .count()
            .reset_index()
            .rename(columns={"index": "INCIDENT_COUNT"})
        )

        one_hot = pd.get_dummies(grouped_data["CATEGORIE"])
        one_hot = one_hot.multiply(grouped_data["INCIDENT_COUNT"], axis="index")

        self.aggregated_dataset.data = grouped_data.join(one_hot)
        self.aggregated_dataset.data = self.aggregated_dataset.data.sort_values(
            by="INCIDENT_COUNT", ascending=False
        ).reset_index(drop=True)
        self.aggregated_dataset.data = self.aggregated_dataset.data.drop(
            ["CATEGORIE", "INCIDENT_COUNT"], axis=1
        )

        self.aggregated_dataset.save_data(
            filepath=self.to_local_file_path(self.dataset_name)
        )

        logger.info(f"Aggregated data for ({self.dataset_name}) successfully saved")
