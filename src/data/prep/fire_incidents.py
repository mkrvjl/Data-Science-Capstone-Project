import os.path

import pandas as pd

from config.data_source_info import DataSourceInfo
from config.logs import get_logger
from haversine import Unit
from data.prep.abstract_processor import DataProcessor
from data.types.geospatial_dataset import GeoSpatialDataset
from data.types.tabular_dataset import TabularDataset
from utils.conversions import time_to_category
from utils.exceptions import InvalidOperation

NOT_RELEVANT_FEATURES = [
    "NOM_VILLE",
    "NOM_ARROND",
    "MTM8_X",
    "MTM8_Y",
    "LATITUDE",
    "LONGITUDE",
    "DIVISION",
]

CATEGORIES = {
    "DESCRIPTION_GROUPE": [
        "1-REPOND",
        "SANS FEU",
        "Alarmes-incendies",
        "AUTREFEU",
        "INCENDIE",
        "nan",
        "FAU-ALER",
        "NOUVEAU",
    ],
    "GROUP": [
        "first_responder",
        "no_fire",
        "fire_alarm",
        "other_fires",
        "fire",
        "n_a",
        "false_alarm_annulation",
        "new",
    ],
    "TYPE": ["C", "C", "C", "B", "A", "C", "C", "C"],
}


logger = get_logger(__name__)


class DatasetFireIncidents(DataProcessor):
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
        add_time_categories: bool = True,
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
        self.curated_dataset = TabularDataset()
        self.aggregated_dataset = TabularDataset()
        self.aggregated_dataset_other = TabularDataset()
        self.dataset = GeoSpatialDataset()
        self.grid = GeoSpatialDataset()
        self.remove_not_relevant: bool = remove_not_relevant
        self.add_time_categories: bool = add_time_categories

    @property
    def settings(self):
        return self._settings

    @property
    def dataset_local_path(self):
        return self._settings.get_local_working_file_path()

    @property
    def dataset_name(self):
        return self.settings.name

    def data_validate(self):
        """
        Validate all the data files.
        """
        self.validate_settings_batch([self.settings])

    def data_load(self):
        """
        Loads the dataset.
        """
        self.dataset.load_from_path(self.dataset_local_path)

        self.dataset.data.drop(columns=NOT_RELEVANT_FEATURES, axis=1, inplace=True)

        self.grid.load_from_path(local_path=self.grid_local_path)

    def data_transform(self):
        """
        Transform the dataset.
        """
        if self.dataset.data is None:
            raise InvalidOperation(
                "No data has been loaded prior to data transformation."
            )

        self.pre_process_dataset()
        self.curate_dataset()
        # save files:
        self.curated_dataset.save_data(
            filepath=self.to_local_file_path(self.dataset_name)
        )

        logger.info(f"Transformed data for ({self.dataset_name}) successfully saved")

    def data_aggregate(self):
        """
        Aggregate the data.
        """
        fires_only_mtl = self.curated_dataset.data[
            self.curated_dataset.data["TYPE"] != "C"
        ]
        no_fires_mtl = self.curated_dataset.data[
            self.curated_dataset.data["TYPE"] == "C"
        ]

        # aggregate tax rolls by grid id
        self.aggregated_dataset.data = (
            fires_only_mtl.groupby(["grid_id", "YEAR", "QUARTER"])["INCIDENT_N"]
            .count()
            .reset_index()
            .rename(columns={"INCIDENT_N": "INCIDENT_COUNT"})
        )

        self.aggregated_dataset_other.data = (
            no_fires_mtl.groupby(["grid_id", "YEAR", "QUARTER"])["INCIDENT_N"]
            .count()
            .reset_index()
            .rename(columns={"INCIDENT_N": "OTHER_FIRES_COUNT"})
        )

        # save aggregated data
        self.aggregated_dataset.save_data(
            filepath=self.to_local_file_path(self.dataset_name)
        )

        self.aggregated_dataset_other.save_data(
            filepath=self.to_local_file_path("other" + self.dataset_name)
        )

    def curate_dataset(self):
        # make sure they're using the same projection reference and merge
        self.dataset.data = self.dataset.data.to_crs(epsg=4326)
        self.dataset.data.crs = self.grid.data.crs
        self.curated_dataset.data = self.dataset.data.sjoin(
            self.grid.data, how="left", rsuffix="grid"
        )

        # some incidents are in the limits of multiple boroughts, just pick one (first) of them.
        self.curated_dataset.data = self.curated_dataset.data.drop_duplicates(
            subset=["INCIDENT_N"], keep="first"
        )
        dataset_size = len(self.curated_dataset.data)

        self.curated_dataset.data = self.curated_dataset.data.dropna(subset=["grid_id"])

        logger.debug(
            f"({dataset_size - len(self.curated_dataset.data)}) incidents were not located in the grid..."
        )

        self.curated_dataset.data.drop(columns=["index_grid"], axis=1, inplace=True)
        # preserve dtypes
        self.curated_dataset.data = self.curated_dataset.data.astype(
            {"grid_id": "int64"}
        )

    def pre_process_dataset(self):
        logger.debug(
            f"Initial dataset ({self.dataset_name}) length is : '{len(self.dataset.data)}'"
        )

        self.dataset.data["CREATION_D"] = pd.to_datetime(
            self.dataset.data["CREATION_D"]
        )

        # add date categories
        self.dataset.data["YEAR"] = self.dataset.data["CREATION_D"].dt.year
        self.dataset.data["MONTH"] = self.dataset.data["CREATION_D"].dt.month
        self.dataset.data["QUARTER"] = self.dataset.data["CREATION_D"].dt.quarter
        self.dataset.data["DAY"] = self.dataset.data["CREATION_D"].dt.day
        self.dataset.data["SHIFT"] = self.dataset.data["CREATION_D"].dt.time.map(
            time_to_category
        )

        # select only years
        # self.dataset.data = self.dataset.data[self.dataset.data["CREATION_D"].dt.year >= 2016]
        # self.dataset.data = self.dataset.data.astype(
        #     {
        #         'NOMBRE_UNI': 'int8',
        #         'DIVISION': 'int8',
        #         'CASERNE': 'int8',
        #     }
        # )
        # merge group / type
        categories_df = pd.DataFrame.from_dict(CATEGORIES)

        self.dataset.data = self.dataset.data.merge(
            categories_df, left_on="DESCRIPTIO", right_on="DESCRIPTION_GROUPE"
        )
        self.dataset.data = self.dataset.data.drop(
            labels=["INCIDENT_T", "DESCRIPTIO"], axis=1
        )
        # drop incident number
        self.dataset.data = self.dataset.data.drop(labels=["INCIDENT_N"], axis=1)
        self.dataset.data = self.dataset.data.reset_index()
        self.dataset.data = self.dataset.data.rename(columns={"index": "INCIDENT_N"})

        logger.debug(
            f"Dataset ({self.dataset_name}) length is now: '{len(self.dataset.data)}'"
        )
