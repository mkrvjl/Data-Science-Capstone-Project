import os
import pandas as pd

from config.data_source_info import DataSourceInfo
from config.logs import get_logger
from haversine import Unit
from data.prep.abstract_processor import DataProcessor
from data.types.geospatial_dataset import GeoSpatialDataset
from data.types.tabular_dataset import TabularDataset
from utils.exceptions import InvalidOperation

NOT_RELEVANT_FEATURES = [
    "SUITE_DEBUT",
    "MUNICIPALITE",
    "ETAGE_HORS_SOL",
    "NOMBRE_LOGEMENT",
    "ANNEE_CONSTRUCTION",
    "CODE_UTILISATION",
    "LETTRE_DEBUT",
    "LETTRE_FIN",
    "LIBELLE_UTILISATION",
    "CATEGORIE_UEF",
    "MATRICULE83",
    "SUPERFICIE_TERRAIN",
    "SUPERFICIE_BATIMENT",
    "NO_ARROND_ILE_CUM",
]
logger = get_logger(__name__)


class DatasetPropertyAssessment(DataProcessor):
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
    ):
        """
        Initialize the Dataset PropertyAssessment.

        Args:
            dataset_settings: DataSourceInfo object containing dataset information.
            grid_distance: Distance for grid creation.
            grid_units: Units for grid creation.
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
        self.aggreated_dataset = TabularDataset()
        self.grid = GeoSpatialDataset()

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
        if not os.path.exists(self.dataset_local_path):
            raise FileNotFoundError(f"Unable to find ({self.dataset_local_path})")

        self.dataset.load_from_path(self.dataset_local_path)

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
        self.dataset.data = self.dataset.data.to_crs(epsg=4326)
        self.dataset.data.crs = self.grid.data.crs
        self.curated_dataset.data = self.dataset.data.sjoin(self.grid.data, how="left")

        # drop data not required
        self.curated_dataset.data = self.curated_dataset.data.drop(
            labels=["geometry", "index_right"], axis=1
        )

        # create bins for age
        year_ranges = [
            float("-inf"),
            1900,
            1920,
            1940,
            1960,
            1980,
            2000,
            2020,
            2023,
            float("inf"),
        ]
        labels = [
            "ANNEE_CONSTR_1900",
            "ANNEE_CONSTR_1900-1920",
            "ANNEE_CONSTR_1920-1940",
            "ANNEE_CONSTR_1940-1960",
            "ANNEE_CONSTR_1960-1980",
            "ANNEE_CONSTR_1980-2000",
            "ANNEE_CONSTR_2000-2020",
            "ANNEE_CONSTR_2020-2023",
            "ANNEE_CONSTR_2023",
        ]

        # Create a new column with the construction year range for each entry
        self.curated_dataset.data["ANNEE_CONS_CATEGORY"] = pd.cut(
            self.curated_dataset.data["ANNEE_CONS"],
            bins=year_ranges,
            labels=labels,
            right=False,
        )
        # create bins for utilisation
        self.curated_dataset.data["IS_CONDOMINIUMS"] = self.curated_dataset.data[
            "LIBELLE_UT"
        ].str.contains("logement", case=False, regex=True) & self.curated_dataset.data[
            "CATEGORIE_"
        ].str.contains(
            "condominium", case=False, regex=True
        )

        self.curated_dataset.data["IS_LOGEMENT"] = (
            self.curated_dataset.data["LIBELLE_UT"].str.contains(
                "logement", case=False, regex=True
            )
            & ~self.curated_dataset.data["CATEGORIE_"].str.contains(
                "condominium", case=False, regex=True
            )
            & ~self.curated_dataset.data["IS_CONDOMINIUMS"]
        )

        self.curated_dataset.data["IS_OUTSIDE"] = (
            self.curated_dataset.data["LIBELLE_UT"].str.contains(
                "parc|stationnement|non aménagé", case=False, regex=True
            )
            & ~self.curated_dataset.data["IS_CONDOMINIUMS"]
            & ~self.curated_dataset.data["IS_LOGEMENT"]
        )

        self.curated_dataset.data["IS_MIXED"] = (
            ~self.curated_dataset.data["IS_CONDOMINIUMS"]
            & ~self.curated_dataset.data["IS_LOGEMENT"]
            & ~self.curated_dataset.data["IS_OUTSIDE"]
        )

        # clean and save data
        self.curated_dataset.data = self.curated_dataset.data.drop_duplicates()

        self.curated_dataset.save_data(
            filepath=self.to_local_file_path(self.dataset_name)
        )

        logger.info(f"Transformed data for ({self.dataset_name}) successfully saved")

    def data_aggregate(self):
        """
        Aggregate the data.
        This method is not required for Crime Dataset.
        """

        # Pivot the data to get Year Range values as columns
        self.aggreated_dataset.data = (
            self.curated_dataset.data.groupby(["grid_id", "ANNEE_CONS_CATEGORY"])
            .size()
            .unstack(fill_value=0)
        )

        self.aggreated_dataset.data["N_FLOOR_AVG"] = self.curated_dataset.data.groupby(
            "grid_id"
        )["ETAGE_HORS"].mean()

        self.aggreated_dataset.data[
            "N_LOGEMENT_SUM"
        ] = self.curated_dataset.data.groupby("grid_id")["NOMBRE_LOG"].sum()

        self.aggreated_dataset.data["N_BUILDINGS"] = self.curated_dataset.data.groupby(
            "grid_id"
        )["ID_UEV"].nunique()

        self.aggreated_dataset.data[
            "LAND_AREA_AVG"
        ] = self.curated_dataset.data.groupby("grid_id")["SUPERFICIE"].sum()

        self.aggreated_dataset.data[
            "BUILD_TOT_AREA_AVG"
        ] = self.curated_dataset.data.groupby("grid_id")["SUPERFIC_1"].sum()

        self.aggreated_dataset.data[
            "BUILDING_CONDOMINIUM_COUNT"
        ] = self.curated_dataset.data.groupby("grid_id")["IS_CONDOMINIUMS"].sum()

        self.aggreated_dataset.data[
            "BUILDING_LOGEMENTS_COUNT"
        ] = self.curated_dataset.data.groupby("grid_id")["IS_LOGEMENT"].sum()

        self.aggreated_dataset.data[
            "BUILDING_MIXED_COUNT"
        ] = self.curated_dataset.data.groupby("grid_id")["IS_MIXED"].sum()

        self.aggreated_dataset.data[
            "BUILDING_OUTSIDE_COUNT"
        ] = self.curated_dataset.data.groupby("grid_id")["IS_OUTSIDE"].sum()

        self.aggreated_dataset.data = self.aggreated_dataset.data.reset_index()

        self.aggreated_dataset.save_data(
            filepath=self.to_local_file_path(self.dataset_name)
        )

        logger.info(f"Aggregated data for ({self.dataset_name}) successfully saved")
