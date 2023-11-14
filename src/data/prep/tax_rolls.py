import os

import pandas as pd

from config.data_source_info import DataSourceInfo
from config.logs import get_logger
from haversine import Unit
from data.prep.abstract_processor import DataProcessor
from data.types.geospatial_dataset import GeoSpatialDataset
from data.types.tabular_dataset import TabularDataset
from utils.exceptions import InvalidOperation


logger = get_logger(__name__)


class DatasetTaxRoll(DataProcessor):
    """
    Class for processing crime dataset.
    """

    def __init__(
        self,
        dataset_settings: dict,
        grid_distance: int,
        grid_units: Unit,
        grid_generic_filepath: str,
        processed_root_dir: str,
        processed_sub_dir: str,
        processed_file_path: str,
        processed_file_name: str,
    ):
        """
        Initialize the Dataset PropertyAssessment.

        Args:
            dataset_settings: DataSourceInfo object containing dataset information.
            grid_distance: Distance for grid creation.
            grid_units: Units for grid creation.
            processed_file_name: The name of the processed property assessment file.
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
        self.pre_processed_data_path = processed_sub_dir, processed_file_name
        self.pre_processed_data = None

    @property
    def settings(self):
        return self._settings

    @property
    def dataset_name(self):
        return str("tax-rolls")

    @property
    def pre_processed_data_path(self):
        return self._curated_data_path

    @pre_processed_data_path.setter
    def pre_processed_data_path(self, processed_info):
        try:
            processed_sub_dir, processed_file_name = processed_info
        except ValueError:
            raise ValueError("Pass an iterable with two items")
        else:
            self._curated_data_path = os.path.join(
                os.path.join(self.root_dir, processed_sub_dir),
                self.processed_file_path.format(
                    dataset_name=str(processed_file_name),
                    grid_distance=str(self.grid_distance),
                    grid_units=str(self.grid_units.value),
                ),
            )

    def data_validate(self):
        """
        Validate all the data files.
        """
        data_to_validate = [value for key, value in self.settings.items()]
        self.validate_settings_batch(data_to_validate)

    def data_load(self):
        """
        Loads the dataset.
        """
        if not os.path.exists(self.pre_processed_data_path):
            raise FileNotFoundError(
                f"Unable to locate file ({self.pre_processed_data_path})"
            )

        # this is to load the property assessment transformed data
        self.pre_processed_data = pd.read_csv(self.pre_processed_data_path)

        self.pre_processed_data = self.pre_processed_data.drop(
            labels=[
                "SUITE_DEBU",
                "MUNICIPALI",
                "ETAGE_HORS",
                "NOMBRE_LOG",
                "ANNEE_CONS",
                "CODE_UTILI",
                "LETTRE_DEB",
                "LETTRE_FIN",
                "LIBELLE_UT",
                "CATEGORIE_",
                "MATRICULE8",
                "SUPERFIC_1",
                "SUPERFICIE",
                "NO_ARROND_",
            ],
            axis=1,
        )

    def data_transform(self):
        """
        Transform the dataset.
        This method is not required for Dataset.
        """

        for key, value in self.settings.items():
            logger.debug(f"Processing tax-rolls ({key.value})...")
            curr_tax_roll = self.clean_tax_file(
                key.value, value.get_local_working_file_path(), self.pre_processed_data
            )
            self.curated_dataset.data = pd.concat(
                [self.curated_dataset.data, curr_tax_roll.data]
            )

        self.curated_dataset.save_data(
            filepath=self.to_local_file_path(self.dataset_name)
        )

        logger.info(f"Transformed data for ({self.dataset_name}) successfully saved")

    def clean_tax_file(
        self,
        tax_file_name: str,
        tax_file_path: str,
        unit_eval_grid: pd.DataFrame,
        get_codes_count: bool = False,
    ) -> TabularDataset:
        # read file and assign proper types to data
        tax_roll = pd.read_csv(
            tax_file_path,
            dtype={
                "ARRONDISSEMENT": int,
                "NOM_ARRONDISSEMENT": str,
                "ANNEE_EXERCICE": int,
                "ID_CUM": int,
                "NO_COMPTE": str,
                "AD_EMPLAC_CIV1": str,
                "AD_EMPLAC_CIV2": str,
                "AD_EMPLAC_GENER": str,
                "AD_EMPLAC_RUE": str,
                "AD_EMPLAC_ORIENT": str,
                "AD_EMPLAC_SUITE1": str,
                "AD_EMPLAC_SUITE2": str,
                "CODE_DESCR_LONGUE": str,
                "DESCR_LONGUE": str,
                "VAL_IMPOSABLE": float,
                "TAUX_IMPOSI": float,
                "MONTANT_DETAIL": float,
            },
        )

        # for debugging, count code types
        if get_codes_count:
            tax_codes_grouped = (
                tax_roll.groupby(["CODE_DESCR_LONGUE", "DESCR_LONGUE"])["ID_CUM"]
                .count()
                .reset_index()
            )
            print(tax_codes_grouped)

        # only get required codes
        tax_roll = tax_roll.loc[tax_roll["CODE_DESCR_LONGUE"] == "E00"]
        tax_roll = tax_roll.loc[tax_roll["ANNEE_EXERCICE"] == 2023]

        # drop data not required
        tax_roll = tax_roll.drop(
            labels=[
                "ARRONDISSEMENT",
                "NO_COMPTE",
                "NOM_ARRONDISSEMENT",
                "TAUX_IMPOSI",
                "MONTANT_DETAIL",
                "ANNEE_EXERCICE",
            ],
            axis=1,
        )

        # merge with grid
        local_tax_roll = TabularDataset()
        local_tax_roll.data = tax_roll.merge(
            unit_eval_grid, left_on="ID_CUM", right_on="ID_UEV"
        )

        local_tax_roll.save_data(filepath=self.to_local_file_path(str(tax_file_name)))

        return local_tax_roll

    def data_aggregate(self):
        """
        Aggregate the data.
        """
        # aggregate tax rolls by grid id
        self.aggregated_dataset.data = (
            self.curated_dataset.data.groupby("grid_id")
            .VAL_IMPOSABLE.agg(["sum", "mean", "count"])
            .reset_index(drop=False)
            .rename(
                columns={
                    "sum": "EVAL_SUM",
                    "mean": "EVAL_MEAN",
                    "count": "NB_TAX_PARCELS",
                }
            )
        )

        # save aggregated data
        self.aggregated_dataset.save_data(
            filepath=self.to_local_file_path(self.dataset_name)
        )

        logger.info(f"Aggregated data for ({self.dataset_name}) successfully saved")
