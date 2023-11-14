import hashlib
import os

import pandas as pd
import config.logs as logs
from data.types.abstract_serealizable import Serializable
from utils.exceptions import InvalidOperation

logger = logs.get_logger(__name__)


class TabularDataset(Serializable):
    """Class that references the scoring set and the reference (training) set.
        It returns a pandas dataframe that the query class will use.

    Attributes:
        data: The dataset represented as a pandas dataframe.
        _ds_hash: hash of the dataset.
    """

    def __init__(self, data: pd.DataFrame = None, ds_hash: str = None):
        self.data = data
        self._ds_hash = ds_hash

    @property
    def ds_hash(self):
        return self._ds_hash

    @ds_hash.setter
    def ds_hash(self, new_ds_hash):
        self._ds_hash = new_ds_hash

    def load_from_path(self, local_path: str) -> pd.DataFrame:
        """Method to load an object from the file path and returning a DataFrame.

        Args:
            local_path:

        Returns:
            Dataset as Geospatial data
        """
        if os.path.exists(local_path):
            # read file contents
            with open(local_path, "rb") as j:
                data = j.read()

            pd_hash = hashlib.md5(data).hexdigest()
            pd_data = pd.read_csv(local_path)
        else:
            raise FileNotFoundError(f"Impossible to read file {local_path}")

        self.data = pd_data.copy()
        self._ds_hash = pd_hash
        return pd_data

    def save_data(self, filepath: str):
        logger.debug(f"Saving data to ({filepath})")

        if "geometry" in self.data.columns:
            self.data = self.data.drop("geometry", axis=1)

        self.data.to_csv(filepath, index=False)

    def to_dict(self) -> dict:
        return {"ds_hash": self._ds_hash}

    @classmethod
    def from_dict(cls, dictionary: dict):
        ds_hash = dictionary["ds_hash"]
        tab_dataset = TabularDataset(ds_hash=ds_hash)
        return tab_dataset
