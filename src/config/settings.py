import json
import os

from config.dataframe_info import DataFrameInfo

# directory where to find dataframe files
root_dir = "src/resources/info"


class ProjectSettings(object):
    _instance = None

    @staticmethod
    def _read_file(filepath: str) -> DataFrameInfo:

        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File {filepath} was not found!")

        with open(filepath, "r") as json_file:
            data = json.loads(json_file.read())

        return DataFrameInfo.from_dict(data)

    def __new__(cls):
        if cls._instance is None:
            print('Creating the object')
            cls._instance = super(ProjectSettings, cls).__new__(cls)

            # ----------------------------------------------------------------------------------------------------------
            # Dataframes source files initialization
            # ----------------------------------------------------------------------------------------------------------
            cls.lim_admin_mtl = cls._read_file(filepath=f"{root_dir}/limites-administratives-agglomeration.json")
            cls.crime_mtl = cls._read_file(filepath=f"{root_dir}/actes-criminels.json")
            cls.fire_incidents = cls._read_file(filepath=f"{root_dir}/interventions-sim.json")
            cls.fire_stations = cls._read_file(filepath=f"{root_dir}/fire-stations.json")

            # ----------------------------------------------------------------------------------------------------------
            # Out directories
            # ----------------------------------------------------------------------------------------------------------
            cls.out_dir = "out"

        return cls._instance


