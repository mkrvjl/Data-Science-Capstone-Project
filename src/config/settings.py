import json
import os
import time
from config.dataframe_info import DataFrameInfo

# directory where to find dataframe files
root_dir = "src/resources/info"


class ProjectSettings(object):
    _instance = None

    @staticmethod
    def _read_settings(filepath: str) -> DataFrameInfo:

        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File {filepath} was not found!")

        with open(filepath, "r") as json_file:
            data = json.loads(json_file.read())

        return DataFrameInfo.from_dict(data)

    def __new__(cls):
        if cls._instance is None:
            print('Getting dataframne settings...')
            start = time.time()

            cls._instance = super(ProjectSettings, cls).__new__(cls)
            # ----------------------------------------------------------------------------------------------------------
            # dataframe settings initialization
            # ----------------------------------------------------------------------------------------------------------

            cls.lim_admin_mtl = cls._read_settings(filepath=f"{root_dir}/limites-administratives-agglomeration.json")
            cls.crime_mtl = cls._read_settings(filepath=f"{root_dir}/actes-criminels.json")
            cls.fire_incidents = cls._read_settings(filepath=f"{root_dir}/interventions-sim.json")
            cls.fire_stations = cls._read_settings(filepath=f"{root_dir}/fire-stations.json")
            # ----------------------------------------------------------------------------------------------------------
            # directories
            # ----------------------------------------------------------------------------------------------------------
            cls.out_dir = "out"
            print('Initialized in {0: 0.2f} seconds'.format(time.time() - start))

        return cls._instance
