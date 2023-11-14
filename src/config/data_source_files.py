import json
import os
from typing import Dict

from utils.enums.dataset_enums import DatasetEnums


class DataSourceFiles:
    def __init__(
        self, shp_path: str = None, geojson_path: str = None, csv_path: str = None
    ):
        if not shp_path and not geojson_path and not csv_path:
            raise ValueError(
                "Please specify at least one SHP, CSV or GEOJSON file path"
            )

        self.local: Dict[DatasetEnums, str] = {}
        self.remote: Dict[DatasetEnums, str] = {}

        self.shp = shp_path
        self.geojson = geojson_path
        self.csv = csv_path

    def __iter__(self):
        yield from {"shp": self.shp, "geojson": self.geojson, "csv": self.csv}.items()

    def __str__(self):
        return json.dumps(self.to_dict(), ensure_ascii=False)

    def __repr__(self):
        return self.__str__()

    def to_json(self):
        return json.dumps(self.to_dict())

    def to_dict(self) -> dict:
        dictionary = {"shp": self.shp, "geojson": self.geojson, "csv": self.csv}

        return dictionary

    @classmethod
    def from_dict(cls, dictionary: dict):
        shp_path, geojson_path, csv_path = None, None, None
        if "shp" in dictionary:
            shp_path = dictionary["shp"]
        if "geojson" in dictionary:
            geojson_path = dictionary["geojson"]
        if "csv" in dictionary:
            csv_path = dictionary["csv"]

        df_source_info = DataSourceFiles(
            shp_path=shp_path, geojson_path=geojson_path, csv_path=csv_path
        )
        return df_source_info
