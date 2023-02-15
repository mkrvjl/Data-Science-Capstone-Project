import json
import os

from config.dataframe_files import DataFrameFiles
from utils.download import get_missing_files


class DataFrameInfo:
    def __init__(
            self,
            name: str,
            url: str,
            directory: str,
            description: str = None,
            local_files: DataFrameFiles = None,
            remote_files: DataFrameFiles = None,
    ):
        self.name = name
        self.description = description
        self.url = url
        self.directory = directory
        self.local = local_files
        self.remote = remote_files

        self.validate()

    def __iter__(self):
        yield from {
            "name": self.name,
            "description": self.description,
            "url": self.url,
            "directory": self.directory,
            "local": self.local,
            "remote": self.remote,
        }.items()

    def __str__(self):
        return json.dumps(self.to_dict(), ensure_ascii=False)

    def __repr__(self):
        return self.__str__()

    def to_json(self):
        return json.dumps(self.to_dict())

    def to_dict(self) -> dict:
        dictionary = {
            "name": self.name,
            "description": self.description,
            "url": self.url,
            "directory": self.directory,
            "local": self.local.to_dict(),
            "remote": self.remote.to_dict(),
        }

        return dictionary

    @classmethod
    def from_dict(cls, dictionary: dict):

        name, url, directory, description = None, None, None, None
        local_files, remote_files = None, None

        if "name" in dictionary:
            name = dictionary["name"]
        if "description" in dictionary:
            description = dictionary["description"]
        if "url" in dictionary:
            url = dictionary["url"]
        if "directory" in dictionary:
            directory = dictionary["directory"]
        if "local" in dictionary:
            local_files = DataFrameFiles.from_dict(dictionary["local"])
        if "remote" in dictionary:
            remote_files = DataFrameFiles.from_dict(dictionary["remote"])

        df_source_info = DataFrameInfo(
            name=name,
            description=description,
            url=url,
            directory=directory,
            local_files=local_files,
            remote_files=remote_files
        )

        return df_source_info

    def validate(self) -> bool:
        """Validates whether all elements of current instance are defined correctly.

        Returns:
            bool: True if valid object, False otherwise
        """
        valid = True

        if self.local.shp and not os.path.exists(self.local.shp):
            get_missing_files(
                url=self.remote.shp,
                filepath=self.local.shp
            )

        if self.local.geojson and not os.path.exists(self.local.geojson):
            get_missing_files(
                url=self.remote.geojson,
                filepath=self.local.geojson
            )

        if self.local.csv and not os.path.exists(self.local.csv):
            get_missing_files(
                url=self.remote.csv,
                filepath=self.local.csv
            )

        return valid
