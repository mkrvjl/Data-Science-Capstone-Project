import json

from config.dataframe_files import DataFrameFiles


class DataFrameInfo:
    def __init__(
            self,
            name: str,
            url: str,
            directory: str,
            local_files: DataFrameFiles = None,
            remote_files: DataFrameFiles = None,
    ):
        self.name = name
        self.url = url
        self.directory = directory
        self.local = local_files
        self.remote = remote_files

    def __iter__(self):
        yield from {
            "label": self.name,
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
            "url": self.url,
            "directory": self.directory,
            "local": self.local.to_dict(),
            "remote": self.remote.to_dict(),
        }

        return dictionary

    @classmethod
    def from_dict(cls, dictionary: dict):

        name, url, directory, local_files, remote_files = None, None, None, None, None

        if "name" in dictionary:
            name = dictionary["name"]
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
            url=url,
            directory=directory,
            local_files=local_files,
            remote_files=remote_files
        )

        return df_source_info
