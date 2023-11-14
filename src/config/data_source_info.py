import json
from typing import Dict

from utils.custom_file_io import override_local_paths


class DataSourceInfo:
    def __init__(
        self,
        name: str,
        url: str,
        directory: str,
        description: str = None,
        preferred_format: str = None,
        remote_files: Dict[str, str] = None,
    ):
        """
        Initialize a DataSourceInfo object.

        Args:
            name (str): The name of the data source.
            url (str): The URL of the data source.
            directory (str): The directory where the data source is stored.
            description (str, optional): The description of the data source. Defaults to None.
            preferred_format (str, optional): The preferred format of the data source. Defaults to None.
            remote_files (Dict[str, str], optional): Dictionary of remote file paths. Defaults to None.
        """
        self.name = name
        self.description = description
        self.url = url
        self.directory = directory
        self.working_db_format: str = preferred_format
        self.remote: Dict[str, str] = remote_files

    def __iter__(self):
        """
        Allow the object to be iterated by yielding key-value pairs of its attributes.
        """
        yield from {
            "name": self.name,
            "description": self.description,
            "url": self.url,
            "directory": self.directory,
            "working_db_format": self.working_db_format,
            "remote": self.remote,
        }.items()

    def __str__(self):
        """
        Return a JSON string representation of the object.
        """
        return json.dumps(self.to_dict(), ensure_ascii=False)

    def __repr__(self):
        """
        Return a string representation of the object.
        """
        return self.__str__()

    def to_json(self):
        """
        Return a JSON string representation of the object.
        """
        return json.dumps(self.to_dict())

    def to_dict(self) -> dict:
        """
        Convert the object to a dictionary representation.

        Returns:
            dict: A dictionary representing the object.
        """
        dictionary = {
            "name": self.name,
            "description": self.description,
            "url": self.url,
            "directory": self.directory,
            "working_db_format": self.working_db_format,
            "remote": self.remote,
        }

        return dictionary

    @classmethod
    def from_dict(cls, dictionary: dict):
        """
        Create a DataSourceInfo object from a dictionary.

        Args:
            dictionary (dict): The dictionary containing the data source information.

        Returns:
            DataSourceInfo: The created DataSourceInfo object.
        """

        name, url, directory, description = None, None, None, None
        working_db_format, local_files, remote_files = None, None, None

        if "name" in dictionary:
            name = dictionary["name"]
        if "description" in dictionary:
            description = dictionary["description"]
        if "url" in dictionary:
            url = dictionary["url"]
        if "directory" in dictionary:
            directory = dictionary["directory"]
        if "working_db_format" in dictionary:
            working_db_format = dictionary["working_db_format"]
        if "remote" in dictionary:
            remote_files = cls.parse_dict(dictionary["remote"])

        df_source_info = DataSourceInfo(
            name=name,
            description=description,
            url=url,
            directory=directory,
            preferred_format=working_db_format,
            remote_files=remote_files,
        )

        return df_source_info

    @classmethod
    def parse_dict(cls, dictionary: dict) -> Dict[str, str]:
        """
        Parse a dictionary to ensure that each key is unique.

        Args:
            dictionary (dict): The dictionary to parse.

        Returns:
            Dict[str, str]: The parsed dictionary.

        Raises:
            ImportError: If a key is not valid (not unique).
        """
        tmp_dict = {}
        for key, value in dictionary.items():
            if key not in tmp_dict.keys():
                tmp_dict[key] = value
            else:
                raise ImportError(f"Key {key} is not valid. Please validate file!")

        return tmp_dict

    def get_db_local_paths(self) -> dict:
        """
        Get the local file paths for each remote file format.

        Returns:
            dict: A dictionary mapping file formats to their corresponding local file paths.
        """
        local_paths = {
            file_format: override_local_paths(
                {file_format: file_path}, root_dir=self.directory
            )[file_format]
            for file_format, file_path in self.remote.items()
        }
        return local_paths

    def get_local_working_file_path(self) -> str:
        """
        Get the local file path for the working database format.

        Returns:
            str: The local file path for the working database format.
        """
        local_paths = self.get_db_local_paths()
        return local_paths[self.working_db_format]
