import json
from abc import ABC, abstractmethod
from config.logs import get_logger
from typing import Callable

logger = get_logger(__name__)


class Serializable(ABC):
    @abstractmethod
    def to_dict(self) -> dict:
        pass

    @classmethod
    @abstractmethod
    def from_dict(cls, dictionary: dict):
        pass

    @classmethod
    @abstractmethod
    def save_data(cls, filepath: str):
        pass

    def export_json(self, path: str) -> None:
        dictionary = self.to_dict()
        with open(file=path, mode="w") as f:
            json.dump(dictionary, f, indent=4)

    @classmethod
    def from_json_path(cls, contract_path: str) -> Callable:
        with open(contract_path, "r") as f:
            dictionary = json.load(f)
        return cls.from_dict(dictionary)

    def __eq__(self, other):
        return self.to_dict() == other.to_dict()

    def copy(self):
        return self.__class__.from_dict(self.to_dict())
