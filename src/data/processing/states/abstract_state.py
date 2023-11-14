from abc import ABC, abstractmethod
from config.logs import get_logger
from typing import Any
from utils.enums.states import StateMachineStates
from data.prep.abstract_processor import DataProcessor
from pathlib import Path

logger = get_logger(__name__)


class AbstractState(ABC):
    state_name: StateMachineStates
    next_state: Any

    @abstractmethod
    def _action(self, dataset: DataProcessor) -> bool:
        pass

    def action(self, dataset: DataProcessor) -> bool:
        logger.info(
            f"Executing ({self.state_name.name}) on ({dataset.dataset_name})..."
        )

        # toggle the working directory (per state)
        dataset.working_dir = self.state_name.value

        # create the working directory if required
        Path(dataset.working_dir).mkdir(parents=True, exist_ok=True)

        # execute the processing
        return self._action(dataset=dataset)
