from data.prep.abstract_processor import DataProcessor
from config.logs import get_logger
from data.processing.states.abstract_state import AbstractState
from data.processing.states.data_transformation import DataTransformationState
from utils.enums.states import StateMachineStates

logger = get_logger(__name__)


class DataLoadingState(AbstractState):
    state_name = StateMachineStates.STATE_LOADING
    next_state: AbstractState = DataTransformationState()

    def _action(self, dataset: DataProcessor):
        dataset.data_load()
        return True
