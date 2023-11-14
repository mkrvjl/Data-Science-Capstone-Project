from config.logs import get_logger
from data.processing.states.abstract_state import AbstractState
from data.processing.states.data_loading import DataLoadingState
from utils.enums.states import StateMachineStates

logger = get_logger(__name__)


class DataValidationState(AbstractState):
    state_name = StateMachineStates.STATE_VALIDATION
    next_state: AbstractState = DataLoadingState()

    def _action(self, dataset):
        dataset.data_validate()
        return True
