from config.logs import get_logger
from data.processing.states.abstract_state import AbstractState
from data.processing.states.data_aggregation import DataAggregationState
from utils.enums.states import StateMachineStates

logger = get_logger(__name__)


class DataTransformationState(AbstractState):
    state_name = StateMachineStates.STATE_TRANSFORMATION
    next_state: AbstractState = DataAggregationState()

    def _action(self, dataset):
        dataset.data_transform()
        return True
