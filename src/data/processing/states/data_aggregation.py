from config.logs import get_logger
from data.processing.states.abstract_state import AbstractState
from data.processing.states.completed import CompletedState
from utils.enums.states import StateMachineStates

logger = get_logger(__name__)


class DataAggregationState(AbstractState):
    state_name = StateMachineStates.STATE_AGGREGATION
    next_state: AbstractState = CompletedState()

    def _action(self, dataset) -> bool:
        dataset.data_aggregate()
        return True
