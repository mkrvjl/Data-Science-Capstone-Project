from config.logs import get_logger
from data.processing.states.abstract_state import AbstractState
from utils.enums.states import StateMachineStates

logger = get_logger(__name__)


class CompletedState(AbstractState):
    state_name = StateMachineStates.STATE_COMPLETED
    next_state = None

    def _action(self, dataset) -> bool:
        return True
