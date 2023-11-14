from enum import Enum


class StateMachineStates(Enum):
    STATE_AGGREGATION = "aggregated"
    STATE_LOADING = "loaded"
    STATE_TRANSFORMATION = "transformed"
    STATE_VALIDATION = "validated"
    STATE_COMPLETED = "completed"
