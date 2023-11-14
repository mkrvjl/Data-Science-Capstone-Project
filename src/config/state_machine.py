from typing import List
from data.prep.abstract_processor import DataProcessor
from data.processing.state_machine import StateMachine
from data.processing.states.completed import CompletedState
from data.processing.states.data_aggregation import DataAggregationState
from data.processing.states.data_loading import DataLoadingState
from data.processing.states.data_transformation import DataTransformationState
from data.processing.states.data_validation import DataValidationState


sm = StateMachine()

# Add states to the state machine
sm.add_state(DataValidationState())
sm.add_state(DataLoadingState())
sm.add_state(DataTransformationState())
sm.add_state(DataAggregationState())
sm.add_state(CompletedState())


def process_datasets(datasets: List[DataProcessor]):
    # Process the state machine
    for dataset in datasets:
        # Set the initial state
        sm.set_initial_state(DataValidationState())

        sm.process(dataset)
