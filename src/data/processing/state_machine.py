from typing import Optional
from config.logs import get_logger
from data.prep.abstract_processor import DataProcessor
from data.processing.states.abstract_state import AbstractState
from utils.enums.states import StateMachineStates

logger = get_logger(__name__)


class StateMachine:
    """
    A state machine implementation.

    The StateMachine class represents a state machine that processes a dataset based on a set of states and their
    transitions. It allows adding states, setting an initial state, and executing the state transitions until the current
    state becomes None.

    Attributes:
        _states (dict): A dictionary to store the states of the state machine, where the state name serves as the key.
        _current_state (Optional[str]): The current state of the state machine.
    """

    def __init__(self):
        self._states: dict = {}
        self._current_state: Optional[str] = None
        logger.debug("State Machine initialized...")

    @property
    def states(self) -> dict[StateMachineStates, AbstractState]:
        """
        Get the '''states''' dictionary.

        Returns:
            dict[str, AbstractState]: The dictionary of states in the state machine.
        """
        return self._states

    @states.setter
    def states(self, states: dict[StateMachineStates, AbstractState]):
        """
        Set the '''states''' dictionary.

        Args:
            states (dict[str, AbstractState]): The dictionary of states to set in the state machine.

        Returns:
            None
        """
        # Validate the state dictionary
        for state_name, state in states.items():
            if not isinstance(state_name, StateMachineStates):
                raise TypeError("State names must be StateMachineStates.")
            if not isinstance(state, AbstractState):
                raise TypeError("States must be instances of AbstractState.")
            if state_name in self._states:
                raise ValueError(f"State '{state_name}' already exists.")

        self._states = states

    @property
    def current_state(self) -> Optional[StateMachineStates]:
        """
        Get the current state.

        Returns:
            Optional[str]: The current state of the state machine.
        """
        return self._current_state

    @current_state.setter
    def current_state(self, state: str):
        """
        Set the current state.

        Args:
            state (str): The state to set as the current state.

        Returns:
            None
        """
        self._current_state = state

    def toggle_state(self):
        """
        Get the next state and assign it to the current state.

        This method retrieves the next state by calling the `next_state()` method and assigns it to the `current_state`
        attribute of the object. The `next_state()` method should return an object representing the next state.

        If the `new_state` is truthy (evaluates to True), the state name of the `new_state` object is assigned to the
        `current_state` attribute. Otherwise, if `new_state` is falsy (evaluates to False), it means that `next_state()`
        did not return a valid state object, and the `current_state` attribute is set to the value of `new_state`.

        Returns:
            None
        """

        new_state = self.next_state()

        if new_state:
            self.current_state = new_state.state_name
        else:
            self.current_state = new_state

    def next_state(self) -> Optional[AbstractState]:
        """
        Get the next state based on the current state.

        This method retrieves the next state based on the current state of the object. It looks up the current state in the
        `states` dictionary attribute and returns the `next_state` attribute of the corresponding state object.

        Returns:
            Optional[AbstractState]: The next state object if found, or None if the current state is not set or not found
            in the `states` dictionary.
        """
        if self.current_state:
            return self.states[self.current_state].next_state

    def add_state(self, state: AbstractState):
        """
        Add a state to the state machine.

        This method adds a state to the state machine by associating it with its state name. The state object is stored in the
        `states` dictionary attribute of the object, where the state name serves as the key.

        Args:
            state (AbstractState): The state object to add to the state machine.

        Returns:
            None
        """

        state_name = state.state_name

        if state_name in self._states:
            raise ValueError(f"State '{state_name}' already exists.")

        self.states[state.state_name] = state
        logger.debug(f"State name ({state.state_name}) added...")

    def set_initial_state(self, state: AbstractState):
        """
        Set the initial state of the state machine.

        This method sets the initial state of the state machine to the specified state object. It assigns the state name of
        the provided state object to the `current_state` attribute of the object.

        Args:
            state (AbstractState): The state object to set as the initial state.

        Returns:
            None
        """

        self.current_state = state.state_name
        logger.debug(f"State name ({self.current_state}) set as initial state...")

    def process(self, dataset: DataProcessor):
        """
        Process the dataset using the state machine.

        This method processes the dataset using a state machine approach. It iteratively executes the actions of each state
        until the current state becomes None. The dataset is passed as an argument to the state actions.

        Args:
            dataset (DataProcessor): The dataset to be processed.

        Raises:
            ValueError: If the current state is not a valid state name in the `states` dictionary attribute.

        Returns:
            None
        """
        logger.info(f"Processing dataset ({dataset.dataset_name})...")

        while self.current_state is not StateMachineStates.STATE_COMPLETED:
            if self.current_state not in self.states:
                raise ValueError(f"Invalid state: {self.current_state}")

            state = self.states[self.current_state]
            logger.debug(f"Current state set to ({str(state.state_name.name)})...")

            if state.action(dataset):
                self.toggle_state()

        logger.info(f"Dataset ({dataset.dataset_name}) processed successfully!")
