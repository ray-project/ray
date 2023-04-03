from abc import ABC


class Instruction(ABC):
    """An instruction represents a single step in the execution schedule."""

    pass


class Send(Instruction):
    """Send data to dest rank."""

    def __init__(self, dest_rank: int):
        self.dest_rank = dest_rank


class Receive(Instruction):
    """Receive data from dest rank."""

    def __init__(self, src_rank: int):
        self.src_rank = src_rank


class Forward(Instruction):
    """Apply forward computation against the model."""

    def __init__(self, count: int):
        self.count = count


class Schedule(ABC):
    """A schedule represents the execution schedule of a stage replica."""

    def steps(self):
        """Yield a list of :class:`Instructions` for each step in the schedule."""
        pass


class Config:
    pass


class ExecutionEngine:
    """A stage replica engine represents a physical replica in the pipeline stage."""

    def __init__(self, schedule: Schedule, config: Config):
        self.input_buffer = []
        self.output_buffer = []
        self.schedule = schedule
        self.stop = False

    def initialize_config(self, config: Config):
        """Initialize the engine with a configuration."""
        pass

    def start(self):
        """Execute the replica according to the schedule."""
        pass

    def stop(self):
        """Stop the engine if it's running."""
        pass

    def check_state(self):
        """Check the state of the engine."""
        pass

    def reconfigure_schedule(self, schedule: Schedule):
        """Reconfgure the engine with a new schedule."""
        pass

    def _execute(self):
        if not self.stop:
            for instruction in self.schedule.steps():
                self._execute_instruction(instruction)

    def _execute_step(self, instruction: Instruction):
        pass
