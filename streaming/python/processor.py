import logging
import sys
import time
import types

logger = logging.getLogger(__name__)
logger.setLevel("INFO")


def _identity(element):
    return element


class ReadTextFile:
    """A source operator instance that reads a text file line by line.

    Attributes:
        filepath (string): The path to the input file.
    """

    def __init__(self, operator):
        self.filepath = operator.other_args
        # TODO (john): Handle possible exception here
        self.reader = open(self.filepath, "r")

    # Read input file line by line
    def run(self, input_gate, output_gate):
        while True:
            record = self.reader.readline()
            # Reader returns empty string ('') on EOF
            if not record:
                self.reader.close()
                return
            output_gate.push(
                record[:-1])  # Push after removing newline characters


class Map:
    """A map operator instance that applies a user-defined
    stream transformation.

    A map produces exactly one output record for each record in
    the input stream.

    """

    def __init__(self, operator):
        self.map_fn = operator.logic

    # Applies the mapper each record of the input stream(s)
    # and pushes resulting records to the output stream(s)
    def run(self, input_gate, output_gate):
        elements = 0
        while True:
            record = input_gate.pull()
            if record is None:
                return
            output_gate.push(self.map_fn(record))
            elements += 1


class FlatMap:
    """A map operator instance that applies a user-defined
    stream transformation.

    A flatmap produces one or more output records for each record in
    the input stream.

    Attributes:
        flatmap_fn (function): The user-defined function.
    """

    def __init__(self, operator):
        self.flatmap_fn = operator.logic

    # Applies the splitter to the records of the input stream(s)
    # and pushes resulting records to the output stream(s)
    def run(self, input_gate, output_gate):
        while True:
            record = input_gate.pull()
            if record is None:
                return
            output_gate.push_all(self.flatmap_fn(record))


class Filter:
    """A filter operator instance that applies a user-defined filter to
    each record of the stream.

    Output records are those that pass the filter, i.e. those for which
    the filter function returns True.

    Attributes:
        filter_fn (function): The user-defined boolean function.
    """

    def __init__(self, operator):
        self.filter_fn = operator.logic

    # Applies the filter to the records of the input stream(s)
    # and pushes resulting records to the output stream(s)
    def run(self, input_gate, output_gate):
        while True:
            record = input_gate.pull()
            if record is None:
                return
            if self.filter_fn(record):
                output_gate.push(record)


class Inspect:
    """A inspect operator instance that inspects the content of the stream.
    Inspect is useful for printing the records in the stream.
    """

    def __init__(self, operator):
        self.inspect_fn = operator.logic

    def run(self, input_gate, output_gate):
        # Applies the inspect logic (e.g. print) to the records of
        # the input stream(s)
        # and leaves stream unaffected by simply pushing the records to
        # the output stream(s)
        while True:
            record = input_gate.pull()
            if record is None:
                return
            if output_gate:
                output_gate.push(record)
            self.inspect_fn(record)


class Reduce:
    """A reduce operator instance that combines a new value for a key
    with the last reduced one according to a user-defined logic.
    """

    def __init__(self, operator):
        self.reduce_fn = operator.logic
        # Set the attribute selector
        self.attribute_selector = operator.other_args
        if self.attribute_selector is None:
            self.attribute_selector = _identity
        elif isinstance(self.attribute_selector, int):
            self.key_index = self.attribute_selector
            self.attribute_selector =\
                lambda record: record[self.attribute_selector]
        elif isinstance(self.attribute_selector, str):
            self.attribute_selector =\
                lambda record: vars(record)[self.attribute_selector]
        elif not isinstance(self.attribute_selector, types.FunctionType):
            sys.exit("Unrecognized or unsupported key selector.")
        self.state = {}  # key -> value

    # Combines the input value for a key with the last reduced
    # value for that key to produce a new value.
    # Outputs the result as (key,new value)
    def run(self, input_gate, output_gate):
        while True:
            record = input_gate.pull()
            if record is None:
                return
            key, rest = record
            new_value = self.attribute_selector(rest)
            # TODO (john): Is there a way to update state with
            # a single dictionary lookup?
            try:
                old_value = self.state[key]
                new_value = self.reduce_fn(old_value, new_value)
                self.state[key] = new_value
            except KeyError:  # Key does not exist in state
                self.state.setdefault(key, new_value)
            output_gate.push((key, new_value))

    # Returns the state of the actor
    def get_state(self):
        return self.state


class KeyBy:
    """A key_by operator instance that physically partitions the
    stream based on a key.
    """

    def __init__(self, operator):
        # Set the key selector
        self.key_selector = operator.other_args
        if isinstance(self.key_selector, int):
            self.key_selector = lambda r: r[self.key_selector]
        elif isinstance(self.key_selector, str):
            self.key_selector = lambda record: vars(record)[self.key_selector]
        elif not isinstance(self.key_selector, types.FunctionType):
            sys.exit("Unrecognized or unsupported key selector.")

    # The actual partitioning is done by the output gate
    def run(self, input_gate, output_gate):
        while True:
            record = input_gate.pull()
            if record is None:
                return
            key = self.key_selector(record)
            output_gate.push((key, record))


# A custom source actor
class Source:
    def __init__(self, operator):
        # The user-defined source with a get_next() method
        self.source = operator.logic

    # Starts the source by calling get_next() repeatedly
    def run(self, input_gate, output_gate):
        start = time.time()
        elements = 0
        while True:
            record = self.source.get_next()
            if not record:
                logger.debug("[writer] puts per second: {}".format(
                    elements / (time.time() - start)))
                return
            output_gate.push(record)
            elements += 1
