from transitions.extensions import GraphMachine
from functools import partial


class Model:

    def clear_state(self, deep=False, force=False):
        print("Clearing state ...")
        return True


model = Model()
machine = GraphMachine(model=model, states=['A', 'B', 'C'],
                       transitions=[
                           {'trigger': 'clear', 'source': 'B', 'dest': 'A', 'conditions': model.clear_state},
                           {'trigger': 'clear', 'source': 'C', 'dest': 'A',
                            'conditions': partial(model.clear_state, False, force=True)},
                       ],
                       initial='A', show_conditions=True)

model.get_graph().draw('my_state_diagram.png', prog='dot')