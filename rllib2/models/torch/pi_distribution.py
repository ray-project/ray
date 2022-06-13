
class PiDistribution:

    # all sampling operations preserve the backpropagation. So if that's not intended
    # user needs to wrap the method call in with torch.no_grad()
    def behavioral_sample(self, shape):
        pass

    def target_sample(self, shape):
        pass

    def log_prob(self, value):
        pass

    def entropy(self):
        pass

class DeterministicDist(PiDistribution):

    def behavioral_sample(self, shape):
        return self.action_logtis

    def target_sample(self, shape):
        return self.action_logits

    def log_prob(self, value):
        raise ValueError

    def entropy(self):
        return torch.zeros_like(self.action_logits)


class SquashedDeterministicDist(DeterministicDist):

    def behavioral_sample(self, shape):
        return super().behavioral_sample(shape).tanh()

    def target_sample(self, shape):
        return super().target_sample(shape).tanh()