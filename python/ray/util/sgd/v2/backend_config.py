class BackendConfig:
    def backend_name(self):
        pass

    def validate(self, name):
        assert name == self.backend_name()
