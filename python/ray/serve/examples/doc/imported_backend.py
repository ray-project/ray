from ray import serve

serve.start()


@serve.deployment
class MyDeployment:
    def __call__(self, arg):
        from my_module import my_model
        self.model = my_model(arg)


MyDeployment.deploy("/model_path.pkl")
