import wandb

class Logger:
    def __init__(self, config):
        self.step = 0
        self.config = config

        self.setup()

    def setup(self):
        pass

    def commit_batch(self):
        self.step += 1

    def log_object(self, name, x):
        pass

    def log_image(self, name, img):
        pass

    def log_images(self, name, imgs):
        pass

    def log_video(self, name, vid):
        pass

    def log_audio(self, name, audio):
        pass

    def log_table(self, name, table):
        pass

    def log_html(self, name, html):
        pass

    def log_histogram(self, name, histogram):
        pass

    def log_3d(self, name, data):
        pass

    def log_summary(self, name, x):
        pass

class WandbLogger(Logger):
    def setup(self):
        # https://docs.wandb.com/library/advanced/environment-variables
        wandb.init(project="ray-sgd-neural-image-compression")

    def log_object(self, name, x):
        wandb.log({name: x}, step=self.step)

    def log_image(self, name, img):
        wandb.log({name: wandb.Image(img)}, step=self.step)

    # todo: this can also log JPEG if you only pass one image
    def log_images(self, name, imgs):
        wandb.log({name: [wandb.Image(img) for img in imgs]}, step=self.step)

    def log_summary(self, name, x):
        wandb.run.summary[name] = x
