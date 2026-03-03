# __fake_start__
from faker import Faker

from ray import serve


@serve.deployment
def create_fake_email():
    return Faker().email()


app = create_fake_email.bind()
# __fake_end__

handle = serve.run(app)
assert handle.remote().result() == "fake@fake.com"
