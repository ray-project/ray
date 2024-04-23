class Faker:
    """Mock Faker class to test fake_email_creator.py.

    Meant to mock https://github.com/joke2k/faker package.
    """

    def email(self) -> str:
        return "fake@fake.com"
