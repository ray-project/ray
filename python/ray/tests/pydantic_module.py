from fastapi import FastAPI
from pydantic import BaseModel


class User(BaseModel):
    name: str


user = User(name="a")

app = FastAPI()


@app.get("/")
def h(u: User) -> str:
    return "a"


def closure():
    app = FastAPI()

    @app.get("/")
    def h(u: User) -> str:
        return "a"

    return app
