import time
from typing import Generator

from user_defined_protos_pb2 import UserDefinedMessage, UserDefinedResponse

from ray import serve


@serve.deployment
class GrpcDeployment:
    def __call__(self, user_message: UserDefinedMessage) -> UserDefinedResponse:
        greeting = f"Hello {user_message.name} from {user_message.foo}"
        num_x2 = user_message.num * 2
        user_response = UserDefinedResponse(
            greeting=greeting,
            num_x2=num_x2,
        )
        return user_response

    def method1(self, user_message: UserDefinedMessage) -> UserDefinedResponse:
        greeting = f"Hello {user_message.foo} from method1"
        num_x2 = user_message.num * 3
        user_response = UserDefinedResponse(
            greeting=greeting,
            num_x2=num_x2,
        )
        return user_response

    def method2(self, user_message: UserDefinedMessage) -> UserDefinedResponse:
        greeting = "This is from method2"
        user_response = UserDefinedResponse(greeting=greeting)
        return user_response

    def streaming(
        self, user_message: UserDefinedMessage
    ) -> Generator[UserDefinedResponse, None, None]:
        for i in range(10):
            greeting = f"{i}: Hello {user_message.name} from {user_message.foo}"
            num_x2 = user_message.num * 2 + i
            user_response = UserDefinedResponse(
                greeting=greeting,
                num_x2=num_x2,
            )
            yield user_response

            time.sleep(0.1)


g = GrpcDeployment.options(name="grpc-deployment").bind()
