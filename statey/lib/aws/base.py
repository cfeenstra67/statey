import aioboto3
import contextlib


class AWSMachine:
    """
    A few helper methods for AWS resources
    """
    service: str

    @contextlib.asynccontextmanager
    async def resource_ctx(self):
        async with aioboto3.resource(self.service) as svc:
            yield svc

    @contextlib.asynccontextmanager
    async def client_ctx(self):
        async with aioboto3.client(self.service) as client:
            yield client
