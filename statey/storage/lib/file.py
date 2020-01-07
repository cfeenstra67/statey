"""
The FileStorage class is the default state storage backend, and saves states
locally on the file system.
"""
import asyncio
import fcntl
import os
import tempfile
from typing import Any, AsyncContextManager, Optional

import aiofiles

from statey.storage import Storage
from statey.utils.helpers import asynccontextmanager


class FileStorage(Storage):
    """
	Storage implementation for storing states locally in files
	"""

    def __init__(self, base_path: str) -> None:
        self.base_path = base_path
        self.temp_file = None

    @staticmethod
    def ensure_path(path: str) -> None:
        """
        Ensure that the given path can be written to i.e. that its parent
        directory exists.
        """
        dirname, _ = os.path.split(path)
        if not os.path.isdir(dirname):
            os.makedirs(dirname)

    def get_path(self, predicate: Any) -> str:
        """
        Get real filesystem path given a predicate
        """
        return os.path.realpath(os.path.join(self.base_path, predicate))

    @asynccontextmanager
    async def write_context(self, predicate: Any) -> AsyncContextManager[Any]:
        """
		Context manager handling any setup or teardown associated
		with state manipulation
		"""
        path = self.get_path(predicate)
        self.ensure_path(path)
        try:
            with tempfile.NamedTemporaryFile(mode="wb+", delete=False) as file:
                # pylint: disable=not-async-context-manager
                async with self.read_context(  # acquire lock on the state file
                    predicate
                ), aiofiles.open(file.name, mode="wb") as async_file:
                    yield async_file
        # pylint: disable=try-except-raise
        except Exception:
            os.remove(file.name)
            raise
        else:
            os.rename(file.name, path)

    @asynccontextmanager
    async def read_context(self, predicate: Any) -> AsyncContextManager[Any]:
        """
		Context manager handling any setup or teardown associated with
		reading states
		"""
        path = self.get_path(predicate)
        self.ensure_path(path)
        try:
            async with aiofiles.open(path, mode="rb+") as file:
                fcntl.lockf(file, fcntl.LOCK_EX)
                try:
                    yield file
                finally:
                    fcntl.lockf(file, fcntl.LOCK_UN)
        except FileNotFoundError:
            yield None

    @asynccontextmanager
    async def delete_context(self, predicate: Any) -> AsyncContextManager[Any]:
        yield

    async def write(self, predicate: Any, context: Any, state_data: bytes) -> None:
        """
		Write the given snapshot to state storage, where the state storage
		location is given by some `predicate`
		"""
        if context is not None:
            await asyncio.gather(context.truncate(), context.seek(0))
            await context.write(state_data)
            await context.seek(0)

    async def read(self, predicate: Any, context: Any) -> Optional[bytes]:
        """
		Given a predicate, retreive the current snapshot
		"""
        if context is not None:
            await context.seek(0)
            content = await context.read()
            await context.seek(0)
            return content
        return None

    async def delete(self, predicate: Any, context: Any) -> None:
        """
		Given a predicate, delete the current snapshot if it exists
		"""
        path = self.get_path(predicate)
        if os.path.isfile(path):
            os.remove(path)
