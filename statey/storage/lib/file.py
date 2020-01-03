"""
The FileStorage class is the default state storage backend, and saves states locally on the file system.
"""
import fcntl
import os
import tempfile
from contextlib import contextmanager
from typing import Any, ContextManager, Optional

from statey.storage import Storage


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
        Ensure that the given path can be written to i.e. that its parent directory exists.
        """
        dirname, _ = os.path.split(path)
        if not os.path.isdir(dirname):
            os.makedirs(dirname)

    def get_path(self, predicate: Any) -> str:
        """
        Get real filesystem path given a predicate
        """
        return os.path.realpath(os.path.join(self.base_path, predicate))

    @contextmanager
    def write_context(self, predicate: Any) -> ContextManager[Any]:
        """
		Context manager handling any setup or teardown associated
		with state manipulation
		"""
        path = self.get_path(predicate)
        self.ensure_path(path)
        try:
            with tempfile.NamedTemporaryFile(mode="wb+", delete=False) as file:
                with self.read_context(predicate):  # acquire lock on the state file
                    yield file
        # pylint: disable=try-except-raise
        except Exception:
            raise
        else:
            os.rename(file.name, path)

    @contextmanager
    def read_context(self, predicate: Any) -> ContextManager[Any]:
        """
		Context manager handling any setup or teardown associated with
		reading states
		"""
        path = self.get_path(predicate)
        self.ensure_path(path)
        try:
            with open(path, "rb+") as file:
                fcntl.lockf(file, fcntl.LOCK_EX)
                try:
                    yield file
                finally:
                    fcntl.lockf(file, fcntl.LOCK_UN)
        except FileNotFoundError:
            yield None

    @contextmanager
    def delete_context(self, predicate: Any) -> ContextManager[Any]:
        yield

    def write(self, predicate: Any, context: Any, state_data: bytes) -> None:
        """
		Write the given snapshot to state storage, where the state storage
		location is given by some `predicate`
		"""
        if context is not None:
            context.truncate()
            context.seek(0)
            context.write(state_data)
            context.seek(0)

    def read(self, predicate: Any, context: Any) -> Optional[bytes]:
        """
		Given a predicate, retreive the current snapshot
		"""
        if context is not None:
            context.seek(0)
            content = context.read()
            context.seek(0)
            return content
        return None

    def delete(self, predicate: Any, context: Any) -> None:
        """
		Given a predicate, delete the current snapshot if it exists
		"""
        path = self.get_path(predicate)
        if os.path.isfile(path):
            os.remove(path)
