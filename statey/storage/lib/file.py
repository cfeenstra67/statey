import fcntl
import os
from contextlib import contextmanager
from typing import Any, ContextManager

from .storage import Storage


class FileStorage(Storage):
	"""
	Storage implementation for storing states locally in files
	"""
	def __init__(self, base_path: str) -> None:
		self.base_path = base_path

	def get_path(self, predicate: Any) -> str:
		return os.path.realpath(os.path.join(self.base_path, predicate))

	def ensure_path(self, path: str) -> None:
		dirname, _ = os.path.split(path)
		os.makedirs(dirname)

	@contextmanager
	def write_context(self, predicate: Any) -> ContextManager[Any]:
		"""
		Context manager handling any setup or teardown associated
		with state manipulation
		"""
		path = self.get_path(predicate)
		self.ensure_path(path)
		with os.open(path, 'wb+') as f:
			fcntl.lockf(f, fcntl.LOCK_EX)
			try:
				yield f
			finally:
				fcntl.lockf(f, fcntl.LOCK_UN)

	@contextmanager
	def read_context(self, predicate: Any) -> ContextManager[Any]:
		"""
		Context manager handling any setup or teardown associated with
		reading states
		"""
		path = self.get_path(predicate)
		self.ensure_path(path)
		with open(path, 'rb+') as f:
			fcntl.lockf(f, fcntl.LOCK_EX)
			try:
				yield f
			finally:
				fcntl.lockf(f, fcntl.LOCK_UN)

	@contextmanager
	def delete_context(self, predicate: Any) -> ContextManager[Any]:
		yield

	def write(self, predicate: Any, context: Any, state_data: bytes) -> None:
		"""
		Write the given snapshot to state storage, where the state storage
		location is given by some `predicate`
		"""
		if context is not None:
			context.write(state_data)
			context.seek(0)

	def read(self, predicate: Any, context: Any) -> Optional[bytes]:
		"""
		Given a predicate, retreive the current snapshot
		"""
		if context is not None:
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
