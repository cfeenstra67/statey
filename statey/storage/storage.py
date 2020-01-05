"""
The Storage class provides an interface for reading and writing states
"""
import abc
from typing import Any, AsyncContextManager, Optional


class Storage(abc.ABC):
    """
	Class encapsulating all state operations.
	"""

    @abc.abstractmethod
    async def write_context(self, predicate: Any) -> AsyncContextManager[Any]:
        """
		Context manager handling any setup or teardown associated
		with state manipulation
		"""
        raise NotImplementedError

    @abc.abstractmethod
    async def read_context(self, predicate: Any) -> AsyncContextManager[Any]:
        """
		Context manager handling any setup or teardown associated with
		reading states
		"""
        raise NotImplementedError

    @abc.abstractmethod
    async def delete_context(self, predicate: Any) -> AsyncContextManager[Any]:
        """
		Context manager handling any setup or teardown associated with
		deleting a state
		"""
        raise NotImplementedError

    async def write(self, predicate: Any, context: Any, state_data: bytes) -> None:
        """
		Write the given snapshot to state storage, where the state storage
		location is given by some `predicate`
		"""
        raise NotImplementedError

    async def read(self, predicate: Any, context: Any) -> Optional[bytes]:
        """
		Given a predicate, retreive the current snapshot
		"""
        raise NotImplementedError

    async def delete(self, predicate: Any, context: Any) -> None:
        """
		Given a predicate, delete the current snapshot if it exists
		"""
        raise NotImplementedError
