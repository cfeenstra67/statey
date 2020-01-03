"""
The Storage class provides an interface for reading and writing states
"""
import abc
from typing import Any, ContextManager, Optional


class Storage(abc.ABC):
    """
	Class encapsulating all state operations.
	"""

    @abc.abstractmethod
    def write_context(self, predicate: Any) -> ContextManager[Any]:
        """
		Context manager handling any setup or teardown associated
		with state manipulation
		"""
        raise NotImplementedError

    @abc.abstractmethod
    def read_context(self, predicate: Any) -> ContextManager[Any]:
        """
		Context manager handling any setup or teardown associated with
		reading states
		"""
        raise NotImplementedError

    @abc.abstractmethod
    def delete_context(self, predicate: Any) -> ContextManager[Any]:
        """
		Context manager handling any setup or teardown associated with
		deleting a state
		"""
        raise NotImplementedError

    def write(self, predicate: Any, context: Any, state_data: bytes) -> None:
        """
		Write the given snapshot to state storage, where the state storage
		location is given by some `predicate`
		"""
        raise NotImplementedError

    def read(self, predicate: Any, context: Any) -> Optional[bytes]:
        """
		Given a predicate, retreive the current snapshot
		"""
        raise NotImplementedError

    def delete(self, predicate: Any, context: Any) -> None:
        """
		Given a predicate, delete the current snapshot if it exists
		"""
        raise NotImplementedError
