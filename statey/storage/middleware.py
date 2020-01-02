import abc


class Middleware(abc.ABC):
	"""
	Implement arbitrary operations on state data before writing it to storage.
	A possible example could be encrypting sensitive data.
	"""
	@abc.abstractmethod
	def apply(self, state_data: bytes) -> bytes:
		"""
		Apply some transformation to the state data before it is written to state
		storage
		"""
		raise NotImplementedError

	@abc.abstractmethod
	def unapply(self, state_data: bytes) -> bytes:
		"""
		Reverse effect of the apply(state_data) function before state data is
		deserialized
		"""
		raise NotImplementedError
