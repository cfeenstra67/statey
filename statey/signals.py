from contextlib import contextmanager
from typing import Callable, ContextManager, Sequence


class Signal:
	"""
	A signal is a container holding potentially multiple callbacks for a particular
	event
	"""
	def __init__(self, callbacks: Sequence[Callable[[Any], Any]] = ()) -> None:
		self.callbacks = list(callbacks)
		self._forwarding_to = None

	def naive_copy(self) -> 'Signal':
		"""
		Make a copy of the current signal without any forwarding information
		"""
		return Signal(self.callbacks)

	@contextmanager
	def forward_context(self, signal: 'Signal') -> ContextManager[None]:
		"""
		Context that allows temporarily forwarding events to the given signal
		"""
		if self._forwarding_to is not None:
			with self._forwarding_to.forward_context(signal):
				yield
			return

		self._forwarding_to = signal
		try:
			yield
		finally:
			self._forwarding_to = None

	def connect(self, func: Callable[[Any], Any]) -> None:
		"""
		Add a callback for this signal
		"""
		self.callbacks.append(func)

	def handle(self, event: Any) -> None:
		"""
		Handle an incoming event
		"""
		if self._forwarding_to is not None:
			self._forwarding_to.handle(event)
			return

		for cb in self.callbacks:
			cb(event)
