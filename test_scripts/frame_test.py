import sys
from functools import wraps, partial

from typing import Any, Callable


def trace_func(func: Callable[[Any], Any], cb: Callable[[Any, Any, Any], None]) -> None:

	@wraps(func)
	def wrapper(*args, **kwargs):
		prev = sys.gettrace()
		trace_frame = sys._getframe(0)

		def tracer(frame, event, arg):
			# Only immediate child frame should be the function call
			if frame.f_back is not trace_frame:
				return
			cb(frame, event, arg)
			if prev is not None:
				prev(frame, event, arg)
			return tracer

		sys.settrace(tracer)
		try:
			return func(*args, **kwargs)
		finally:
			sys.settrace(prev)

	return wrapper


def locals_history(frame):

	current_locals = frame.f_locals.copy()

	def diff():
		nonlocal current_locals

		new_locals = frame.f_locals.copy()

		updated = {
			key: loc for key, loc in new_locals.items()
			if key not in current_locals
			or loc is not current_locals[key]
		}
		deleted = {key for key in current_locals if key not in new_locals}
		current_locals = new_locals

		return updated, deleted

	diff.frame = frame

	return diff


def make_tracer(diff_cb: Callable[[Any, Any, Any], None]):

	current_history = None

	def tracer(frame, event, arg):
		nonlocal current_history

		if current_history is None:
			current_history = locals_history(frame)

		diff_cb(frame, *current_history())

		return tracer

	return tracer


def locals_diff_tracer(diff_cb: Callable[[Any, Any, Any], None]):

	tracer = make_tracer(diff_cb)

	def dec(func):
		return trace_func(func, cb=tracer)

	return dec


def scope_update_handler(handler: Callable[[str, Any], None]):

	@locals_diff_tracer
	def differ(frame, updated, deleted):
		for key, val in updated.items():
			handler(frame, key, val)

	return differ


@scope_update_handler(lambda _, x, y: print("UPDATING", x, y))
def func():
	a = 1
	b = 2
	print("after")
	return a + b


res = func()


afeafe = 1
