try:
	import aioboto3
except ImportError as err:
	raise RuntimeError('Aioboto3 is required to use AWS resources.') from err


def register() -> None:
	from . import ec2, security_group
	ec2.register()
	security_group.register()
