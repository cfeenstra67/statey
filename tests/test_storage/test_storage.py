import os

import pytest
from statey.storage.lib.file import FileStorage


@pytest.fixture
def storage(tmpdir):
	return FileStorage(tmpdir)


@pytest.mark.asyncio
async def test_file_storage(storage, tmpdir):
	predicate = 'state.json'
	
	async with storage.write_context(predicate) as ctx:
		await storage.write(predicate, ctx, b'Test')

	with open(os.path.join(tmpdir, predicate)) as f:
		assert f.read() == 'Test'

	async with storage.read_context(predicate) as ctx:
		assert b'Test' == await storage.read(predicate, ctx)


@pytest.mark.asyncio
async def test_file_storage_error(storage, tmpdir):
	predicate = 'state.json'

	async with storage.write_context(predicate) as ctx:
		await storage.write(predicate, ctx, b'Test ABC ABC.')

	with open(os.path.join(tmpdir, predicate)) as f:
		assert f.read() == 'Test ABC ABC.'

	# No issue w/ an error in write context
	CustomException = type('CustomException', (Exception,), {})

	try:
		async with storage.write_context(predicate) as ctx:
			raise CustomException
	except CustomException:
		pass
	else:
		assert False, 'This should have raised an error.'

	with open(os.path.join(tmpdir, predicate)) as f:
		assert f.read() == 'Test ABC ABC.'

	# Even if we've already written something
	try:
		async with storage.write_context(predicate) as ctx:
			await storage.write(predicate, ctx, b'Test ABCD.')
			raise CustomException
	except CustomException:
		pass
	else:
		assert False, 'This should have raised an error.'

	with open(os.path.join(tmpdir, predicate)) as f:
		assert f.read() == 'Test ABC ABC.'

	# No issue w/ an error in read context
	try:
		async with storage.read_context(predicate) as ctx:
			raise CustomException
	except CustomException:
		pass
	else:
		assert False, 'This should have raised an error.'

	with open(os.path.join(tmpdir, predicate)) as f:
		assert f.read() == 'Test ABC ABC.'
