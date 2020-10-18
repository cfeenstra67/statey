

cdef extern from "libstalumigo.h":

	ctypedef long GoInt

	struct Setup_return:
		GoInt r0
		char* r1

	Setup_return Setup()

	struct Teardown_return:
		GoInt r0
		char* r1

	Teardown_return Teardown()

	struct GetProviderSchema_return:
		GoInt r0
		char* r1
		char* r2

	GetProviderSchema_return GetProviderSchema(char* name, GoInt version)


def setup():
	res = Setup()
	if res.r0 == 0:
		return None
	raise Exception('Error: %s' % res.r1)


def teardown():
	res = Teardown()
	if res.r0 == 0:
		return None
	raise Exception('Error: %s', res.r1)


def get_provider_schema(name, version):
	res = GetProviderSchema(name, version)
	if res.r0 == 0:
		return res.r1
	raise Exception('Error: %s', res.r2)
