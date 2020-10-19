import json
from cpython.string cimport PyString_AsString
from libc.stdlib cimport malloc, free


cdef char ** to_cstring_array(list_str):
	cdef char **ret = <char **>malloc(len(list_str) * sizeof(char *))
	for i in xrange(len(list_str)):
		as_bytes = list_str[i].encode()
		ret[i] = as_bytes
	return ret


cdef extern from "libstalumigo.h":

	ctypedef signed char GoInt8
	ctypedef unsigned char GoUint8
	ctypedef short GoInt16
	ctypedef unsigned short GoUint16
	ctypedef int GoInt32
	ctypedef unsigned int GoUint32
	ctypedef long long GoInt64
	ctypedef unsigned long long GoUint64
	ctypedef GoInt64 GoInt
	ctypedef GoUint64 GoUint
	ctypedef float GoFloat32
	ctypedef double GoFloat64

	struct ContextSetup_return:
		GoInt r0
		char* r1

	ContextSetup_return ContextSetup(char* name)

	struct ContextTeardown_return:
		GoInt r0
		char* r1

	ContextTeardown_return ContextTeardown(char* name)

	struct ContextListPlugins_return:
		GoInt r0
		char** r1
		GoInt r2
		char* r3

	ContextListPlugins_return ContextListPlugins(char* name)

	struct ProviderGetSchema_return:
		GoInt r0
		char* r1
		char* r2

	ProviderGetSchema_return ProviderGetSchema(char* ctxName, char* name, GoInt version)

	struct ProviderCheckConfig_return:
		GoInt r0
		char* r1
		char* r2
		char* r3

	ProviderCheckConfig_return ProviderCheckConfig(char* ctx, char* provider, char* urn, char* olds, char* news, GoUint8 allowUnknowns)

	struct ProviderDiffConfig_return:
		GoInt r0
		char* r1
		char* r2

	ProviderDiffConfig_return ProviderDiffConfig(char* ctx, char* provider, char* urn, char* olds, char* news, GoUint8 allowUnknowns, char** ignoreChanges, GoInt nIgnoreChanges)

	struct ProviderConfigure_return:
		GoInt r0
		char* r1

	ProviderConfigure_return ProviderConfigure(char* ctx, char* provider, char* inputs)

	struct ProviderCheck_return:
		GoInt r0
		char* r1
		char* r2
		char* r3

	ProviderCheck_return ProviderCheck(char* ctx, char* provider, char* urn, char* olds, char* news, GoUint8 allowUnknowns)

	struct ProviderDiff_return:
		GoInt r0
		char* r1
		char* r2

	ProviderDiff_return ProviderDiff(char* ctx, char* provider, char* urn, char* id, char* olds, char* news, GoUint8 allowUnknowns, char** ignoreChanges, GoInt nIgnoreChanges)

	struct ProviderCreate_return:
		GoInt r0
		char* r1
		char* r2

	ProviderCreate_return ProviderCreate(char* ctx, char* provider, char* urn, char* news, GoFloat64 timeout, GoUint8 preview)

	struct ProviderRead_return:
		GoInt r0
		char* r1
		char* r2

	ProviderRead_return ProviderRead(char* ctx, char* provider, char* urn, char* id, char* inputs, char* state)

	struct ProviderUpdate_return:
		GoInt r0
		char* r1
		char* r2

	ProviderUpdate_return ProviderUpdate(char* ctx, char* provider, char* urn, char* id, char* olds, char* news, GoFloat64 timeout, char** ignoreChanges, GoInt nIgnoreChanges, GoUint8 preview)

	struct ProviderDelete_return:
		GoInt r0
		GoInt r1
		char* r2

	ProviderDelete_return ProviderDelete(char* ctx, char* provider, char* urn, char* id, char* news, GoFloat64 timeout)


def context_setup(bytes ctxName):
	res = ContextSetup(ctxName)
	if res.r0 == 0:
		return None
	raise Exception('Error: %s' % res.r1)


def context_teardown(bytes ctxName):
	res = ContextTeardown(ctxName)
	if res.r0 == 0:
		return None
	raise Exception('Error: %s' % res.r1)


def context_list_plugins(bytes ctxName):
	res = ContextListPlugins(ctxName)
	if res.r0 == 0:
		return [x.decode() for x in res.r1[:res.r2]]
	raise Exception('Error: %s' % res.r3)


def provider_get_schema(bytes ctxName, bytes name, int version):
	res = ProviderGetSchema(ctxName, name, version)
	if res.r0 == 0:
		return res.r1
	raise Exception('Error: %s' % res.r2)


def provider_check_config(ctx, provider, urn, olds, news, allowUnknowns):
	olds_encoded = json.dumps(olds).encode()
	news_encoded = json.dumps(news).encode()
	res = ProviderCheckConfig(ctx, provider, urn, olds_encoded, news_encoded, allowUnknowns)
	if res.r0 == 0:
		props_decoded = json.loads(res.r1)
		failures_decoded = json.loads(res.r2)
		return props_decoded, failures_decoded
	raise Exception('Error: %s' % res.r3)


def provider_diff_config(ctx, provider, urn, olds, news, allow_unknowns, ignore_changes):
	olds_encoded = json.dumps(olds).encode()
	news_encoded = json.dumps(news).encode()
	res = ProviderDiffConfig(
		ctx, provider, urn,
		olds_encoded, news_encoded,
		allow_unknowns, to_cstring_array(ignore_changes), len(ignore_changes)
	)
	if res.r0 == 0:
		out_decoded = json.loads(res.r1)
		return out_decoded
	raise Exception('Error: %s' % res.r2)


def provider_configure(ctx, provider, inputs):
	inputs_encoded = json.dumps(inputs).encode()
	res = ProviderConfigure(ctx, provider, inputs_encoded)
	if res.r0 == 0:
		return None
	raise Exception('Error: %s' % res.r1)


def provider_check(ctx, provider, urn, olds, news, allow_unknowns):
	olds_encoded = json.dumps(olds).encode()
	news_encoded = json.dumps(news).encode()
	res = ProviderCheck(ctx, provider, urn, olds_encoded, news_encoded, allow_unknowns)
	if res.r0 == 0:
		props = json.loads(res.r1)
		failures = json.loads(res.r2)
		return props, failures
	raise Exception('Error: %s' % res.r3)


def provider_diff(ctx, provider, urn, id, olds, news, allow_unknowns, ignore_changes):
	olds_encoded = json.dumps(olds).encode()
	news_encoded = json.dumps(news).encode()
	res = ProviderDiff(
		ctx, provider, urn, id,
		olds_encoded, news_encoded,
		allow_unknowns, to_cstring_array(ignore_changes), len(ignore_changes)
	)
	if res.r0 == 0:
		out_decoded = json.loads(res.r1)
		return out_decoded
	raise Exception('Error: %s' % res.r2)


def provider_create(ctx, provider, urn, news, timeout, preview):
	news_encoded = json.dumps(news).encode()
	res = ProviderCreate(ctx, provider, urn, news_encoded, timeout, preview)
	if res.r0 == 0:
		out_decoded = json.loads(res.r1)
		return out_decoded
	raise Exception('Error: %s' % res.r2)


def provider_read(ctx, provider, urn, id, inputs, state):
	input_encoded = json.dumps(inputs).encode()
	state_encoded = json.dumps(state).encode()
	res = ProviderRead(ctx, provider, urn, id, input_encoded, state_encoded)
	if res.r0 == 0:
		out_decoded = json.loads(res.r1)
		return out_decoded
	raise Exception('Error: %s' % res.r2)


def provider_update(ctx, provider, urn, id, olds, news, timeout, ignore_changes, preview):
	olds_encoded = json.dumps(olds).encode()
	news_encoded = json.dumps(news).encode()
	res = ProviderUpdate(
		ctx, provider, urn, id,
		olds_encoded, news_encoded,
		timeout, to_cstring_array(ignore_changes), len(ignore_changes), preview
	)
	if res.r0 == 0:
		out_decoded = json.loads(res.r1)
		return out_decoded
	raise Exception('Error: %s' % res.r2)


def provider_delete(ctx, provider, urn, id, news, timeout):
	news_encoded = json.dumps(news).encode()
	res = ProviderDelete(ctx, provider, urn, id, news_encoded, timeout)
	if res.r0 == 0:
		return res.r1
	raise Exception('Error: %s', res.r2)
