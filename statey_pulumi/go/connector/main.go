package main

import "C"

import (
	"encoding/json"
	"fmt"
	"os"
	"unsafe"

	"stalumi"
	"github.com/pulumi/pulumi/sdk/v2/go/common/resource"
	"github.com/pulumi/pulumi/sdk/v2/go/common/util/cmdutil"
	"github.com/pulumi/pulumi/sdk/v2/go/common/tokens"
)

//export ContextSetup
func ContextSetup(name *C.char) (int, *C.char) {
	cwd, err := os.Getwd()
	if err != nil {
		return -1, C.CString(fmt.Sprintf("error getting cwd: %v", err))
	}

	sink := cmdutil.Diag()

	goName := C.GoString(name)

	if err := stalumi.SetupContext(goName, cwd, sink, sink); err != nil {
		return -1, C.CString(fmt.Sprintf("error setting up context: %v", err))
	}

	ctx, err := stalumi.GetContext(goName)
	if err != nil {
		return -1, C.CString(fmt.Sprintf("error getting context: %v", err))
	}

	if err := ctx.InstallPlugins(); err != nil {
		return -1, C.CString(fmt.Sprintf("error installing plugins: %v", err))
	}

	return 0, nil
}


//export ContextTeardown
func ContextTeardown(name *C.char) (int, *C.char) {
	goName := C.GoString(name)
	if err := stalumi.CloseContext(goName); err != nil {
		return -1, C.CString(fmt.Sprintf("error closing context: %v", err))
	}
	return 0, nil
}

//export ContextListPlugins
func ContextListPlugins(name *C.char) (int, **C.char, int, *C.char) {
	ctx, err := stalumi.GetContext(C.GoString(name))
	if err != nil {
		return -1, nil, -1, C.CString(fmt.Sprintf("error getting context: %v", err))
	}

	plugins := ctx.ListPlugins()
	pluginsLen := C.int(len(plugins))
	cArray := C.malloc(C.size_t(pluginsLen) * C.size_t(unsafe.Sizeof(uintptr(0))))

	a := (*[1 << 30 - 1]*C.char)(cArray)

	for i, plug := range plugins {
		a[i] = C.CString(plug.Name)
	}

	return 0, (**C.char)(cArray), len(plugins), nil
}

//export ProviderGetSchema
func ProviderGetSchema(ctxName *C.char, name *C.char, version int) (int, *C.char, *C.char) {
	goCtxName := C.GoString(ctxName)
	provider, err := stalumi.Provider(goCtxName, tokens.Package(C.GoString(name)))
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error getting provider: %v", err))
	}

	response, err := (*provider).GetSchema(version)
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error getting schema: %v", err))
	}

	return 0, C.CString(string(response)), nil
}

//export ProviderCheckConfig
func ProviderCheckConfig(
	ctx *C.char,
	provider *C.char,
	urn *C.char,
	olds *C.char,
	news *C.char,
	allowUnknowns bool,
) (int, *C.char, *C.char, *C.char) {
	providerObj, err := stalumi.Provider(C.GoString(ctx), tokens.Package(C.GoString(provider)))
	if err != nil {
		return -1, nil, nil, C.CString(fmt.Sprintf("error getting provider: %v", err))
	}

	oldMap, err := stalumi.JSONToPropertyMap([]byte(C.GoString(olds)))
	if err != nil {
		return -1, nil, nil, C.CString(fmt.Sprintf("error marshalling olds: %v", err))
	}

	newMap, err := stalumi.JSONToPropertyMap([]byte(C.GoString(news)))
	if err != nil {
		return -1, nil, nil, C.CString(fmt.Sprintf("error marshalling news: %v", err))
	}

	urnValue := resource.URN(C.GoString(urn))

	props, failures, err := (*providerObj).CheckConfig(urnValue, oldMap, newMap, allowUnknowns)
	if err != nil {
		return -1, nil, nil, C.CString(fmt.Sprintf("error checking config: %v", err))
	}

	failuresOut, err := json.Marshal(failures)
	if err != nil {
		return -1, nil, nil, C.CString(fmt.Sprintf("error marshalling failures: %v", err))
	}

	propsOut, err := stalumi.PropertyMapToJSON(props)
	if err != nil {
		return -1, nil, nil, C.CString(fmt.Sprintf("error marshalling props: %v", err))
	}

	return 0, C.CString(string(propsOut)), C.CString(string(failuresOut)), nil
}

//export ProviderDiffConfig
func ProviderDiffConfig(
	ctx *C.char,
	provider *C.char,
	urn *C.char,
	olds *C.char,
	news *C.char,
	allowUnknowns bool,
	ignoreChanges **C.char,
	nIgnoreChanges int,
) (int, *C.char, *C.char) {

	providerObj, err := stalumi.Provider(C.GoString(ctx), tokens.Package(C.GoString(provider)))
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error getting provider: %v", err))
	}

	oldMap, err := stalumi.JSONToPropertyMap([]byte(C.GoString(olds)))
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error marshalling olds: %v", err))
	}

	newMap, err := stalumi.JSONToPropertyMap([]byte(C.GoString(news)))
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error marshalling news: %v", err))
	}

	urnValue := resource.URN(C.GoString(urn))

	ignoreChangesSlice := (*[1 << 28]*C.char)(unsafe.Pointer(ignoreChanges))[:nIgnoreChanges:nIgnoreChanges]
	var ignoreChangesStrings []string

	for _, s := range ignoreChangesSlice {
		ignoreChangesStrings = append(ignoreChangesStrings, C.GoString(s))
	}

	result, err := (*providerObj).DiffConfig(urnValue, oldMap, newMap, allowUnknowns, ignoreChangesStrings)
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error diffing config: %v", err))
	}

	resultEncoded, err := json.Marshal(result)
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error marshalling result: %v", err))
	}

	return 0, C.CString(string(resultEncoded)), nil
}

//export ProviderConfigure
func ProviderConfigure(ctx *C.char, provider *C.char, inputs *C.char) (int, *C.char) {
	providerObj, err := stalumi.Provider(C.GoString(ctx), tokens.Package(C.GoString(provider)))
	if err != nil {
		return -1, C.CString(fmt.Sprintf("error getting provider: %v", err))
	}

	data, err := stalumi.JSONToPropertyMap([]byte(C.GoString(inputs)))
	if err != nil {
		return -1, C.CString(fmt.Sprintf("error unmarshalling inputs: %v", err))
	}

	if err := (*providerObj).Configure(data); err != nil {
		return -1, C.CString(fmt.Sprintf("error configuring provider: %v", err))
	}

	return 0, nil
}

//export ProviderCheck
func ProviderCheck(
	ctx *C.char,
	provider *C.char,
	urn *C.char,
	olds *C.char,
	news *C.char,
	allowUnknowns bool,
) (int, *C.char, *C.char, *C.char) {
	providerObj, err := stalumi.Provider(C.GoString(ctx), tokens.Package(C.GoString(provider)))
	if err != nil {
		return -1, nil, nil, C.CString(fmt.Sprintf("error getting provider: %v", err))
	}

	oldMap, err := stalumi.JSONToPropertyMap([]byte(C.GoString(olds)))
	if err != nil {
		return -1, nil, nil, C.CString(fmt.Sprintf("error marshalling olds: %v", err))
	}

	newMap, err := stalumi.JSONToPropertyMap([]byte(C.GoString(news)))
	if err != nil {
		return -1, nil, nil, C.CString(fmt.Sprintf("error marshalling news: %v", err))
	}

	urnValue := resource.URN(C.GoString(urn))

	props, failures, err := (*providerObj).Check(urnValue, oldMap, newMap, allowUnknowns)
	if err != nil {
		return -1, nil, nil, C.CString(fmt.Sprintf("error checking config: %v", err))
	}

	failuresOut, err := json.Marshal(failures)
	if err != nil {
		return -1, nil, nil, C.CString(fmt.Sprintf("error marshalling failures: %v", err))
	}

	propsOut, err := stalumi.PropertyMapToJSON(props)
	if err != nil {
		return -1, nil, nil, C.CString(fmt.Sprintf("error marshalling props: %v", err))
	}

	return 0, C.CString(string(propsOut)), C.CString(string(failuresOut)), nil
}

//export ProviderDiff
func ProviderDiff(
	ctx *C.char,
	provider *C.char,
	urn *C.char,
	id *C.char,
	olds *C.char,
	news *C.char,
	allowUnknowns bool,
	ignoreChanges **C.char,
	nIgnoreChanges int,
) (int, *C.char, *C.char) {

	providerObj, err := stalumi.Provider(C.GoString(ctx), tokens.Package(C.GoString(provider)))
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error getting provider: %v", err))
	}

	oldMap, err := stalumi.JSONToPropertyMap([]byte(C.GoString(olds)))
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error marshalling olds: %v", err))
	}

	newMap, err := stalumi.JSONToPropertyMap([]byte(C.GoString(news)))
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error marshalling news: %v", err))
	}

	urnValue := resource.URN(C.GoString(urn))
	idValue := resource.ID(C.GoString(id))

	ignoreChangesSlice := (*[1 << 28]*C.char)(unsafe.Pointer(ignoreChanges))[:nIgnoreChanges:nIgnoreChanges]
	var ignoreChangesStrings []string

	for _, s := range ignoreChangesSlice {
		ignoreChangesStrings = append(ignoreChangesStrings, C.GoString(s))
	}

	result, err := (*providerObj).Diff(
		urnValue, idValue,
		oldMap, newMap,
		allowUnknowns, ignoreChangesStrings,
	)
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error diffing config: %v", err))
	}

	resultEncoded, err := json.Marshal(result)
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error marshalling result: %v", err))
	}

	return 0, C.CString(string(resultEncoded)), nil
}

type ProviderCreateResponse struct {
	ID resource.ID
	Properties json.RawMessage
	Status resource.Status
}

//export ProviderCreate
func ProviderCreate(
	ctx *C.char,
	provider *C.char,
	urn *C.char,
	news *C.char,
	timeout float64,
	preview bool,
) (int, *C.char, *C.char) {

	providerObj, err := stalumi.Provider(C.GoString(ctx), tokens.Package(C.GoString(provider)))
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error getting provider: %v", err))
	}

	newMap, err := stalumi.JSONToPropertyMap([]byte(C.GoString(news)))
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error marshalling news: %v", err))
	}

	urnValue := resource.URN(C.GoString(urn))

	id, props, status, err := (*providerObj).Create(urnValue, newMap, timeout, preview)
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error creating resource: %v", err))
	}

	propsJson, err := stalumi.PropertyMapToJSON(props)
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error encoding properties: %v", err))
	}

	resp := ProviderCreateResponse{
		ID: id,
		Properties: json.RawMessage(propsJson),
		Status: status,
	}

	encodedResp, err := json.Marshal(resp)
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error encoding response: %v", err))
	}

	return 0, C.CString(string(encodedResp)), nil

}

type ProviderReadResponse struct {
	ID resource.ID
	Inputs json.RawMessage
	Outputs json.RawMessage
	Status resource.Status
}

//export ProviderRead
func ProviderRead(
	ctx *C.char,
	provider *C.char,
	urn *C.char,
	id *C.char,
	inputs *C.char,
	state *C.char,
) (int, *C.char, *C.char) {

	providerObj, err := stalumi.Provider(C.GoString(ctx), tokens.Package(C.GoString(provider)))
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error getting provider: %v", err))
	}

	oldMap, err := stalumi.JSONToPropertyMap([]byte(C.GoString(inputs)))
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error marshalling olds: %v", err))
	}

	newMap, err := stalumi.JSONToPropertyMap([]byte(C.GoString(state)))
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error marshalling news: %v", err))
	}

	urnValue := resource.URN(C.GoString(urn))
	idValue := resource.ID(C.GoString(id))

	result, status, err := (*providerObj).Read(urnValue, idValue, oldMap, newMap)
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error diffing config: %v", err))
	}

	inputsEncoded, err := stalumi.PropertyMapToJSON(result.Inputs)
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error encoding inputs: %v", err))
	}

	outputsEncoded, err := stalumi.PropertyMapToJSON(result.Outputs)
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error encoding outputs: %v", err))
	}

	resp := ProviderReadResponse{
		ID: idValue,
		Inputs: json.RawMessage(inputsEncoded),
		Outputs: json.RawMessage(outputsEncoded),
		Status: status,
	}

	resultEncoded, err := json.Marshal(resp)
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error marshalling result: %v", err))
	}

	return 0, C.CString(string(resultEncoded)), nil
}

//export ProviderUpdate
func ProviderUpdate(
	ctx *C.char,
	provider *C.char,
	urn *C.char,
	id *C.char,
	olds *C.char,
	news *C.char,
	timeout float64,
	ignoreChanges **C.char,
	nIgnoreChanges int,
	preview bool,
) (int, *C.char, *C.char) {

	providerObj, err := stalumi.Provider(C.GoString(ctx), tokens.Package(C.GoString(provider)))
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error getting provider: %v", err))
	}

	oldMap, err := stalumi.JSONToPropertyMap([]byte(C.GoString(olds)))
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error marshalling olds: %v", err))
	}

	newMap, err := stalumi.JSONToPropertyMap([]byte(C.GoString(news)))
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error marshalling news: %v", err))
	}

	urnValue := resource.URN(C.GoString(urn))
	idValue := resource.ID(C.GoString(id))

	ignoreChangesSlice := (*[1 << 28]*C.char)(unsafe.Pointer(ignoreChanges))[:nIgnoreChanges:nIgnoreChanges]
	var ignoreChangesStrings []string

	for _, s := range ignoreChangesSlice {
		ignoreChangesStrings = append(ignoreChangesStrings, C.GoString(s))
	}

	props, status, err := (*providerObj).Update(
		urnValue, idValue,
		oldMap, newMap,
		timeout, ignoreChangesStrings, preview,
	)
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error diffing config: %v", err))
	}

	propsEncoded, err := stalumi.PropertyMapToJSON(props)
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error encoding props: %v", err))
	}

	resp := ProviderCreateResponse{
		ID: idValue,
		Properties: json.RawMessage(propsEncoded),
		Status: status,
	}

	resultEncoded, err := json.Marshal(resp)
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error marshalling result: %v", err))
	}

	return 0, C.CString(string(resultEncoded)), nil
}

//export ProviderDelete
func ProviderDelete(
	ctx *C.char,
	provider *C.char,
	urn *C.char,
	id *C.char,
	news *C.char,
	timeout float64,
) (int, int, *C.char) {

	providerObj, err := stalumi.Provider(C.GoString(ctx), tokens.Package(C.GoString(provider)))
	if err != nil {
		return -1, -1, C.CString(fmt.Sprintf("error getting provider: %v", err))
	}

	newMap, err := stalumi.JSONToPropertyMap([]byte(C.GoString(news)))
	if err != nil {
		return -1, -1, C.CString(fmt.Sprintf("error marshalling news: %v", err))
	}

	urnValue := resource.URN(C.GoString(urn))
	idValue := resource.ID(C.GoString(id))

	status, err := (*providerObj).Delete(urnValue, idValue, newMap, timeout)
	if err != nil {
		return -1, -1, C.CString(fmt.Sprintf("error creating resource: %v", err))
	}

	return 0, int(status), nil

}

func main() {}
