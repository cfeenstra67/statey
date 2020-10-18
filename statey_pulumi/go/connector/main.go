package main

import "C"

import (
	"fmt"
	"os"

	"stalumi"
	"github.com/pulumi/pulumi/sdk/v2/go/common/util/cmdutil"
	"github.com/pulumi/pulumi/sdk/v2/go/common/tokens"
)

//export Setup
func Setup() (int, *C.char) {
	cwd, err := os.Getwd()
	if err != nil {
		return -1, C.CString(fmt.Sprintf("error getting cwd: %v", err))
	}

	sink := cmdutil.Diag()

	if err := stalumi.SetupContext(cwd, sink, sink); err != nil {
		return -1, C.CString(fmt.Sprintf("error setting up context: %v", err))
	}

	ctx, err := stalumi.GetContext()
	if err != nil {
		return -1, C.CString(fmt.Sprintf("error getting context: %v", err))
	}

	if err := ctx.InstallPlugins(); err != nil {
		return -1, C.CString(fmt.Sprintf("error installing plugins: %v", err))
	}

	return 0, nil
}


//export Teardown
func Teardown() (int, *C.char) {
	if err := stalumi.CloseContext(); err != nil {
		return -1, C.CString(fmt.Sprintf("error closing context: %v", err))
	}
	return 0, nil
}

//export GetProviderSchema
func GetProviderSchema(name *C.char, version int) (int, *C.char, *C.char) {
	provider, err := stalumi.Provider(tokens.Package(C.GoString(name)))
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error getting provider: %v", err))
	}

	response, err := (*provider).GetSchema(version)
	if err != nil {
		return -1, nil, C.CString(fmt.Sprintf("error getting schema: %v", err))
	}

	return 0, C.CString(string(response)), nil
}

func main() {}
