package stalumi

import (
	"fmt"

	"github.com/pulumi/pulumi/sdk/v2/go/common/diag"
	"github.com/pulumi/pulumi/sdk/v2/go/common/resource"
	"github.com/pulumi/pulumi/sdk/v2/go/common/resource/plugin"
	"github.com/pulumi/pulumi/sdk/v2/go/common/tokens"
)

var (
	ctx *Context = nil
	providers map[tokens.Package]*plugin.Provider = make(map[tokens.Package]*plugin.Provider)
)

func SetupContext(path string, sink, statusSink diag.Sink) error {
	if ctx == nil {
		if err := ForceSetupContext(path, sink, statusSink); err != nil {
			return fmt.Errorf("context creation failed: %v", err)
		}
	}
	return nil
}

func ForceSetupContext(path string, sink, statusSink diag.Sink) error {
	var err error
	ctx, err = NewContextFromPath(path, sink, statusSink)
	// panic(123)
	return err
}

func GetContext() (*Context, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context has not been initialized.")
	}
	return ctx, nil
}

func GetConfig(name tokens.Package) *resource.PropertyMap {
	switch name {
	case "aws":
		return &resource.PropertyMap{"region": resource.NewStringProperty("us-east-2")}
	}
	return new(resource.PropertyMap)
}

func Provider(name tokens.Package) (*plugin.Provider, error) {
	value, ok := providers[name]
	if !ok {
		var err error
		ctx, err = GetContext()
		if err != nil {
			return nil, err
		}
		value, err := ctx.Provider(name, nil)
		if err != nil {
			return nil, err
		}
		providers[name] = value
		config := GetConfig(name)
		if err := (*value).Configure(*config); err != nil {
			return nil, err
		}
	}
	return value, nil
}

func CloseProviders() error {
	ctx, err := GetContext()
	if err != nil {
		return err
	}
	for _, provider := range providers {
		ctx.CloseProvider(provider)
	}
	return nil
}

func CloseContext() error {
	if _, err := GetContext(); err != nil {
		return err
	}
	if err := CloseProviders(); err != nil {
		return err
	}
	ctx.Close()
	ctx = nil
	return nil
}
