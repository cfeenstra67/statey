package stalumi

import (
	"fmt"
	"path/filepath"

	"github.com/blang/semver"

	"github.com/pulumi/pulumi/pkg/v2/engine"
	"github.com/pulumi/pulumi/sdk/v2/go/common/diag"
	"github.com/pulumi/pulumi/sdk/v2/go/common/resource/plugin"
	"github.com/pulumi/pulumi/sdk/v2/go/common/tokens"
	"github.com/pulumi/pulumi/sdk/v2/go/common/workspace"
)

type Context struct {
	Info engine.Projinfo
	PluginCtx *plugin.Context
	WorkingDirectory string
	Main string
	Sink diag.Sink
	StatusSink diag.Sink
}

func NewContextFromPath(path string, sink, statusSink diag.Sink) (*Context, error) {
	projectPath, err := workspace.DetectProjectPathFrom(path)
	if err != nil {
		return nil, fmt.Errorf("error detecting project: %v", err)
	}

	proj, err := workspace.LoadProject(path)
	if err != nil {
		return nil, fmt.Errorf("error loading project: %v", err)
	}

	root := filepath.Dir(projectPath)

	projInfo := engine.Projinfo{Proj: proj, Root: root}

	pwd, main, ctx, err := engine.ProjectInfoContext(&projInfo, nil, nil, sink, statusSink, false, nil)
	if err != nil {
		return nil, fmt.Errorf("error obtaining context: %v", err)
	}

	newCtx := Context{
		Info: projInfo,
		PluginCtx: ctx,
		WorkingDirectory: pwd,
		Main: main,
		Sink: sink,
		StatusSink: statusSink,
	}

	return &newCtx, nil
}

func (c *Context) Close() {
	c.PluginCtx.Close()
}

func (c *Context) InstallPlugins() error {
	return engine.RunInstallPlugins(
		c.Info.Proj, c.WorkingDirectory, c.Main, nil, c.PluginCtx,
	)
}

func (c *Context) Provider(name tokens.Package, version *semver.Version) (*plugin.Provider, error) {
	provider, err := c.PluginCtx.Host.Provider(name, version)
	return &provider, err
}

func (c *Context) CloseProvider(provider *plugin.Provider) error {
	return c.PluginCtx.Host.CloseProvider(*provider)
}
