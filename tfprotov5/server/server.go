package tf5server

import (
	"context"
	"sync"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"github.com/hashicorp/go-uuid"
	"github.com/hashicorp/terraform-plugin-go/tfprotov5"
	"github.com/hashicorp/terraform-plugin-go/tfprotov5/internal/fromproto"
	"github.com/hashicorp/terraform-plugin-go/tfprotov5/internal/tfplugin5"
	"github.com/hashicorp/terraform-plugin-go/tfprotov5/internal/toproto"
	tflog "github.com/hashicorp/terraform-plugin-log"
	tfsdklog "github.com/hashicorp/terraform-plugin-log/sdk"
)

const sublogkey = "plugin-go"

// ServeOpt is an interface for defining options that can be passed to the
// Serve function. Each implementation modifies the ServeConfig being
// generated. A slice of ServeOpts then, cumulatively applied, render a full
// ServeConfig.
type ServeOpt interface {
	ApplyServeOpt(*ServeConfig) error
}

// ServeConfig contains the configured options for how a provider should be
// served.
type ServeConfig struct {
	logger       hclog.Logger
	debugCtx     context.Context
	debugCh      chan *plugin.ReattachConfig
	debugCloseCh chan struct{}
}

type serveConfigFunc func(*ServeConfig) error

func (s serveConfigFunc) ApplyServeOpt(in *ServeConfig) error {
	return s(in)
}

// WithDebug returns a ServeOpt that will set the server into debug mode, using
// the passed options to populate the go-plugin ServeTestConfig.
func WithDebug(ctx context.Context, config chan *plugin.ReattachConfig, closeCh chan struct{}) ServeOpt {
	return serveConfigFunc(func(in *ServeConfig) error {
		in.debugCtx = ctx
		in.debugCh = config
		in.debugCloseCh = closeCh
		return nil
	})
}

// WithGoPluginLogger returns a ServeOpt that will set the logger that
// go-plugin should use to log messages.
func WithGoPluginLogger(logger hclog.Logger) ServeOpt {
	return serveConfigFunc(func(in *ServeConfig) error {
		in.logger = logger
		return nil
	})
}

// Serve starts a tfprotov5.ProviderServer serving, ready for Terraform to
// connect to it. The name passed in should be the fully qualified name that
// users will enter in the source field of the required_providers block, like
// "registry.terraform.io/hashicorp/time".
//
// Zero or more options to configure the server may also be passed. The default
// invocation is sufficient, but if the provider wants to run in debug mode or
// modify the logger that go-plugin is using, ServeOpts can be specified to
// support that.
func Serve(name string, serverFactory func() tfprotov5.ProviderServer, opts ...ServeOpt) error {
	var conf ServeConfig
	for _, opt := range opts {
		err := opt.ApplyServeOpt(&conf)
		if err != nil {
			return err
		}
	}
	serveConfig := &plugin.ServeConfig{
		HandshakeConfig: plugin.HandshakeConfig{
			ProtocolVersion:  5,
			MagicCookieKey:   "TF_PLUGIN_MAGIC_COOKIE",
			MagicCookieValue: "d602bf8f470bc67ca7faa0386276bbdd4330efaf76d1a219cb4d6991ca9872b2",
		},
		Plugins: plugin.PluginSet{
			"provider": &GRPCProviderPlugin{
				GRPCProvider: serverFactory,
			},
		},
		GRPCServer: plugin.DefaultGRPCServer,
	}
	if conf.logger != nil {
		serveConfig.Logger = conf.logger
	}
	if conf.debugCh != nil {
		serveConfig.Test = &plugin.ServeTestConfig{
			Context:          conf.debugCtx,
			ReattachConfigCh: conf.debugCh,
			CloseCh:          conf.debugCloseCh,
		}
	}
	plugin.Serve(serveConfig)
	return nil
}

type server struct {
	downstream tfprotov5.ProviderServer
	tfplugin5.UnimplementedProviderServer

	stopMu sync.Mutex
	stopCh chan struct{}
}

func mergeStop(ctx context.Context, cancel context.CancelFunc, stopCh chan struct{}) {
	select {
	case <-ctx.Done():
		return
	case <-stopCh:
		cancel()
	}
}

// stoppableContext returns a context that wraps `ctx` but will be canceled
// when the server's stopCh is closed.
//
// This is used to cancel all in-flight contexts when the Stop method of the
// server is called.
func (s *server) stoppableContext(ctx context.Context) context.Context {
	s.stopMu.Lock()
	defer s.stopMu.Unlock()

	stoppable, cancel := context.WithCancel(ctx)
	go mergeStop(stoppable, cancel, s.stopCh)
	return stoppable
}

// loggingContext returns a terraform-plugin-log equipped context.Context based
// on `ctx`.
func (s *server) loggingContext(ctx context.Context) context.Context {
	// set up root SDK logger
	ctx = tfsdklog.New(ctx, tfsdklog.WithStderrFromInit())

	// set up plugin-go-specific logger
	ctx = tfsdklog.NewSubsystem(ctx, sublogkey, tfsdklog.WithLevelFromEnv("TF_LOG_PROVIDER_SDK_PLUGIN_GO"))

	// set up provider logger
	// TODO: let's get the provider name in here somehow and set a default
	// environment variable, maybe the module name
	ctx = tflog.New(ctx, tflog.WithLogName("provider"), tflog.WithStderrFromInit())

	// inject request ID in logs
	requestID, err := uuid.GenerateUUID()
	if err != nil {
		tfsdklog.Error(ctx, "couldn't generate request ID", "error", err)
	} else {
		ctx = s.allLoggersWithContext(ctx, "request_id", requestID)
	}

	return ctx
}

func (s *server) allLoggersWithContext(ctx context.Context, key string, value interface{}) context.Context {
	ctx = tfsdklog.With(ctx, key, value)
	ctx = tfsdklog.SubsystemWith(ctx, sublogkey, key, value)
	ctx = tflog.With(ctx, key, value)
	return ctx
}

// New converts a tfprotov5.ProviderServer into a server capable of handling
// Terraform protocol requests and issuing responses using the gRPC types.
func New(serve tfprotov5.ProviderServer) tfplugin5.ProviderServer {
	return &server{
		downstream: serve,
		stopCh:     make(chan struct{}),
	}
}

func (s *server) GetSchema(ctx context.Context, req *tfplugin5.GetProviderSchema_Request) (*tfplugin5.GetProviderSchema_Response, error) {
	ctx = s.loggingContext(ctx)
	ctx = s.stoppableContext(ctx)
	tfsdklog.SubsystemTrace(ctx, sublogkey, "got GetSchema RPC")
	r, err := fromproto.GetProviderSchemaRequest(req)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "calling GetProviderSchema")
	resp, err := s.downstream.GetProviderSchema(ctx, r)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "called GetProviderSchema")
	ret, err := toproto.GetProviderSchema_Response(resp)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "served GetSchema RPC")
	return ret, nil
}

func (s *server) PrepareProviderConfig(ctx context.Context, req *tfplugin5.PrepareProviderConfig_Request) (*tfplugin5.PrepareProviderConfig_Response, error) {
	ctx = s.loggingContext(ctx)
	ctx = s.stoppableContext(ctx)
	tfsdklog.SubsystemTrace(ctx, sublogkey, "got PrepareProviderConfig RPC")
	r, err := fromproto.PrepareProviderConfigRequest(req)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "calling PrepareProviderConfig")
	resp, err := s.downstream.PrepareProviderConfig(ctx, r)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "called PrepareProviderConfig")
	ret, err := toproto.PrepareProviderConfig_Response(resp)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "served PrepareProviderConfig")
	return ret, nil
}

func (s *server) Configure(ctx context.Context, req *tfplugin5.Configure_Request) (*tfplugin5.Configure_Response, error) {
	ctx = s.loggingContext(ctx)
	ctx = s.stoppableContext(ctx)
	tfsdklog.SubsystemTrace(ctx, sublogkey, "got Configure RPC")
	r, err := fromproto.ConfigureProviderRequest(req)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "calling ConfigureProvider")
	resp, err := s.downstream.ConfigureProvider(ctx, r)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "called ConfigureProvider")
	ret, err := toproto.Configure_Response(resp)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "served Configure")
	return ret, nil
}

// stop closes the stopCh associated with the server and replaces it with a new
// one.
//
// This causes all in-flight requests for the server to have their contexts
// canceled.
func (s *server) stop(ctx context.Context) {
	tfsdklog.SubsystemTrace(ctx, sublogkey, "acquiring lock to stop all running goroutines")
	s.stopMu.Lock()
	defer s.stopMu.Unlock()

	tfsdklog.SubsystemTrace(ctx, sublogkey, "stopping all running goroutines")
	close(s.stopCh)
	s.stopCh = make(chan struct{})
}

func (s *server) Stop(ctx context.Context, req *tfplugin5.Stop_Request) (*tfplugin5.Stop_Response, error) {
	ctx = s.loggingContext(ctx)
	ctx = s.stoppableContext(ctx)
	tfsdklog.SubsystemTrace(ctx, sublogkey, "got Stop RPC")
	r, err := fromproto.StopProviderRequest(req)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "calling StopProvider")
	resp, err := s.downstream.StopProvider(ctx, r)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "called StopProvider")
	s.stop(ctx)
	ret, err := toproto.Stop_Response(resp)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "served Stop")
	return ret, nil
}

func (s *server) ValidateDataSourceConfig(ctx context.Context, req *tfplugin5.ValidateDataSourceConfig_Request) (*tfplugin5.ValidateDataSourceConfig_Response, error) {
	ctx = s.loggingContext(ctx)
	ctx = s.stoppableContext(ctx)
	ctx = s.allLoggersWithContext(ctx, "data_source", req.TypeName)
	tfsdklog.SubsystemTrace(ctx, sublogkey, "got ValidateDataSourceConfig RPC")
	r, err := fromproto.ValidateDataSourceConfigRequest(req)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "calling ValidateDataSourceConfig")
	resp, err := s.downstream.ValidateDataSourceConfig(ctx, r)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "called ValidateDataSourceConfig")
	ret, err := toproto.ValidateDataSourceConfig_Response(resp)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "served ValidateDataSourceConfig")
	return ret, nil
}

func (s *server) ReadDataSource(ctx context.Context, req *tfplugin5.ReadDataSource_Request) (*tfplugin5.ReadDataSource_Response, error) {
	ctx = s.loggingContext(ctx)
	ctx = s.stoppableContext(ctx)
	ctx = s.allLoggersWithContext(ctx, "data_source", req.TypeName)
	tfsdklog.SubsystemTrace(ctx, sublogkey, "got ReadDataSource RPC")
	r, err := fromproto.ReadDataSourceRequest(req)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "calling ReadDataSource")
	resp, err := s.downstream.ReadDataSource(ctx, r)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "called ReadDataSource")
	ret, err := toproto.ReadDataSource_Response(resp)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "served ReadDataSource")
	return ret, nil
}

func (s *server) ValidateResourceTypeConfig(ctx context.Context, req *tfplugin5.ValidateResourceTypeConfig_Request) (*tfplugin5.ValidateResourceTypeConfig_Response, error) {
	ctx = s.loggingContext(ctx)
	ctx = s.stoppableContext(ctx)
	ctx = s.allLoggersWithContext(ctx, "resource", req.TypeName)
	tfsdklog.SubsystemTrace(ctx, sublogkey, "got ValidateResourceTypeConfig RPC")
	r, err := fromproto.ValidateResourceTypeConfigRequest(req)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "calling ValidateResourceTypeConfig")
	resp, err := s.downstream.ValidateResourceTypeConfig(ctx, r)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "called ValidateResourceTypeConfig")
	ret, err := toproto.ValidateResourceTypeConfig_Response(resp)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "served ValidateResourceTypeConfig")
	return ret, nil
}

func (s *server) UpgradeResourceState(ctx context.Context, req *tfplugin5.UpgradeResourceState_Request) (*tfplugin5.UpgradeResourceState_Response, error) {
	ctx = s.loggingContext(ctx)
	ctx = s.stoppableContext(ctx)
	ctx = s.allLoggersWithContext(ctx, "resource", req.TypeName)
	ctx = s.allLoggersWithContext(ctx, "from_schema_version", req.Version)
	tfsdklog.SubsystemTrace(ctx, sublogkey, "got UpgradeResourceState RPC")
	r, err := fromproto.UpgradeResourceStateRequest(req)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "calling UpgradeResourceState")
	resp, err := s.downstream.UpgradeResourceState(ctx, r)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "called UpgradeResourceState")
	ret, err := toproto.UpgradeResourceState_Response(resp)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "served UpgradeResourceState")
	return ret, nil
}

func (s *server) ReadResource(ctx context.Context, req *tfplugin5.ReadResource_Request) (*tfplugin5.ReadResource_Response, error) {
	ctx = s.loggingContext(ctx)
	ctx = s.stoppableContext(ctx)
	ctx = s.allLoggersWithContext(ctx, "resource", req.TypeName)
	tfsdklog.SubsystemTrace(ctx, sublogkey, "got ReadResource RPC")
	r, err := fromproto.ReadResourceRequest(req)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "calling ReadResource")
	resp, err := s.downstream.ReadResource(ctx, r)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "called ReadResource")
	ret, err := toproto.ReadResource_Response(resp)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "served ReadResource")
	return ret, nil
}

func (s *server) PlanResourceChange(ctx context.Context, req *tfplugin5.PlanResourceChange_Request) (*tfplugin5.PlanResourceChange_Response, error) {
	ctx = s.loggingContext(ctx)
	ctx = s.stoppableContext(ctx)
	ctx = s.allLoggersWithContext(ctx, "resource", req.TypeName)
	tfsdklog.SubsystemTrace(ctx, sublogkey, "got PlanResourceChange RPC")
	r, err := fromproto.PlanResourceChangeRequest(req)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "calling PlanResourceChange")
	resp, err := s.downstream.PlanResourceChange(ctx, r)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "called PlanResourceChange")
	ret, err := toproto.PlanResourceChange_Response(resp)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "served PlanResourceChange")
	return ret, nil
}

func (s *server) ApplyResourceChange(ctx context.Context, req *tfplugin5.ApplyResourceChange_Request) (*tfplugin5.ApplyResourceChange_Response, error) {
	ctx = s.loggingContext(ctx)
	ctx = s.stoppableContext(ctx)
	ctx = s.allLoggersWithContext(ctx, "resource", req.TypeName)
	tfsdklog.SubsystemTrace(ctx, sublogkey, "got ApplyResourceChange RPC")
	r, err := fromproto.ApplyResourceChangeRequest(req)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "calling ApplyResourceChange")
	resp, err := s.downstream.ApplyResourceChange(ctx, r)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "called ApplyResourceChange")
	ret, err := toproto.ApplyResourceChange_Response(resp)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "served ApplyResourceChange")
	return ret, nil
}

func (s *server) ImportResourceState(ctx context.Context, req *tfplugin5.ImportResourceState_Request) (*tfplugin5.ImportResourceState_Response, error) {
	ctx = s.loggingContext(ctx)
	ctx = s.stoppableContext(ctx)
	ctx = s.allLoggersWithContext(ctx, "resource", req.TypeName)
	ctx = s.allLoggersWithContext(ctx, "import_id", req.Id)
	tfsdklog.SubsystemTrace(ctx, sublogkey, "got ImportResourceState RPC")
	r, err := fromproto.ImportResourceStateRequest(req)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "calling ImportResourceState")
	resp, err := s.downstream.ImportResourceState(ctx, r)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "called ImportResourceState")
	ret, err := toproto.ImportResourceState_Response(resp)
	if err != nil {
		return nil, err
	}
	tfsdklog.SubsystemTrace(ctx, sublogkey, "served ImportResourceState")
	return ret, nil
}
