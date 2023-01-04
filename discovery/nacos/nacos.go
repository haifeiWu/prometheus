package nacos

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/discovery/targetgroup"
)

const (
	// metaLabelPrefix is the meta prefix used for all meta labels.
	// in this discovery.
	metaLabelPrefix = model.MetaLabelPrefix + "nacos_"
)

var (
	DefaultSDConfig = SDConfig{
		Server:          "localhost:8500",
		RefreshInterval: model.Duration(30 * time.Second),
	}
)

func init() {
	discovery.RegisterConfig(&SDConfig{})
}

// SDConfig is the configuration for applications running on Nacos.
type SDConfig struct {
	Server          string         `yaml:"server,omitempty"`
	Namespace       string         `yaml:"namespace,omitempty"`
	Services        []string       `yaml:"services,omitempty"`
	GroupName       string         `yaml:"group_name,omitempty"`
	Clusters        string         `yaml:"clusters,omitempty"`
	RefreshInterval model.Duration `yaml:"refresh_interval,omitempty"`
}

// Name returns the name of the Config.
func (*SDConfig) Name() string { return "nacos" }

// NewDiscoverer returns a Discoverer for the Config.
func (c *SDConfig) NewDiscoverer(opts discovery.DiscovererOptions) (discovery.Discoverer, error) {
	return NewDiscovery(c, opts.Logger)
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *SDConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultSDConfig
	type plain SDConfig
	err := unmarshal((*plain)(c))
	if err != nil {
		return err
	}

	if strings.TrimSpace(c.Server) == "" {
		return errors.New("nacos SD configuration requires a server address")
	}
	return nil
}

// Discovery provides service discovery based on a Eureka instance.
type Discovery struct {
	client          *naming_client.INamingClient
	clientNamespace string
	watchedServices []string // Set of services which will be discovered.
	refreshInterval time.Duration
	logger          log.Logger
}

// NewDiscovery creates a new Eureka discovery for the given role.
func NewDiscovery(conf *SDConfig, logger log.Logger) (*Discovery, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	clientConf := *constant.NewClientConfig(
		constant.WithNamespaceId(conf.Namespace),
		constant.WithTimeoutMs(5000),
		constant.WithNotLoadCacheAtStart(true),
	)

	serverSplitStr := strings.Split(conf.Server, ":")
	if len(serverSplitStr) < 2 {
		return nil, errors.New("nacos SD configuration requires a server address")
	}

	port, err := strconv.ParseInt(serverSplitStr[1], 10, 64)
	if err != nil {
		return nil, err
	}

	// init discovery client
	nameClient, err := clients.NewNamingClient(
		vo.NacosClientParam{
			ClientConfig: &clientConf,
			ServerConfigs: []constant.ServerConfig{
				*constant.NewServerConfig(serverSplitStr[0], uint64(port)),
			},
		},
	)

	if err != nil {
		return nil, err
	}
	cd := &Discovery{
		client:          &nameClient,
		watchedServices: conf.Services,
		refreshInterval: time.Duration(conf.RefreshInterval),
		clientNamespace: conf.Namespace,
		logger:          logger,
	}
	return cd, nil
}

func (d *Discovery) Run(ctx context.Context, ch chan<- []*targetgroup.Group) {
	// 订阅服务发现服务  进行实列节点的更新
	if len(d.watchedServices) == 0 {
		// We need to watch the catalog.
		ticker := time.NewTicker(d.refreshInterval)

		// Watched services and their cancellation functions.
		//services := make(map[string]func())
		//var lastIndex uint64

		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			default:
				//d.watchServices(ctx, ch, &lastIndex, services)

				<-ticker.C
			}
		}
	} else {
		// We only have fully defined services.
		for _, name := range d.watchedServices {
			//d.watchService(ctx, ch, name)
			d.logger.Log(name)
		}
		<-ctx.Done()
	}
}
