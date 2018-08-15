/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cmd

import (
	"fmt"
	"sync"
	"time"

	"github.com/spf13/pflag"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/kubernetes-incubator/custom-metrics-apiserver/pkg/cmd/server"
	"github.com/kubernetes-incubator/custom-metrics-apiserver/pkg/dynamicmapper"
	"github.com/kubernetes-incubator/custom-metrics-apiserver/pkg/provider"
)

type AdapterBase struct {
	*server.CustomMetricsAdapterServerOptions

	// Name is the name of the API server.  It defaults to custom-metrics-adapter
	Name string

	// RemoteKubeConfigFile specifies the kubeconfig to use to construct
	// the dynamic client and RESTMapper.  It's set from a flag.
	RemoteKubeConfigFile string
	// DiscoveryInterval specifies the interval at which to recheck discovery
	// information for the discovery RESTMapper.  It's set from a flag.
	DiscoveryInterval time.Duration

	// FlagSet is the flagset to add flags to.
	// It defaults to the normal CommandLine flags
	// if not explicitly set
	FlagSet *pflag.FlagSet

	// flagOnce controls initialization of the flags.
	flagOnce sync.Once

	clientConfig    *rest.Config
	discoveryClient discovery.DiscoveryInterface
	restMapper      apimeta.RESTMapper
	dynamicClient   dynamic.Interface

	cmProvider provider.CustomMetricsProvider
	emProvider provider.ExternalMetricsProvider
}

// InstallFlags installs the minimum required set of flags into the flagset.
// It's safe to call multiple times.
func (b *AdapterBase) InstallFlags() {
	b.initFlagSet()
	b.flagOnce.Do(func() {
		if b.CustomMetricsAdapterServerOptions == nil {
			b.CustomMetricsAdapterServerOptions = server.NewCustomMetricsAdapterServerOptions()
		}
		
		b.SecureServing.AddFlags(b.FlagSet)
		b.Authentication.AddFlags(b.FlagSet)
		b.Authorization.AddFlags(b.FlagSet)
		b.Features.AddFlags(b.FlagSet)

		b.FlagSet.StringVar(&b.RemoteKubeConfigFile, "lister-kubeconfig", b.RemoteKubeConfigFile,
			"kubeconfig file pointing at the 'core' kubernetes server with enough rights to list "+
				"any described objects")
		b.FlagSet.DurationVar(&b.DiscoveryInterval, "discovery-interval", b.DiscoveryInterval,
			"interval at which to refresh API discovery information")
	})
}

func (b *AdapterBase) initFlagSet() {
	if b.FlagSet == nil {
		// default to the normal commandline flags
		b.FlagSet = pflag.CommandLine
	}
}

// Flags returns the flagset used by this adapter.
// It will initialize the flagset with the minimum required set
// of flags as well.
func (b *AdapterBase) Flags() *pflag.FlagSet {
	b.initFlagSet()
	b.InstallFlags()

	return b.FlagSet
}

func (b *AdapterBase) ClientConfig() (*rest.Config, error) {
	if b.clientConfig == nil {
		var clientConfig *rest.Config
		var err error
		if len(b.RemoteKubeConfigFile) > 0 {
			loadingRules := &clientcmd.ClientConfigLoadingRules{ExplicitPath: b.RemoteKubeConfigFile}
			loader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{})

			clientConfig, err = loader.ClientConfig()
		} else {
			clientConfig, err = rest.InClusterConfig()
		}
		if err != nil {
			return nil, fmt.Errorf("unable to construct lister client config to initialize provider: %v", err)
		}
		b.clientConfig = clientConfig
	}
	return b.clientConfig, nil
}

func (b *AdapterBase) DiscoveryClient() (discovery.DiscoveryInterface, error) {
	if b.discoveryClient == nil {
		clientConfig, err := b.ClientConfig()
		if err != nil {
			return nil, err
		}
		discoveryClient, err := discovery.NewDiscoveryClientForConfig(clientConfig)
		if err != nil {
			return nil, fmt.Errorf("unable to construct discovery client for dynamic client: %v", err)
		}
		b.discoveryClient = discoveryClient
	}
	return b.discoveryClient, nil
}

func (b *AdapterBase) RESTMapper() (apimeta.RESTMapper, error) {
	if b.restMapper == nil {
		discoveryClient, err := b.DiscoveryClient()
		if err != nil {
			return nil, err
		}
		// NB: since we never actually look at the contents of
		// the objects we fetch (beyond ObjectMeta), unstructured should be fine
		dynamicMapper, err := dynamicmapper.NewRESTMapper(discoveryClient, b.DiscoveryInterval)
		if err != nil {
			return nil, fmt.Errorf("unable to construct dynamic discovery mapper: %v", err)
		}

		b.restMapper = dynamicMapper
	}
	return b.restMapper, nil
}

func (b *AdapterBase) DynamicClient() (dynamic.Interface, error) {
	if b.dynamicClient == nil {
		clientConfig, err := b.ClientConfig()
		if err != nil {
			return nil, err
		}
		dynClient, err := dynamic.NewForConfig(clientConfig)
		if err != nil {
			return nil, fmt.Errorf("unable to construct lister client to initialize provider: %v", err)
		}
		b.dynamicClient = dynClient
	}
	return b.dynamicClient, nil
}

func (b *AdapterBase) WithCustomMetrics(p provider.CustomMetricsProvider) {
	b.cmProvider = p
}

func (b *AdapterBase) WithExternalMetrics(p provider.ExternalMetricsProvider) {
	b.emProvider = p
}

func (b *AdapterBase) Run(stopCh <-chan struct{}) error {
	b.InstallFlags()  // just to be sure

	if b.Name == "" {
		b.Name = "custom-metrics-adapter"
	}

	config, err := b.Config()
	if err != nil {
		return err
	}

	server, err := config.Complete().New(b.Name, b.cmProvider, b.emProvider)
	if err != nil {
		return err
	}
	return server.GenericAPIServer.PrepareRun().Run(stopCh)
}
