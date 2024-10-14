/*
Copyright 2020 The Kubernetes Authors.

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

package cluster

import (
	"context"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	intrec "sigs.k8s.io/controller-runtime/pkg/internal/recorder"
	"sigs.k8s.io/controller-runtime/pkg/runtime/inject"
)

type cluster struct {
	// config is the rest.config used to talk to the apiserver.  Required.
	config *rest.Config

	// scheme is the scheme injected into Controllers, EventHandlers, Sources and Predicates.  Defaults
	// to scheme.scheme.
	scheme *runtime.Scheme

	cache cache.Cache

	// TODO(directxman12): Provide an escape hatch to get individual indexers
	// client is the client injected into Controllers (and EventHandlers, Sources and Predicates).
	client client.Client

	// apiReader is the reader that will make requests to the api server and not the cache.
	apiReader client.Reader

	// fieldIndexes knows how to add field indexes over the Cache used by this controller,
	// which can later be consumed via field selectors from the injected client.
	fieldIndexes client.FieldIndexer

	// recorderProvider is used to generate event recorders that will be injected into Controllers
	// (and EventHandlers, Sources and Predicates).
	recorderProvider *intrec.Provider

	// mapper is used to map resources to kind, and map kind and version.
	mapper meta.RESTMapper

	// Logger is the logger that should be used by this manager.
	// If none is set, it defaults to log.Log global logger.
	logger logr.Logger
}

func (c *cluster) SetFields(i interface{}) error {
	if _, err := inject.ConfigInto(c.config, i); err != nil {
		return err
	}
	if _, err := inject.ClientInto(c.client, i); err != nil {
		return err
	}
	if _, err := inject.APIReaderInto(c.apiReader, i); err != nil {
		return err
	}
	if _, err := inject.SchemeInto(c.scheme, i); err != nil {
		return err
	}
	if _, err := inject.CacheInto(c.cache, i); err != nil {
		return err
	}
	if _, err := inject.MapperInto(c.mapper, i); err != nil {
		return err
	}
	return nil
}

func (c *cluster) GetConfig() *rest.Config {
	return c.config
}

func (c *cluster) GetClient() client.Client {
	return c.client
}

func (c *cluster) GetScheme() *runtime.Scheme {
	return c.scheme
}

func (c *cluster) GetFieldIndexer() client.FieldIndexer {
	return c.fieldIndexes
}

func (c *cluster) GetCache() cache.Cache {
	return c.cache
}

func (c *cluster) GetEventRecorderFor(name string) record.EventRecorder {
	return c.recorderProvider.GetEventRecorderFor(name)
}

func (c *cluster) GetRESTMapper() meta.RESTMapper {
	return c.mapper
}

func (c *cluster) GetAPIReader() client.Reader {
	return c.apiReader
}

func (c *cluster) GetLogger() logr.Logger {
	return c.logger
}

func (c *cluster) Start(ctx context.Context) error {
	defer c.recorderProvider.Stop(ctx)
	return c.cache.Start(ctx)
}
