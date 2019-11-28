/*
Copyright 2019 The Knative Authors
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

// Package adapter implements a sample receive adapter that generates events
// at a regular interval.
package adapter

import (
	"context"
	"net/url"
	"strconv"
	"time"

	ce "github.com/cloudevents/sdk-go/pkg/cloudevents"
	"github.com/cloudevents/sdk-go/pkg/cloudevents/client"
	"github.com/cloudevents/sdk-go/pkg/cloudevents/types"
	"go.uber.org/zap"
	"knative.dev/eventing/pkg/adapter"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/source"
)

type envConfig struct {
	// Include the standard adapter.EnvConfig used by all adapters.
	adapter.EnvConfig

	// Interval between events, for example "5s", "100ms"
	Interval time.Duration `envconfig:"INTERVAL" required:"true"`
}

func NewEnv() adapter.EnvConfigAccessor { return &envConfig{} }

// Adapter generates events at a regular interval.
type Adapter struct {
	interval time.Duration
	nextID   int
	sink     client.Client
}

var sourceURI = types.URIRef{URL: url.URL{Scheme: "http", Host: "heartbeat.example.com", Path: "/heartbeat-source"}}

func strptr(s string) *string { return &s }

func (a *Adapter) newEvent() ce.Event {
	e := ce.Event{
		Context: ce.EventContextV1{
			ID:              strconv.Itoa(a.nextID),
			Type:            "com.example.heartbeat",
			Source:          sourceURI,
			Time:            &types.Timestamp{Time: time.Now()},
			DataContentType: strptr("text/json"),
		}.AsV1(),
		Data: map[string]string{"heartbeat": a.interval.String()},
	}
	a.nextID++
	return e
}

// Start runs the adapter.
// Returns if stopCh is closed or Send() returns an error.
func (a *Adapter) Start(stopCh <-chan struct{}) error {
	for {
		select {
		case <-time.After(a.interval):
			_, _, err := a.sink.Send(context.Background(), a.newEvent())
			if err != nil {
				return err
			}
		case <-stopCh:
			return nil
		}
	}
}

func NewAdapter(ctx context.Context, aEnv adapter.EnvConfigAccessor, sink client.Client, reporter source.StatsReporter) adapter.Adapter {
	env := aEnv.(*envConfig) // Will always be our own envConfig type
	logging.FromContext(ctx).Info("Heartbeat example", zap.Duration("interval", env.Interval))
	return &Adapter{interval: env.Interval, sink: sink}
}
