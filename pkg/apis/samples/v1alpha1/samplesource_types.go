/*
Copyright 2019 The Knative Authors.

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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	duckv1alpha1 "knative.dev/pkg/apis/duck/v1alpha1"
	"knative.dev/pkg/kmeta"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:openapi-gen=true
type SampleSource struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec holds the desired state of the SampleSource (from the client).
	Spec SampleSourceSpec `json:"spec"`

	// Status communicates the observed state of the SampleSource (from the controller).
	// +optional
	Status SampleSourceStatus `json:"status,omitempty"`
}

// Check that SampleSource is a runtime.Object.
var _ runtime.Object = (*SampleSource)(nil)

// Check that we can create OwnerReferences to a SampleSource.
var _ kmeta.OwnerRefable = (*SampleSource)(nil)

// Check that SampleSource implements the Conditions duck type.
var _ = duck.VerifyType(&SampleSource{}, &duckv1.Conditions{})

const (
	// SampleSourceEventType is the SampleSource CloudEvent type.
	SampleSourceEventType = "dev.knative.sample.source"
)

// SampleSourceSpec holds the desired state of the SampleSource (from the client).
type SampleSourceSpec struct {
	// ServiceAccountName holds the name of the Kubernetes service account
	// as which the underlying K8s resources should be run. If unspecified
	// this will default to the "default" service account for the namespace
	// in which the SampleSource exists.
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty"`

	// ServerURL is the URL of the source HTTP server
	ServerURL string `json:"serverURL"`

	// Sink is a reference to an object that will resolve to a host
	// name to use as the sink.
	Sink *duckv1alpha1.Destination `json:"sink"`
}

const (
	// SampleSourceConditionReady is set when the revision is starting to materialize
	// runtime resources, and becomes true when those resources are ready.
	SampleSourceConditionReady = apis.ConditionReady
)

// SampleSourceStatus communicates the observed state of the SampleSource (from the controller).
type SampleSourceStatus struct {
	duckv1beta1.Status `json:",inline"`

	// SinkURI is the current active sink URI that has been configured
	// for the SampleSource.
	// +optional
	SinkURI string `json:"sinkUri,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SampleSourceList is a list of SampleSource resources
type SampleSourceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []SampleSource `json:"items"`
}
