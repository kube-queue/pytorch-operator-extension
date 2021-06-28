// Code generated by informer-gen. DO NOT EDIT.

package externalversions

import (
	"fmt"

	v1alpha1 "github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	schema "k8s.io/apimachinery/pkg/runtime/schema"
	cache "k8s.io/client-go/tools/cache"
)

// GenericInformer is type of SharedIndexInformer which will locate and delegate to other
// sharedInformers based on type
type GenericInformer interface {
	Informer() cache.SharedIndexInformer
	Lister() cache.GenericLister
}

type genericInformer struct {
	informer cache.SharedIndexInformer
	resource schema.GroupResource
}

// Informer returns the SharedIndexInformer.
func (f *genericInformer) Informer() cache.SharedIndexInformer {
	return f.informer
}

// Lister returns the GenericLister.
func (f *genericInformer) Lister() cache.GenericLister {
	return cache.NewGenericLister(f.Informer().GetIndexer(), f.resource)
}

// ForResource gives generic access to a shared informer of the matching type
// TODO extend this to unknown resources with a client pool
func (f *sharedInformerFactory) ForResource(resource schema.GroupVersionResource) (GenericInformer, error) {
	switch resource {
	// Group=scheduling.x-k8s.io, Version=v1alpha1
	case v1alpha1.SchemeGroupVersion.WithResource("queues"):
		return &genericInformer{resource: resource.GroupResource(), informer: f.Scheduling().V1alpha1().Queues().Informer()}, nil
	case v1alpha1.SchemeGroupVersion.WithResource("queueunits"):
		return &genericInformer{resource: resource.GroupResource(), informer: f.Scheduling().V1alpha1().QueueUnits().Informer()}, nil

	}

	return nil, fmt.Errorf("no informer found for %v", resource)
}
