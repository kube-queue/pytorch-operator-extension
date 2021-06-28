package app

import (
	v1 "github.com/kube-queue/pytorch-operator-extension/pkg/pytorch-operator/apis/pytorch/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/sample-controller/pkg/signals"

	"github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	queueversioned "github.com/kube-queue/api/pkg/client/clientset/versioned"
	queueinformers "github.com/kube-queue/api/pkg/client/informers/externalversions"
	"github.com/kube-queue/pytorch-operator-extension/cmd/app/options"
	"github.com/kube-queue/pytorch-operator-extension/pkg/contorller"
	pytorchjobversioned "github.com/kube-queue/pytorch-operator-extension/pkg/pytorch-operator/client/clientset/versioned"
	pytorchjobinformers "github.com/kube-queue/pytorch-operator-extension/pkg/pytorch-operator/client/informers/externalversions"
	"k8s.io/klog/v2"
)

const (
	ConsumerRefKind       = "PyTorchJob"
	ConsumerRefAPIVersion = "kubeflow.org/v1"
)

// Run runs the server.
func Run(opt *options.ServerOption) error {
	var restConfig *rest.Config
	var err error
	// Set up signals so we handle the first shutdown signal gracefully.
	stopCh := signals.SetupSignalHandler()

	if restConfig, err = rest.InClusterConfig(); err != nil {
		if restConfig, err = clientcmd.BuildConfigFromFlags("", opt.KubeConfig); err != nil {
			return err
		}
	}

	queueClient, err := queueversioned.NewForConfig(restConfig)
	if err != nil {
		return err
	}

	pytorchJobClient, err := pytorchjobversioned.NewForConfig(restConfig)
	if err != nil {
		return err
	}

	queueInformerFactory := queueinformers.NewSharedInformerFactory(queueClient, 0)
	queueInformer := queueInformerFactory.Scheduling().V1alpha1().QueueUnits().Informer()
	pytorchJobInformerFactory := pytorchjobinformers.NewSharedInformerFactory(pytorchJobClient, 0)
	pytorchJobInformer := pytorchJobInformerFactory.Kubeflow().V1().PyTorchJobs().Informer()

	pytorchExtensionController := contorller.NewPyTorchExtensionController(queueInformerFactory.Scheduling().V1alpha1().QueueUnits(),
		queueClient,
		pytorchJobInformerFactory.Kubeflow().V1().PyTorchJobs(),
		pytorchJobClient,
	)

	queueInformer.AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch qu := obj.(type) {
				case *v1alpha1.QueueUnit:
					if qu.Spec.ConsumerRef.Kind == ConsumerRefKind && qu.Spec.ConsumerRef.APIVersion == ConsumerRefAPIVersion {
						return true
					}
					return false
				default:
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    pytorchExtensionController.AddQueueUnit,
				UpdateFunc: pytorchExtensionController.UpdateQueueUnit,
				DeleteFunc: pytorchExtensionController.DeleteQueueUnit,
			},
		},
	)

	pytorchJobInformer.AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch obj.(type) {
				case *v1.PyTorchJob:
					return true
				default:
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    pytorchExtensionController.AddPytorchJob,
				UpdateFunc: pytorchExtensionController.UpdatePytorchJob,
				DeleteFunc: pytorchExtensionController.DeletePytorchJob,
			},
		},
	)

	// start queueunit informer
	go queueInformerFactory.Start(stopCh)
	// start pytorchjob informer
	go pytorchJobInformerFactory.Start(stopCh)

	err = pytorchExtensionController.Run(2, stopCh)
	if err != nil {
		klog.Fatalf("Error running pytorchExtensionController", err.Error())
		return err
	}

	return nil
}
