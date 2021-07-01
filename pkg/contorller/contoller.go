package contorller

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"

	"github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	queueversioned "github.com/kube-queue/api/pkg/client/clientset/versioned"
	queueInformers "github.com/kube-queue/api/pkg/client/informers/externalversions/scheduling/v1alpha1"
	commonv1 "github.com/kube-queue/pytorch-operator-extension/pkg/pytorch-operator/apis/common/job_controller/v1"
	pytorchjobv1 "github.com/kube-queue/pytorch-operator-extension/pkg/pytorch-operator/apis/pytorch/v1"
	pytorchJobversioned "github.com/kube-queue/pytorch-operator-extension/pkg/pytorch-operator/client/clientset/versioned"
	pytorchjobinformers "github.com/kube-queue/pytorch-operator-extension/pkg/pytorch-operator/client/informers/externalversions/pytorch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

const (
	// MaxRetries is the number of times a queue item will be retried before it is dropped out of the queue.
	// With the current rate-limiter in use (5ms*2^(maxRetries-1)) the following numbers represent the times
	// a queue item is going to be requeued:
	//
	// 1-10 retry times: 5ms, 10ms, 20ms, 40ms, 80ms, 160ms, 320ms, 640ms, 1.3s, 2.6s,
	// 11-20 retry times: 5.1s, 10.2s, 20.4s, 41s, 82s, 164s, 328s, 656s(11min), 1312s(21min), 2624s(43min)
	MaxRetries = 20
	// Suspend is a flag annotation for pytorchjob to use the queueunit crd
	Suspend = "scheduling.x-k8s.io/suspend"
)

type PyTorchExtensionController struct {
	queueInformer      queueInformers.QueueUnitInformer
	queueClient        *queueversioned.Clientset
	pytorchJobInformer pytorchjobinformers.PyTorchJobInformer
	pytorchJobClient   *pytorchJobversioned.Clientset
	workqueue          workqueue.RateLimitingInterface
}

func NewPyTorchExtensionController(queueInformer queueInformers.QueueUnitInformer,
	queueClient *queueversioned.Clientset,
	pytorchJobInformer pytorchjobinformers.PyTorchJobInformer,
	pytorchJobClient *pytorchJobversioned.Clientset) *PyTorchExtensionController {
	return &PyTorchExtensionController{
		queueInformer:      queueInformer,
		queueClient:        queueClient,
		pytorchJobInformer: pytorchJobInformer,
		pytorchJobClient:   pytorchJobClient,
		workqueue:          workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "QueueUnit"),
	}
}

func (pc *PyTorchExtensionController) Run(threadiness int, stopCh <-chan struct{}) error {
	defer runtime.HandleCrash()
	defer pc.workqueue.ShutDown()

	klog.Info("Start PyTorchExtensionController Run function")
	if !cache.WaitForCacheSync(stopCh, pc.queueInformer.Informer().HasSynced) {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	for i := 0; i < threadiness; i++ {
		go wait.Until(pc.runWorker, time.Second, stopCh)
	}
	klog.Info("Started workers")
	<-stopCh
	klog.Info("Shutting down workers")

	return nil
}

func (pc *PyTorchExtensionController) runWorker() {
	for pc.processNextWorkItem() {
	}
}

func (pc *PyTorchExtensionController) processNextWorkItem() bool {
	obj, shutdown := pc.workqueue.Get()
	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer pc.workqueue.Done.
	err := func(obj interface{}) error {
		defer pc.workqueue.Done(obj)
		var key string
		var ok bool

		if key, ok = obj.(string); !ok {
			pc.workqueue.Forget(obj)
			runtime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}

		err := pc.syncHandler(key)
		pc.handleErr(err, key)

		return nil
	}(obj)

	if err != nil {
		runtime.HandleError(err)
	}

	return true
}

func (pc *PyTorchExtensionController) syncHandler(key string) error {
	// Convert the namespace/name string into a distinct namespace and name
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return err
	}
	// Get queueunit from cache
	queueUnit, err := pc.queueInformer.Lister().QueueUnits(namespace).Get(name)
	if err != nil {
		if errors.IsNotFound(err) {
			klog.Errorf("QueueUnit %s has been deleted: %s ...", queueUnit.Name)
			// If can'pc get queueunit, return nil, handleErr function will forget key from workqueue
			return nil
		}
		runtime.HandleError(fmt.Errorf("failed to get queueunit by: %s/%s", namespace, name))

		return err
	}
	klog.Infof("Get informer from add/update event,queueUnit:%v/%v", queueUnit.Namespace, queueUnit.Name)

	if queueUnit.Status.Phase == v1alpha1.Dequeued {
		klog.Infof("QueueUnit %v/%v has dequeued", queueUnit.Namespace, queueUnit.Name)
		err = pc.deleteQueueAnotationInPytorchJob(queueUnit)
		if errors.IsNotFound(err) {
			// If can't find pytorchjob for queueunit, return err, handleErr function will requeue key MaxRetries times
			return err
		}
	}

	return nil
}

func (pc *PyTorchExtensionController) deleteQueueUnitWhenJobNotFound(key string) {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return
	}

	err = pc.deleteQueueUnitInstance(namespace, name)
	if err != nil {
		klog.Errorf("Delete queueunit error: %v/%v %v", namespace, name, err.Error())
		return
	}
	klog.Warningf("Delete queueunit %v/%v because can't find related pytorchjob ", namespace, name)
}

func (pc *PyTorchExtensionController) handleErr(err error, key string) {
	if err == nil {
		pc.workqueue.Forget(key)
		return
	}

	if pc.workqueue.NumRequeues(key) < MaxRetries {
		pc.workqueue.AddRateLimited(key)
		klog.Infof("We will requeue %v %d times,because:%v, has retried %d times", key, MaxRetries, err, pc.workqueue.NumRequeues(key)+1)
		return
	}

	runtime.HandleError(err)
	klog.Infof("Dropping queueunit %q out of the workqueue: %v", key, err)
	pc.workqueue.Forget(key)
	// If still can't find job after retry, delete queueunit
	pc.deleteQueueUnitWhenJobNotFound(key)
}

func (pc *PyTorchExtensionController) enqueueQueueUnit(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		runtime.HandleError(err)
		return
	}
	pc.workqueue.AddRateLimited(key)
}

func (pc *PyTorchExtensionController) AddQueueUnit(obj interface{}) {
	qu := obj.(*v1alpha1.QueueUnit)
	klog.Infof("Add queueunit:%v/%v", qu.Namespace, qu.Name)
	pc.enqueueQueueUnit(qu)
}

func (pc *PyTorchExtensionController) UpdateQueueUnit(oldObj, newObj interface{}) {
	oldQu := oldObj.(*v1alpha1.QueueUnit)
	newQu := newObj.(*v1alpha1.QueueUnit)
	if oldQu.ResourceVersion == newQu.ResourceVersion {
		return
	}
	pc.enqueueQueueUnit(newQu)
}

func (pc *PyTorchExtensionController) DeleteQueueUnit(obj interface{}) {
	qu := obj.(*v1alpha1.QueueUnit)
	klog.Infof("QueueUnit deleted:%v/%v", qu.Namespace, qu.Name)
}

func (pc *PyTorchExtensionController) AddPytorchJob(obj interface{}) {
	pytorchJob := obj.(*pytorchjobv1.PyTorchJob)
	klog.Infof("Add pytorchjob:%v/%v", pytorchJob.Namespace, pytorchJob.Name)
}

func (pc *PyTorchExtensionController) UpdatePytorchJob(_, newObj interface{}) {
	newJob := newObj.(*pytorchjobv1.PyTorchJob)
	conditionsLen := len(newJob.Status.Conditions)
	if conditionsLen > 0 {
		lastCondition := newJob.Status.Conditions[conditionsLen-1]
		if lastCondition.Type == commonv1.JobFailed || lastCondition.Type == commonv1.JobSucceeded {
			klog.Infof("job %v/%v finished, current lastCondition.Type: [%v]", newJob.Namespace, newJob.Name, lastCondition.Type)
			pc.deleteQueueUnitAfterJobTerminated(newJob)
		}
	}
}

func (pc *PyTorchExtensionController) DeletePytorchJob(obj interface{}) {
	job := obj.(*pytorchjobv1.PyTorchJob)
	pc.deleteQueueUnitAfterJobTerminated(job)
}

func (pc *PyTorchExtensionController) deleteQueueUnitAfterJobTerminated(job *pytorchjobv1.PyTorchJob) {
	qulist, err := pc.queueClient.SchedulingV1alpha1().QueueUnits(job.Namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		klog.Errorf("DeletePytorchJob error: get qulist failed %v/%v %v", job.Namespace, job.Name, err.Error())
		return
	}

	for _, qu := range qulist.Items {
		if qu.Spec.ConsumerRef.Name == job.Name {
			err = pc.deleteQueueUnitInstance(job.Namespace, qu.Name)
			if err != nil {
				klog.Errorf("Delete queueunit error: delete qu failed %v/%v %v", qu.Namespace, qu.Name, err)
			}
			klog.Infof("Delete queueunit %s because related pytorchjob %v/%v terminated", qu.Name, job.Namespace, job.Name)
		}
	}
}

func (pc *PyTorchExtensionController) deleteQueueUnitInstance(namespace, name string) error {
	err := pc.queueClient.SchedulingV1alpha1().QueueUnits(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
	if err != nil {
		return err
	}

	return nil
}

func (pc *PyTorchExtensionController) deleteQueueAnotationInPytorchJob(qu *v1alpha1.QueueUnit) error {
	namespace := qu.Spec.ConsumerRef.Namespace
	pytorchJobName := qu.Spec.ConsumerRef.Name
	pytorchJob, err := pc.pytorchJobClient.KubeflowV1().PyTorchJobs(qu.Spec.ConsumerRef.Namespace).Get(context.TODO(), pytorchJobName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			klog.Warningf("Can not find related pytorchjob:%v for queueunit:%v in namespace:%v", pytorchJobName, qu.Name, namespace)
			return err
		}
		klog.Errorf("Get pytorchjob failed %v/%v %v", namespace, pytorchJobName, err.Error())
		return err
	}

	var annotation = map[string]string{}
	for k, v := range pytorchJob.Annotations {
		if k != Suspend {
			annotation[k] = v
		}
	}
	pytorchJob.SetAnnotations(annotation)

	// TODO change to patch
	_, err = pc.pytorchJobClient.KubeflowV1().PyTorchJobs(namespace).Update(context.TODO(), pytorchJob, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("UpdateQueueUnit error: update pytorchjob failed %v/%v %v", namespace, pytorchJobName, err.Error())
		return err
	}
	klog.Infof("Update annotations for pytorchjob %v/%v", pytorchJob.Namespace, pytorchJob.Name)

	return nil
}
