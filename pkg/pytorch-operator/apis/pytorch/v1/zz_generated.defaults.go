// +build !ignore_autogenerated

// Code generated by defaulter-gen. DO NOT EDIT.

package v1

import (
	runtime "k8s.io/apimachinery/pkg/runtime"
)

// RegisterDefaults adds defaulters functions to the given scheme.
// Public to allow building arbitrary schemes.
// All generated defaulters are covering - they call all nested defaulters.
func RegisterDefaults(scheme *runtime.Scheme) error {
	scheme.AddTypeDefaultingFunc(&PyTorchJob{}, func(obj interface{}) { SetObjectDefaults_PyTorchJob(obj.(*PyTorchJob)) })
	scheme.AddTypeDefaultingFunc(&PyTorchJobList{}, func(obj interface{}) { SetObjectDefaults_PyTorchJobList(obj.(*PyTorchJobList)) })
	return nil
}

func SetObjectDefaults_PyTorchJob(in *PyTorchJob) {
	SetDefaults_PyTorchJob(in)
}

func SetObjectDefaults_PyTorchJobList(in *PyTorchJobList) {
	for i := range in.Items {
		a := &in.Items[i]
		SetObjectDefaults_PyTorchJob(a)
	}
}
