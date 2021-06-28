module github.com/kube-queue/pytorch-operator-extension

go 1.15

require (
	github.com/kube-queue/api v0.0.0-20210623033849-bffe1acb5aa9
	k8s.io/api v0.21.2
	k8s.io/apimachinery v0.21.2
	k8s.io/client-go v0.21.2
	k8s.io/code-generator v0.21.2
	k8s.io/klog v1.0.0 // indirect
	k8s.io/klog/v2 v2.9.0
	k8s.io/kube-openapi v0.0.0-20210527164424-3c818078ee3d
	k8s.io/sample-controller v0.21.2
)

replace (
	k8s.io/api => k8s.io/api v0.18.5
	k8s.io/apimachinery => k8s.io/apimachinery v0.18.5
	k8s.io/client-go => k8s.io/client-go v0.18.5
	k8s.io/code-generator => k8s.io/code-generator v0.18.5
	k8s.io/sample-controller => k8s.io/sample-controller v0.18.5
)
