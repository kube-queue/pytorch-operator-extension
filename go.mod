module github.com/kube-queue/pytorch-operator-extension

go 1.15

require (
	k8s.io/api v0.21.2
	k8s.io/apimachinery v0.21.2
	k8s.io/code-generator v0.21.2
	k8s.io/kube-openapi v0.0.0-20210305001622-591a79e4bda7
)

replace (
	k8s.io/api => k8s.io/api v0.18.5
	k8s.io/apimachinery => k8s.io/apimachinery v0.18.5
	k8s.io/code-generator => k8s.io/code-generator v0.18.5
)
