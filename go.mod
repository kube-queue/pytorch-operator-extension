module github.com/kube-queue/pytorch-operator-extension

go 1.15

require (
	github.com/google/go-cmp v0.4.0 // indirect
	github.com/kr/pretty v0.2.0 // indirect
	github.com/stretchr/testify v1.5.1 // indirect
	golang.org/x/net v0.0.0-20200324143707-d3edc9973b7e // indirect
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
	k8s.io/api v0.21.2
	k8s.io/apimachinery v0.21.2
)

replace (
	k8s.io/api => k8s.io/api v0.18.5
	k8s.io/apimachinery => k8s.io/apimachinery v0.18.5
)
