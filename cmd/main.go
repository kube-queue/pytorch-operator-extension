package main

import (
	"flag"

	"github.com/kube-queue/pytorch-operator-extension/cmd/app"
	"github.com/kube-queue/pytorch-operator-extension/cmd/app/options"
	"k8s.io/klog/v2"
)

func main() {
	s := options.NewServerOption()
	s.AddFlags(flag.CommandLine)
	flag.Parse()

	if err := app.Run(s); err != nil {
		klog.Fatalf("Failed to run: %v", err)
	}
}
