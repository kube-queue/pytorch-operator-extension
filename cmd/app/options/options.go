package options

import "flag"

type ServerOption struct {
	KubeConfig string
}

func NewServerOption() *ServerOption {
	s := ServerOption{}
	return &s
}

func (s *ServerOption) AddFlags(fs *flag.FlagSet) {
	fs.StringVar(&s.KubeConfig, "kubeconfig", "", "The path of kubeconfig file")
}
