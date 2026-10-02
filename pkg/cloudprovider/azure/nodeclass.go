package azure

import "github.com/openshift/karpenter-operator/pkg/cloudprovider/common"

func (p *Provider) DefaultNodeClassProvider() common.DefaultNodeClassProvider {
	// TODO(AUTOSCALE-969): return Azure default NodeClass provider once defaults are defined.
	return nil
}
