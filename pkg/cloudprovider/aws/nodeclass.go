package aws

import (
	"fmt"

	openshiftkarpenterv1 "github.com/openshift/karpenter-operator/api/karpenter/v1"
	"github.com/openshift/karpenter-operator/pkg/cloudprovider/common"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

var _ common.DefaultNodeClassProvider = defaultEC2NodeClassProvider{}

type defaultEC2NodeClassProvider struct{}

// DefaultNodeClass returns the default AWS NodeClass and mutation function for its complete desired state.
func (defaultEC2NodeClassProvider) DefaultNodeClass(infraID string) (client.Object, controllerutil.MutateFn, error) {
	nodeClass := &openshiftkarpenterv1.OpenshiftEC2NodeClass{
		TypeMeta: metav1.TypeMeta{
			APIVersion: openshiftkarpenterv1.SchemeGroupVersion.String(),
			Kind:       "OpenshiftEC2NodeClass",
		},
		ObjectMeta: metav1.ObjectMeta{Name: "default"},
	}
	mutate := func() error {
		nodeClass.Labels = map[string]string{
			"app.kubernetes.io/managed-by": "karpenter-operator",
		}
		nodeClass.Spec = openshiftkarpenterv1.OpenshiftEC2NodeClassSpec{
			SubnetSelectorTerms: []openshiftkarpenterv1.SubnetSelectorTerm{
				{
					Tags: map[string]string{
						"kubernetes.io/role/internal-elb":                "1",
						fmt.Sprintf("kubernetes.io/cluster/%s", infraID): "*",
					},
				},
			},
			SecurityGroupSelectorTerms: []openshiftkarpenterv1.SecurityGroupSelectorTerm{
				{
					Tags: map[string]string{
						"karpenter.sh/discovery": infraID,
					},
				},
			},
		}
		return nil
	}
	return nodeClass, mutate, nil
}

func (defaultEC2NodeClassProvider) WatchObject() client.Object {
	return &openshiftkarpenterv1.OpenshiftEC2NodeClass{}
}
