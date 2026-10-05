package operator

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/openshift/karpenter-operator/test/pkg/environment"

	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"

	ctrl "sigs.k8s.io/controller-runtime"
)

var env *environment.Environment
var coreClient typedcorev1.CoreV1Interface

func TestOperator(t *testing.T) {
	RegisterFailHandler(Fail)
	BeforeSuite(func() {
		var err error
		env, err = environment.New()
		Expect(err).NotTo(HaveOccurred())

		cfg, err := ctrl.GetConfig()
		Expect(err).NotTo(HaveOccurred())
		coreClient, err = typedcorev1.NewForConfig(cfg)
		Expect(err).NotTo(HaveOccurred())
	})
	RunSpecs(t, "Operator Suite")
}
