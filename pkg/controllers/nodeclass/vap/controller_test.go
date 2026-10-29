package vap

import (
	"context"
	"testing"

	. "github.com/onsi/gomega"

	testfake "github.com/openshift/karpenter-operator/test/pkg/fake"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

const (
	testPolicyName  = "karpenter.testnodeclass.hypershift.io"
	testBindingName = "karpenter-binding.testnodeclass.hypershift.io"
)

func TestControllerReconcile(t *testing.T) {
	providerPolicySpec := func(matchConditions ...admissionv1.MatchCondition) admissionv1.ValidatingAdmissionPolicySpec {
		return admissionv1.ValidatingAdmissionPolicySpec{
			MatchConstraints: &admissionv1.MatchResources{
				ResourceRules: []admissionv1.NamedRuleWithOperations{{
					RuleWithOperations: admissionv1.RuleWithOperations{
						Operations: []admissionv1.OperationType{admissionv1.OperationAll},
						Rule: admissionv1.Rule{
							APIGroups:   []string{"karpenter.test.example.io"},
							APIVersions: []string{"*"},
							Resources:   []string{"testnodeclasses"},
						},
					},
				}},
			},
			MatchConditions: matchConditions,
			Validations: []admissionv1.Validation{{
				Expression: "has(oldObject.spec) && has(object.spec) && object.spec == oldObject.spec",
				Message:    "TestNodeClass resource can't be changed directly",
			}},
		}
	}
	hccoCondition := admissionv1.MatchCondition{
		Name:       "exclude-hcco-user",
		Expression: "'system:hosted-cluster-config' != request.userInfo.username",
	}
	providerCondition := admissionv1.MatchCondition{
		Name:       "exclude-test-user",
		Expression: "'system:test' != request.userInfo.username",
	}
	wantBindingSpec := admissionv1.ValidatingAdmissionPolicyBindingSpec{
		PolicyName:        testPolicyName,
		ValidationActions: []admissionv1.ValidationAction{admissionv1.Deny},
	}

	tests := map[string]struct {
		objects           []client.Object
		policySpec        admissionv1.ValidatingAdmissionPolicySpec
		expectPolicySpec  admissionv1.ValidatingAdmissionPolicySpec
		expectBindingSpec admissionv1.ValidatingAdmissionPolicyBindingSpec
	}{
		"When the policy and binding are missing, it should create them with the hosted cluster config user exemption": {
			policySpec:        providerPolicySpec(),
			expectPolicySpec:  providerPolicySpec(hccoCondition),
			expectBindingSpec: wantBindingSpec,
		},
		"When the provider sets match conditions, it should keep them and add the hosted cluster config user exemption": {
			policySpec:        providerPolicySpec(providerCondition),
			expectPolicySpec:  providerPolicySpec(providerCondition, hccoCondition),
			expectBindingSpec: wantBindingSpec,
		},
		"When the policy and binding were changed, it should restore them": {
			objects: []client.Object{
				&admissionv1.ValidatingAdmissionPolicy{
					ObjectMeta: metav1.ObjectMeta{Name: testPolicyName},
					Spec: admissionv1.ValidatingAdmissionPolicySpec{
						Validations: []admissionv1.Validation{{Expression: "true"}},
					},
				},
				&admissionv1.ValidatingAdmissionPolicyBinding{
					ObjectMeta: metav1.ObjectMeta{Name: testBindingName},
					Spec: admissionv1.ValidatingAdmissionPolicyBindingSpec{
						PolicyName:        testPolicyName,
						ValidationActions: []admissionv1.ValidationAction{admissionv1.Audit},
					},
				},
			},
			policySpec:        providerPolicySpec(),
			expectPolicySpec:  providerPolicySpec(hccoCondition),
			expectBindingSpec: wantBindingSpec,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			g := NewWithT(t)
			hostedClient := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(tc.objects...).Build()
			config := &ControllerConfig{
				HostedCluster: &testfake.Cluster{Cl: hostedClient, Ca: &testfake.Cache{}},
				PolicyName:    testPolicyName,
				PolicySpec:    tc.policySpec,
				BindingName:   testBindingName,
				BindingSpec:   wantBindingSpec,
			}
			controller := NewController(testManager(), config)

			_, err := controller.Reconcile(t.Context(), ctrl.Request{})
			g.Expect(err).NotTo(HaveOccurred())

			policy := &admissionv1.ValidatingAdmissionPolicy{}
			g.Expect(hostedClient.Get(t.Context(), client.ObjectKey{Name: testPolicyName}, policy)).To(Succeed())
			g.Expect(policy.Spec).To(Equal(tc.expectPolicySpec))
			g.Expect(config.PolicySpec).To(Equal(tc.policySpec), "the provider policy spec must not be mutated")

			binding := &admissionv1.ValidatingAdmissionPolicyBinding{}
			g.Expect(hostedClient.Get(t.Context(), client.ObjectKey{Name: testBindingName}, binding)).To(Succeed())
			g.Expect(binding.Spec).To(Equal(tc.expectBindingSpec))
		})
	}
}

func TestControllerReconcileGate(t *testing.T) {
	g := NewWithT(t)
	hostedClient := fake.NewClientBuilder().WithScheme(testScheme()).Build()
	managementClient := fake.NewClientBuilder().WithScheme(testScheme()).Build()
	gateCalled := false
	controller := NewController(&testfake.Manager{Cl: managementClient}, &ControllerConfig{
		HostedCluster: &testfake.Cluster{Cl: hostedClient, Ca: &testfake.Cache{}},
		PolicyName:    testPolicyName,
		BindingName:   testBindingName,
		ShouldReconcile: func(_ context.Context, gotReader client.Reader) (bool, error) {
			gateCalled = true
			g.Expect(gotReader).To(Equal(managementClient))
			return false, nil
		},
	})
	_, err := controller.Reconcile(t.Context(), ctrl.Request{})
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(gateCalled).To(BeTrue())
	g.Expect(apierrors.IsNotFound(hostedClient.Get(t.Context(), client.ObjectKey{Name: testPolicyName}, &admissionv1.ValidatingAdmissionPolicy{}))).To(BeTrue())
	g.Expect(apierrors.IsNotFound(hostedClient.Get(t.Context(), client.ObjectKey{Name: testBindingName}, &admissionv1.ValidatingAdmissionPolicyBinding{}))).To(BeTrue())
}

func TestManagedObjectPredicate(t *testing.T) {
	controller := NewController(testManager(), &ControllerConfig{
		PolicyName:  testPolicyName,
		BindingName: testBindingName,
	})
	p := controller.managedObjectPredicate()
	policy := func(name string, generation int64) *admissionv1.ValidatingAdmissionPolicy {
		return &admissionv1.ValidatingAdmissionPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: name, Generation: generation},
		}
	}
	binding := func(name string, generation int64) *admissionv1.ValidatingAdmissionPolicyBinding {
		return &admissionv1.ValidatingAdmissionPolicyBinding{
			ObjectMeta: metav1.ObjectMeta{Name: name, Generation: generation},
		}
	}

	tests := map[string]struct {
		eventType string
		oldObject client.Object
		newObject client.Object
		want      bool
	}{
		"When the managed policy is created, it should reconcile": {
			eventType: "create",
			newObject: policy(testPolicyName, 1),
			want:      true,
		},
		"When an unrelated policy is created, it should not reconcile": {
			eventType: "create",
			newObject: policy("unrelated", 1),
			want:      false,
		},
		"When a policy uses the managed binding name, it should not reconcile": {
			eventType: "create",
			newObject: policy(testBindingName, 1),
			want:      false,
		},
		"When another object type uses the managed policy name, it should not reconcile": {
			eventType: "create",
			newObject: &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: testPolicyName}},
			want:      false,
		},
		"When the managed policy is updated without a generation change, it should not reconcile": {
			eventType: "update",
			oldObject: policy(testPolicyName, 1),
			newObject: policy(testPolicyName, 1),
			want:      false,
		},
		"When the managed policy generation changes, it should reconcile": {
			eventType: "update",
			oldObject: policy(testPolicyName, 1),
			newObject: policy(testPolicyName, 2),
			want:      true,
		},
		"When the managed binding generation changes, it should reconcile": {
			eventType: "update",
			oldObject: binding(testBindingName, 1),
			newObject: binding(testBindingName, 2),
			want:      true,
		},
		"When an unrelated binding generation changes, it should not reconcile": {
			eventType: "update",
			oldObject: binding("unrelated", 1),
			newObject: binding("unrelated", 2),
			want:      false,
		},
		"When the managed binding is deleted, it should reconcile": {
			eventType: "delete",
			oldObject: binding(testBindingName, 1),
			want:      true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			g := NewWithT(t)
			var got bool
			switch tc.eventType {
			case "create":
				got = p.Create(event.CreateEvent{Object: tc.newObject})
			case "update":
				got = p.Update(event.UpdateEvent{ObjectOld: tc.oldObject, ObjectNew: tc.newObject})
			case "delete":
				got = p.Delete(event.DeleteEvent{Object: tc.oldObject})
			default:
				t.Fatalf("unsupported event type %q", tc.eventType)
			}
			g.Expect(got).To(Equal(tc.want))
		})
	}
}

func testScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	return scheme
}

func testManager() *testfake.Manager {
	return &testfake.Manager{Cl: fake.NewClientBuilder().WithScheme(testScheme()).Build()}
}
