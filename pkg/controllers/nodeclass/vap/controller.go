package vap

import (
	"context"
	"fmt"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/client-go/util/workqueue"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// excludeHCCOUserCondition exempts the operator, which writes to the hosted cluster as the
// hosted cluster config user, from the policy.
var excludeHCCOUserCondition = admissionv1.MatchCondition{
	Name:       "exclude-hcco-user",
	Expression: "'system:hosted-cluster-config' != request.userInfo.username",
}

// ControllerConfig configures a provider's NodeClass admission policy.
type ControllerConfig struct {
	HostedCluster cluster.Cluster
	PolicyName    string
	// PolicySpec is the provider-specific policy. The controller adds the condition that
	// exempts the hosted cluster config user.
	PolicySpec  admissionv1.ValidatingAdmissionPolicySpec
	BindingName string
	BindingSpec admissionv1.ValidatingAdmissionPolicyBindingSpec

	// ShouldReconcile optionally gates reconciliation using the management-cluster client.
	ShouldReconcile func(context.Context, client.Reader) (bool, error)
	// ManagementWatchObject optionally triggers reconciliation from the management-cluster cache.
	ManagementWatchObject client.Object
	// ManagementWatchPredicate optionally filters events for ManagementWatchObject.
	ManagementWatchPredicate predicate.Predicate
}

// Controller keeps a provider's ValidatingAdmissionPolicy and its binding in the hosted cluster.
type Controller struct {
	config       *ControllerConfig
	hostedClient client.Client
	hostedCache  cache.Cache

	managementClient client.Client
}

func NewController(mgr ctrl.Manager, cfg *ControllerConfig) *Controller {
	controller := &Controller{
		config:           cfg,
		managementClient: mgr.GetClient(),
	}
	if cfg.HostedCluster != nil {
		controller.hostedClient = cfg.HostedCluster.GetClient()
		controller.hostedCache = cfg.HostedCluster.GetCache()
	}
	return controller
}

func (c *Controller) Name() string {
	return "nodeclass-vap"
}

func (c *Controller) SetupWithManager(mgr ctrl.Manager) error {
	if c.config.HostedCluster == nil {
		return fmt.Errorf("hosted cluster is required for nodeclass admission policy controller")
	}
	// All events map to a single work item, because the controller always applies both objects.
	enqueue := handler.EnqueueRequestsFromMapFunc(func(context.Context, client.Object) []reconcile.Request {
		return []reconcile.Request{{}}
	})
	isManaged := c.managedObjectPredicate()

	// Trigger an initial reconcile, because the watches only fire once the objects exist.
	initialSync := source.Func(func(_ context.Context, q workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
		q.Add(reconcile.Request{})
		return nil
	})

	controllerBuilder := ctrl.NewControllerManagedBy(mgr).
		Named(c.Name()).
		WatchesRawSource(source.Kind[client.Object](c.hostedCache, &admissionv1.ValidatingAdmissionPolicy{}, enqueue, isManaged)).
		WatchesRawSource(source.Kind[client.Object](c.hostedCache, &admissionv1.ValidatingAdmissionPolicyBinding{}, enqueue, isManaged)).
		WatchesRawSource(initialSync)
	if c.config.ManagementWatchObject != nil {
		if c.config.ManagementWatchPredicate != nil {
			controllerBuilder = controllerBuilder.Watches(
				c.config.ManagementWatchObject,
				enqueue,
				builder.WithPredicates(c.config.ManagementWatchPredicate),
			)
		} else {
			controllerBuilder = controllerBuilder.Watches(c.config.ManagementWatchObject, enqueue)
		}
	}
	return controllerBuilder.Complete(c)
}

func (c *Controller) managedObjectPredicate() predicate.Predicate {
	isManaged := predicate.NewPredicateFuncs(func(object client.Object) bool {
		switch object.(type) {
		case *admissionv1.ValidatingAdmissionPolicy:
			return object.GetName() == c.config.PolicyName
		case *admissionv1.ValidatingAdmissionPolicyBinding:
			return object.GetName() == c.config.BindingName
		default:
			return false
		}
	})
	return predicate.And(isManaged, predicate.GenerationChangedPredicate{})
}

func (c *Controller) Reconcile(ctx context.Context, _ ctrl.Request) (ctrl.Result, error) {
	if c.config.ShouldReconcile != nil {
		shouldReconcile, err := c.config.ShouldReconcile(ctx, c.managementClient)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("checking the nodeclass admission policy reconciliation gate: %w", err)
		}
		if !shouldReconcile {
			return ctrl.Result{}, nil
		}
	}

	policy := &admissionv1.ValidatingAdmissionPolicy{}
	policy.Name = c.config.PolicyName
	op, err := controllerutil.CreateOrUpdate(ctx, c.hostedClient, policy, func() error {
		policy.Spec = c.policySpec()
		return nil
	})
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling validating admission policy %s: %w", c.config.PolicyName, err)
	}
	// Updates are not logged: the API server defaults fields PolicySpec leaves unset,
	// so CreateOrUpdate reports an update on every reconcile.
	if op == controllerutil.OperationResultCreated {
		ctrl.LoggerFrom(ctx).Info("Created ValidatingAdmissionPolicy", "policy", c.config.PolicyName)
	}

	binding := &admissionv1.ValidatingAdmissionPolicyBinding{}
	binding.Name = c.config.BindingName
	op, err = controllerutil.CreateOrUpdate(ctx, c.hostedClient, binding, func() error {
		binding.Spec = *c.config.BindingSpec.DeepCopy()
		return nil
	})
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling validating admission policy binding %s: %w", c.config.BindingName, err)
	}
	switch op {
	case controllerutil.OperationResultCreated:
		ctrl.LoggerFrom(ctx).Info("Created ValidatingAdmissionPolicyBinding", "binding", c.config.BindingName)
	case controllerutil.OperationResultUpdated:
		ctrl.LoggerFrom(ctx).Info("Updated ValidatingAdmissionPolicyBinding", "binding", c.config.BindingName)
	}
	return ctrl.Result{}, nil
}

func (c *Controller) policySpec() admissionv1.ValidatingAdmissionPolicySpec {
	spec := *c.config.PolicySpec.DeepCopy()
	spec.MatchConditions = append(spec.MatchConditions, excludeHCCOUserCondition)
	return spec
}
