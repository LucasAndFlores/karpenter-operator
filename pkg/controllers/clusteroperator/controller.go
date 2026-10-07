package clusteroperator

import (
	"context"
	"fmt"

	autoscalingv1alpha1 "github.com/openshift/karpenter-operator/pkg/apis/autoscaling/v1alpha1"

	configv1 "github.com/openshift/api/config/v1"
	configac "github.com/openshift/client-go/config/applyconfigurations/config/v1"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

const (
	clusterOperatorName = "karpenter"
	fieldManager        = "karpenter-operator"
)

type ControllerConfig struct {
	Namespace                string
	ReleaseVersion           string
	AdditionalRelatedObjects []configv1.ObjectReference
}

type Controller struct {
	client client.Client
	config *ControllerConfig
}

func NewController(mgr ctrl.Manager, cfg *ControllerConfig) *Controller {
	return &Controller{
		client: mgr.GetClient(),
		config: cfg,
	}
}

func (r *Controller) Name() string {
	return "clusteroperator"
}

func (r *Controller) Reconcile(ctx context.Context, _ ctrl.Request) (ctrl.Result, error) {
	var conditions []*configac.ClusterOperatorStatusConditionApplyConfiguration
	conditions = append(conditions, r.operandConditions(ctx)...)

	if err := r.applyStatus(ctx, conditions); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating ClusterOperator status: %w", err)
	}
	return ctrl.Result{}, nil
}

func (r *Controller) operandConditions(ctx context.Context) []*configac.ClusterOperatorStatusConditionApplyConfiguration {
	karp := &autoscalingv1alpha1.Karpenter{}
	if err := r.client.Get(ctx, client.ObjectKey{Name: autoscalingv1alpha1.SingletonName}, karp); err != nil {
		if apierrors.IsNotFound(err) {
			return availableConditions("KarpenterNotFound", fmt.Sprintf("at version %s", r.config.ReleaseVersion))
		}
		return degradedConditions("KarpenterCheckFailed", fmt.Sprintf("Failed to get Karpenter CR: %v", err))
	}

	deploy := &appsv1.Deployment{}
	err := r.client.Get(ctx, client.ObjectKey{Namespace: r.config.Namespace, Name: "karpenter"}, deploy)

	switch {
	case apierrors.IsNotFound(err):
		return progressingConditions("OperandNotYetCreated", "Waiting for karpenter Deployment to be created")
	case err != nil:
		return degradedConditions("OperandCheckFailed", fmt.Sprintf("Failed to get operand Deployment: %v", err))
	case deploy.Status.AvailableReplicas < 1:
		return progressingConditions("OperandNotReady", "Waiting for karpenter Deployment to become available")
	case deploy.Status.UpdatedReplicas != deploy.Status.Replicas:
		return progressingConditions("OperandRollingOut", "Karpenter Deployment is rolling out")
	default:
		return availableConditions("AsExpected", fmt.Sprintf("at version %s", r.config.ReleaseVersion))
	}
}

func (r *Controller) applyStatus(ctx context.Context, conditions []*configac.ClusterOperatorStatusConditionApplyConfiguration) error {
	// Ensure the ClusterOperator object exists.
	co := configac.ClusterOperator(clusterOperatorName)
	if err := r.client.Apply(ctx, co, client.FieldOwner(fieldManager)); err != nil {
		return fmt.Errorf("applying ClusterOperator: %w", err)
	}

	// Read existing conditions to preserve LastTransitionTime for unchanged statuses.
	existing := &configv1.ClusterOperator{}
	if err := r.client.Get(ctx, client.ObjectKey{Name: clusterOperatorName}, existing); err != nil {
		return fmt.Errorf("reading existing ClusterOperator: %w", err)
	}

	now := metav1.Now()
	for _, c := range conditions {
		if c.LastTransitionTime == nil {
			c.WithLastTransitionTime(now)
		}
		if c.Type != nil && c.Status != nil {
			if prev := findCondition(existing.Status.Conditions, *c.Type); prev != nil && prev.Status == *c.Status {
				c.WithLastTransitionTime(prev.LastTransitionTime)
			}
		}
	}

	status := configac.ClusterOperator(clusterOperatorName).
		WithStatus(configac.ClusterOperatorStatus().
			WithConditions(conditions...).
			WithVersions(
				configac.OperandVersion().WithName("operator").WithVersion(r.config.ReleaseVersion),
			).
			WithRelatedObjects(r.relatedObjects()...),
		)
	return r.client.SubResource("status").Apply(ctx, status, client.FieldOwner(fieldManager), client.ForceOwnership)
}

func (r *Controller) relatedObjects() []*configac.ObjectReferenceApplyConfiguration {
	objs := []*configac.ObjectReferenceApplyConfiguration{
		configac.ObjectReference().WithGroup("").WithResource("namespaces").WithName(r.config.Namespace),
		configac.ObjectReference().WithGroup("apps").WithResource("deployments").WithName("karpenter-operator").WithNamespace(r.config.Namespace),
		configac.ObjectReference().WithGroup("rbac.authorization.k8s.io").WithResource("clusterroles").WithName("karpenter-operator"),
		configac.ObjectReference().WithGroup("rbac.authorization.k8s.io").WithResource("clusterrolebindings").WithName("karpenter-operator"),
		configac.ObjectReference().WithGroup("karpenter.sh").WithResource("nodepools").WithName(""),
		configac.ObjectReference().WithGroup("karpenter.sh").WithResource("nodeclaims").WithName(""),
	}
	for _, ref := range r.config.AdditionalRelatedObjects {
		objs = append(objs, configac.ObjectReference().
			WithGroup(ref.Group).
			WithResource(ref.Resource).
			WithName(ref.Name).
			WithNamespace(ref.Namespace))
	}
	return objs
}

func (r *Controller) SetupWithManager(mgr ctrl.Manager) error {
	reconcileRequest := []ctrl.Request{{NamespacedName: client.ObjectKey{Name: clusterOperatorName}}}

	return ctrl.NewControllerManagedBy(mgr).
		Named(r.Name()).
		For(&configv1.ClusterOperator{}, builder.WithPredicates(predicate.NewPredicateFuncs(func(o client.Object) bool {
			return o.GetName() == clusterOperatorName
		}))).
		Watches(&autoscalingv1alpha1.Karpenter{}, handler.EnqueueRequestsFromMapFunc(
			func(_ context.Context, o client.Object) []ctrl.Request {
				if o.GetName() != autoscalingv1alpha1.SingletonName {
					return nil
				}
				return reconcileRequest
			},
		)).
		Watches(&appsv1.Deployment{}, handler.EnqueueRequestsFromMapFunc(
			func(_ context.Context, o client.Object) []ctrl.Request {
				if o.GetNamespace() != r.config.Namespace || o.GetName() != "karpenter" {
					return nil
				}
				return reconcileRequest
			},
		)).
		Complete(r)
}

// --- Condition builders ---

func condition(condType configv1.ClusterStatusConditionType, status configv1.ConditionStatus, reason, message string) *configac.ClusterOperatorStatusConditionApplyConfiguration {
	c := configac.ClusterOperatorStatusCondition().
		WithType(condType).
		WithStatus(status).
		WithReason(reason)
	if message != "" {
		c.WithMessage(message)
	}
	return c
}

func availableConditions(reason, message string) []*configac.ClusterOperatorStatusConditionApplyConfiguration {
	return []*configac.ClusterOperatorStatusConditionApplyConfiguration{
		condition(configv1.OperatorAvailable, configv1.ConditionTrue, reason, message),
		condition(configv1.OperatorProgressing, configv1.ConditionFalse, "AsExpected", ""),
		condition(configv1.OperatorDegraded, configv1.ConditionFalse, "AsExpected", ""),
		condition(configv1.OperatorUpgradeable, configv1.ConditionTrue, "AsExpected", ""),
	}
}

func progressingConditions(reason, message string) []*configac.ClusterOperatorStatusConditionApplyConfiguration {
	return []*configac.ClusterOperatorStatusConditionApplyConfiguration{
		condition(configv1.OperatorAvailable, configv1.ConditionTrue, "AsExpected", ""),
		condition(configv1.OperatorProgressing, configv1.ConditionTrue, reason, message),
		condition(configv1.OperatorDegraded, configv1.ConditionFalse, "AsExpected", ""),
		condition(configv1.OperatorUpgradeable, configv1.ConditionTrue, "AsExpected", ""),
	}
}

func degradedConditions(reason, message string) []*configac.ClusterOperatorStatusConditionApplyConfiguration {
	return []*configac.ClusterOperatorStatusConditionApplyConfiguration{
		condition(configv1.OperatorAvailable, configv1.ConditionTrue, "AsExpected", ""),
		condition(configv1.OperatorProgressing, configv1.ConditionFalse, "AsExpected", ""),
		condition(configv1.OperatorDegraded, configv1.ConditionTrue, reason, message),
		condition(configv1.OperatorUpgradeable, configv1.ConditionTrue, "AsExpected", ""),
	}
}

func findCondition(conditions []configv1.ClusterOperatorStatusCondition, condType configv1.ClusterStatusConditionType) *configv1.ClusterOperatorStatusCondition {
	for i := range conditions {
		if conditions[i].Type == condType {
			return &conditions[i]
		}
	}
	return nil
}
