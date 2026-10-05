package crd

import (
	"context"
	"fmt"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/client-go/util/workqueue"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/samber/lo"
)

type ControllerConfig struct {
	Namespace string
	CRDs      []*apiextensionsv1.CustomResourceDefinition

	// HostedCluster targets a remote cluster for CRD writes and watches.
	// When nil, the controller uses the manager's own client and cache (standalone mode).
	HostedCluster cluster.Cluster
}

// Controller reconciles the Karpenter CRDs (NodePool, NodeClaim, EC2NodeClass, etc.)
// so the operand can start its watches and caches.
type Controller struct {
	targetClient client.Client // writes CRDs to the target cluster
	targetCache  cache.Cache   // watches CRDs on the target cluster
	config       *ControllerConfig
}

func NewController(mgr ctrl.Manager, cfg *ControllerConfig) *Controller {
	targetClient := mgr.GetClient()
	targetCache := mgr.GetCache()
	if cfg.HostedCluster != nil {
		targetClient = cfg.HostedCluster.GetClient()
		targetCache = cfg.HostedCluster.GetCache()
	}
	return &Controller{
		targetClient: targetClient,
		targetCache:  targetCache,
		config:       cfg,
	}
}

func (c *Controller) Name() string {
	return "crd"
}

func (c *Controller) Reconcile(ctx context.Context, _ ctrl.Request) (ctrl.Result, error) {
	for _, desired := range c.config.CRDs {
		if err := c.applyCRD(ctx, desired); err != nil {
			return ctrl.Result{}, fmt.Errorf("applying CRD %s: %w", desired.Name, err)
		}
	}

	return ctrl.Result{}, nil
}

func (c *Controller) applyCRD(ctx context.Context, desired *apiextensionsv1.CustomResourceDefinition) error {
	crd := &apiextensionsv1.CustomResourceDefinition{}
	crd.Name = desired.Name
	op, err := controllerutil.CreateOrUpdate(ctx, c.targetClient, crd, func() error {
		crd.Spec = *desired.Spec.DeepCopy()
		return nil
	})
	if err != nil {
		return err
	}
	// Updates are not logged: the API server defaults fields the embedded CRDs leave unset,
	// so CreateOrUpdate reports an update on every reconcile.
	if op == controllerutil.OperationResultCreated {
		ctrl.LoggerFrom(ctx).Info("Created CRD", "crd", desired.Name)
	}
	return nil
}

func (c *Controller) SetupWithManager(mgr ctrl.Manager) error {
	ctrlr, err := controller.New(c.Name(), mgr, controller.Options{Reconciler: c})
	if err != nil {
		return fmt.Errorf("creating controller: %w", err)
	}

	managedCRDs := lo.KeyBy(c.config.CRDs, func(crd *apiextensionsv1.CustomResourceDefinition) string {
		return crd.Name
	})

	// Watch CRDs on the target cluster (hosted cluster in HCP mode, in-cluster in standalone).
	if err := ctrlr.Watch(source.Kind(c.targetCache, &apiextensionsv1.CustomResourceDefinition{},
		handler.TypedEnqueueRequestsFromMapFunc(func(_ context.Context, o *apiextensionsv1.CustomResourceDefinition) []reconcile.Request {
			if _, ok := managedCRDs[o.GetName()]; !ok {
				return nil
			}
			return []reconcile.Request{{NamespacedName: client.ObjectKey{Name: o.GetName()}}}
		}),
		predicate.TypedGenerationChangedPredicate[*apiextensionsv1.CustomResourceDefinition]{},
	)); err != nil {
		return fmt.Errorf("watching CRDs: %w", err)
	}

	// Trigger initial reconcile at startup to create CRDs before any watches fire.
	if err := ctrlr.Watch(source.Func(func(_ context.Context, q workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
		q.Add(reconcile.Request{})
		return nil
	})); err != nil {
		return fmt.Errorf("watching initial sync: %w", err)
	}

	return nil
}
