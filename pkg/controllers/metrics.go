package controllers

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"

	"sigs.k8s.io/controller-runtime/pkg/client"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/prometheus/client_golang/prometheus"
)

const OperatorContainerName = "karpenter-operator"

const OperatorInfoMetricName = "karpenter_operator_info"

// operatorImage returns the image reported by the running operator container.
// A missing container status means the kubelet has not reported it yet.
func operatorImage(ctx context.Context, reader client.Reader, namespace, podName string) (string, error) {
	pod := &corev1.Pod{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, pod); err != nil {
		return "", fmt.Errorf("get operator pod: %w", err)
	}

	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == OperatorContainerName && status.Image != "" {
			return status.Image, nil
		}
	}
	return "", nil
}

// setupOperatorInfoMetric registers a metric describing the running operator.
func setupOperatorInfoMetric(ctx context.Context, reader client.Reader, namespace string, registerer prometheus.Registerer) error {
	podName := os.Getenv("POD_NAME")
	if podName == "" {
		return fmt.Errorf("POD_NAME is not set")
	}

	image, err := waitForOperatorImage(ctx, reader, namespace, podName)
	if err != nil {
		return fmt.Errorf("read operator image: %w", err)
	}

	return registerer.Register(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: OperatorInfoMetricName,
		Help: "Information about the running Karpenter operator container.",
		ConstLabels: prometheus.Labels{
			"image":      image,
			"go_version": runtime.Version(),
			"go_arch":    runtime.GOARCH,
		},
	}, func() float64 { return 1 }))
}

// SetupOperatorInfoMetricWithRetry retries metric setup up to five times, stopping
// after successful registration or context cancellation.
func SetupOperatorInfoMetricWithRetry(ctx context.Context, reader client.Reader, namespace string) {
	setupOperatorInfoMetricWithRetry(ctx, reader, namespace, crmetrics.Registry)
}

func setupOperatorInfoMetricWithRetry(ctx context.Context, reader client.Reader, namespace string, registerer prometheus.Registerer) {
	backoff := wait.Backoff{
		Duration: time.Second,
		Factor:   2,
		Steps:    5,
	}

	err := wait.ExponentialBackoffWithContext(ctx, backoff, func(ctx context.Context) (bool, error) {
		if err := setupOperatorInfoMetric(ctx, reader, namespace, registerer); err != nil {
			klog.Error("failed to set up operator info metric: ", err)
			return false, nil
		}
		return true, nil
	})

	if err != nil && ctx.Err() == nil {
		klog.Error("failed to set up operator info metric after 5 attempts: ", err)
	}
}

func waitForOperatorImage(ctx context.Context, reader client.Reader, namespace, podName string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	for {
		image, err := operatorImage(ctx, reader, namespace, podName)
		if err != nil && !apierrors.IsNotFound(err) {
			return "", err
		}
		if err == nil && image != "" {
			return image, nil
		}

		select {
		case <-ctx.Done():
			return "", fmt.Errorf("operator pod %s/%s container status unavailable: %w", namespace, podName, ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}
