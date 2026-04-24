package k8s

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tilt-dev/tilt/pkg/logger"
)

func ownerFetcherTestCtx() context.Context {
	return logger.WithLogger(context.Background(), logger.NewTestLogger(&bytes.Buffer{}))
}

func TestVisitOneParent(t *testing.T) {
	kCli := NewFakeK8sClient(t)
	ov := NewOwnerFetcher(context.Background(), kCli)

	pod, rs := fakeOneParentChain()
	kCli.Inject(NewK8sEntity(rs))

	tree, err := ov.OwnerTreeOf(context.Background(), NewK8sEntity(pod))
	assert.NoError(t, err)
	assert.Equal(t, `Pod:pod-a
  ReplicaSet:rs-a`, tree.String())
}

func TestVisitTwoParentsEnsureListCaching(t *testing.T) {
	kCli := NewFakeK8sClient(t)
	ov := NewOwnerFetcher(context.Background(), kCli)

	pod, rs, dep := fakeTwoParentChain()
	kCli.Inject(NewK8sEntity(rs), NewK8sEntity(dep))

	tree, err := ov.OwnerTreeOf(context.Background(), NewK8sEntity(pod))
	assert.NoError(t, err)
	assert.Equal(t, `Pod:pod-a
  ReplicaSet:rs-a
    Deployment:dep-a`, tree.String())
	assert.Equal(t, 2, kCli.listCallCount)
	assert.Equal(t, 0, kCli.getByReferenceCallCount)
}

func TestVisitTwoParentsNoList(t *testing.T) {
	kCli := NewFakeK8sClient(t)
	kCli.listReturnsEmpty = true
	ov := NewOwnerFetcher(context.Background(), kCli)

	pod, rs, dep := fakeTwoParentChain()
	kCli.Inject(NewK8sEntity(rs), NewK8sEntity(dep))

	tree, err := ov.OwnerTreeOf(context.Background(), NewK8sEntity(pod))
	assert.NoError(t, err)
	assert.Equal(t, `Pod:pod-a
  ReplicaSet:rs-a
    Deployment:dep-a`, tree.String())
	assert.Equal(t, 2, kCli.listCallCount)
	assert.Equal(t, 2, kCli.getByReferenceCallCount)
}

func TestOwnerFetcherParallelism(t *testing.T) {
	kCli := NewFakeK8sClient(t)
	kCli.listReturnsEmpty = true
	ov := NewOwnerFetcher(context.Background(), kCli)

	pod, rs := fakeOneParentChain()
	kCli.Inject(NewK8sEntity(rs))

	count := 30
	g, ctx := errgroup.WithContext(context.Background())
	for i := 0; i < count; i++ {
		g.Go(func() error {
			_, err := ov.OwnerTreeOf(ctx, NewK8sEntity(pod))
			return err
		})
	}

	err := g.Wait()
	assert.NoError(t, err)
	assert.Equal(t, 1, kCli.getByReferenceCallCount)
}

func TestCircular(t *testing.T) {
	kCli := NewFakeK8sClient(t)
	kCli.listReturnsEmpty = true
	ov := NewOwnerFetcher(context.Background(), kCli)

	pod1, pod2, pod3 := fakeCircularReference()
	kCli.Inject(NewK8sEntity(pod2), NewK8sEntity(pod3))

	tree, err := ov.OwnerTreeOf(context.Background(), NewK8sEntity(pod1))
	assert.NoError(t, err)
	assert.Equal(t, `Pod:pod-a
  Pod:pod-b
    Pod:pod-c`, tree.String())
}

// TestTransientErrorNotCachedPermanently reproduces the production bug where a
// single transient API error (e.g. 429 throttling on a loaded cluster) would
// permanently poison the OwnerFetcher cache, causing all future lookups for
// that UID to return the cached error forever.
//
// Scenario: pod watch fires → OwnerTreeOf → GetMetaByReference returns 429 →
// error is cached. API server recovers → next pod watch fires →
// OwnerTreeOf should retry (not return cached error).
func TestTransientErrorNotCachedPermanently(t *testing.T) {
	ctx := ownerFetcherTestCtx()
	kCli := NewFakeK8sClient(t)
	kCli.listReturnsEmpty = true
	ov := NewOwnerFetcher(ctx, kCli)

	pod, rs := fakeOneParentChain()
	kCli.Inject(NewK8sEntity(rs))

	// Step 1: API server is overloaded, GetMetaByReference returns 429.
	kCli.getByReferenceError = fmt.Errorf("server throttled: 429")

	_, err := ov.OwnerTreeOf(ctx, NewK8sEntity(pod))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "429")

	// Step 2: API server recovers.
	kCli.getByReferenceError = nil

	// Step 3: Next pod watch event fires, calling OwnerTreeOf again for the
	// same UID. Before the fix, this would return the cached 429 error
	// forever. After the fix, the evicted promise allows a fresh attempt.
	tree, err := ov.OwnerTreeOf(ctx, NewK8sEntity(pod))
	assert.NoError(t, err)
	assert.Equal(t, `Pod:pod-a
  ReplicaSet:rs-a`, tree.String())
}

// TestTransientListMetaErrorRetries verifies that a failed ListMeta call
// (used to batch-fetch metadata for a resource type) does not permanently
// prevent metadata fetching. Before the fix, sync.Once would consume the
// attempt even on failure, so the fetch would never retry.
func TestTransientListMetaErrorRetries(t *testing.T) {
	ctx := ownerFetcherTestCtx()
	kCli := NewFakeK8sClient(t)
	ov := NewOwnerFetcher(ctx, kCli)

	pod, rs := fakeOneParentChain()
	kCli.Inject(NewK8sEntity(rs))

	// Step 1: ListMeta fails (throttled). The fetch falls through to
	// GetMetaByReference, which still works.
	kCli.listMetaError = fmt.Errorf("server throttled: 429")

	tree, err := ov.OwnerTreeOf(ctx, NewK8sEntity(pod))
	assert.NoError(t, err)
	assert.Equal(t, `Pod:pod-a
  ReplicaSet:rs-a`, tree.String())
	assert.Equal(t, 1, kCli.getByReferenceCallCount)

	// Step 2: API server recovers. New OwnerFetcher simulates the same
	// process with the retriableOnce still in play.
	kCli.listMetaError = nil
	ov2 := NewOwnerFetcher(ctx, kCli)

	pod2 := pod.DeepCopy()
	pod2.Name = "pod-b"
	pod2.UID = "pod-b-uid"
	tree2, err := ov2.OwnerTreeOf(ctx, NewK8sEntity(pod2))
	assert.NoError(t, err)
	assert.Equal(t, `Pod:pod-b
  ReplicaSet:rs-a`, tree2.String())
}

// TestCascadingErrorEviction reproduces the cascading failure: when a pod's
// owner (ReplicaSet) lookup fails, both the pod's promise AND the
// ReplicaSet's promise are rejected. Without eviction, every pod under that
// ReplicaSet is permanently broken — not just the one that triggered the
// error.
func TestCascadingErrorEviction(t *testing.T) {
	ctx := ownerFetcherTestCtx()
	kCli := NewFakeK8sClient(t)
	kCli.listReturnsEmpty = true
	ov := NewOwnerFetcher(ctx, kCli)

	pod, rs, dep := fakeTwoParentChain()
	kCli.Inject(NewK8sEntity(rs), NewK8sEntity(dep))

	// Step 1: Transient error during owner tree resolution.
	// The pod calls ownerTreeOfRefHelper(RS), which calls
	// GetMetaByReference(RS) → error. Both the pod promise and the RS
	// promise are rejected.
	kCli.getByReferenceError = fmt.Errorf("connection refused")

	_, err := ov.OwnerTreeOf(ctx, NewK8sEntity(pod))
	assert.Error(t, err)

	// Step 2: Error clears.
	kCli.getByReferenceError = nil

	// Step 3: Both the pod AND the RS promises must have been evicted for
	// the full Pod → RS → Deployment chain to resolve.
	tree, err := ov.OwnerTreeOf(ctx, NewK8sEntity(pod))
	assert.NoError(t, err)
	assert.Equal(t, `Pod:pod-a
  ReplicaSet:rs-a
    Deployment:dep-a`, tree.String())
}

func fakeOneParentChain() (*v1.Pod, *appsv1.ReplicaSet) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-a",
			UID:       "pod-a-uid",
			Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "ReplicaSet",
					Name:       "rs-a",
					UID:        "rs-a-uid",
				},
			},
		},
	}
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rs-a",
			UID:       "rs-a-uid",
			Namespace: "default",
		},
	}
	return pod, rs
}

func fakeTwoParentChain() (*v1.Pod, *appsv1.ReplicaSet, *appsv1.Deployment) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-a",
			UID:       "pod-a-uid",
			Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "ReplicaSet",
					Name:       "rs-a",
					UID:        "rs-a-uid",
				},
			},
		},
	}
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rs-a",
			UID:       "rs-a-uid",
			Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "dep-a",
					UID:        "dep-a-uid",
				},
			},
		},
	}
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "dep-a",
			UID:       "dep-a-uid",
			Namespace: "default",
		},
	}
	return pod, rs, dep
}

func fakeCircularReference() (*v1.Pod, *v1.Pod, *v1.Pod) {
	pod1 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-a",
			UID:       "pod-a-uid",
			Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Kind:       "Pod",
					Name:       "pod-b",
					UID:        "pod-b-uid",
				},
			},
		},
	}
	pod2 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-b",
			UID:       "pod-b-uid",
			Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Kind:       "Pod",
					Name:       "pod-c",
					UID:        "pod-c-uid",
				},
			},
		},
	}
	pod3 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-c",
			UID:       "pod-c-uid",
			Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Kind:       "Pod",
					Name:       "pod-a",
					UID:        "pod-a-uid",
				},
			},
		},
	}

	return pod1, pod2, pod3
}
