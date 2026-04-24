package k8s

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"golang.org/x/exp/slices"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/tilt-dev/tilt/pkg/logger"
)

// The ObjectRefTree only contains immutable properties
// of a Kubernetes object: the name, namespace, and UID
type ObjectRefTree struct {
	Ref               v1.ObjectReference
	CreationTimestamp metav1.Time
	Owners            []ObjectRefTree
}

func (t ObjectRefTree) UIDs() []types.UID {
	result := []types.UID{t.Ref.UID}
	for _, owner := range t.Owners {
		result = append(result, owner.UIDs()...)
	}
	return result
}

func (t ObjectRefTree) stringLines() []string {
	result := []string{fmt.Sprintf("%s:%s", t.Ref.Kind, t.Ref.Name)}
	for _, owner := range t.Owners {
		// indent each of the owners by two spaces
		branchLines := owner.stringLines()
		for _, branchLine := range branchLines {
			result = append(result, fmt.Sprintf("  %s", branchLine))
		}
	}
	return result
}

func (t ObjectRefTree) String() string {
	return strings.Join(t.stringLines(), "\n")
}

type resourceNamespace struct {
	Namespace Namespace
	GVK       schema.GroupVersionKind
}

// retriableOnce is like sync.Once but allows retrying if the function reports failure.
// On success, subsequent calls are no-ops (like sync.Once).
// On failure, subsequent calls will re-attempt the function.
type retriableOnce struct {
	mu   sync.Mutex
	done bool
}

func (r *retriableOnce) Do(f func() bool) {
	r.mu.Lock()
	if r.done {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	// Unlike sync.Once we don't deduplicate concurrent calls, but this is
	// acceptable for metadata fetches where the cost of a rare duplicate
	// list is far lower than the cost of a permanently failed cache entry.
	ok := f()

	if ok {
		r.mu.Lock()
		r.done = true
		r.mu.Unlock()
	}
}

type MetaClient interface {
	GetMetaByReference(ctx context.Context, ref v1.ObjectReference) (metav1.Object, error)
	ListMeta(ctx context.Context, gvk schema.GroupVersionKind, ns Namespace) ([]metav1.Object, error)
	WatchMeta(ctx context.Context, gvk schema.GroupVersionKind, ns Namespace) (<-chan metav1.Object, error)
}

type OwnerFetcher struct {
	globalCtx context.Context
	cli       MetaClient
	cache     map[types.UID]*objectTreePromise
	mu        *sync.Mutex

	metaCache       map[types.UID]metav1.Object
	resourceFetches map[resourceNamespace]*retriableOnce
}

func NewOwnerFetcher(ctx context.Context, metaClient MetaClient) OwnerFetcher {
	return OwnerFetcher{
		globalCtx: ctx,
		cli:       metaClient,
		cache:     make(map[types.UID]*objectTreePromise),
		mu:        &sync.Mutex{},

		metaCache:       make(map[types.UID]metav1.Object),
		resourceFetches: make(map[resourceNamespace]*retriableOnce),
	}
}

func (v OwnerFetcher) getOrCreateResourceFetch(gvk schema.GroupVersionKind, ns Namespace) *retriableOnce {
	v.mu.Lock()
	defer v.mu.Unlock()
	rns := resourceNamespace{Namespace: ns, GVK: gvk}
	fetch, ok := v.resourceFetches[rns]
	if !ok {
		fetch = &retriableOnce{}
		v.resourceFetches[rns] = fetch
	}
	return fetch
}

// As an optimization, we batch fetch all the ObjectMetas of a resource type
// the first time we need that resource, then watch updates.
func (v OwnerFetcher) ensureResourceFetched(gvk schema.GroupVersionKind, ns Namespace) {
	fetch := v.getOrCreateResourceFetch(gvk, ns)
	fetch.Do(func() bool {
		metas, err := v.cli.ListMeta(v.globalCtx, gvk, ns)
		if err != nil {
			logger.Get(v.globalCtx).Warnf("Error fetching metadata for %s in %s: %v", gvk, ns, err)
			return false
		}

		v.mu.Lock()
		for _, meta := range metas {
			v.metaCache[meta.GetUID()] = meta
		}
		v.mu.Unlock()

		ch, err := v.cli.WatchMeta(v.globalCtx, gvk, ns)
		if err != nil {
			logger.Get(v.globalCtx).Warnf("Error watching metadata for %s in %s: %v", gvk, ns, err)
			return false
		}

		go func() {
			for meta := range ch {
				// NOTE(nick): I don't think we can ever get a blank UID, but want to protect
				// us from weird k8s bugs.
				if meta.GetUID() == "" {
					continue
				}

				v.mu.Lock()
				v.metaCache[meta.GetUID()] = meta
				v.mu.Unlock()
			}
		}()

		return true
	})
}

// Returns a promise and a boolean. The boolean is true if the promise is
// already in progress, and false if the caller is responsible for
// resolving/rejecting the promise.
func (v OwnerFetcher) getOrCreatePromise(id types.UID) (*objectTreePromise, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	promise, ok := v.cache[id]
	if !ok {
		promise = newObjectTreePromise()
		v.cache[id] = promise
	}
	return promise, ok
}

// evictPromise removes a rejected promise from the cache so that subsequent
// calls to getOrCreatePromise will create a fresh promise and retry.
// This prevents transient API errors from permanently poisoning the cache.
func (v OwnerFetcher) evictPromise(id types.UID) {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.cache, id)
}

func (v OwnerFetcher) OwnerTreeOfRef(ctx context.Context, ref v1.ObjectReference) (result ObjectRefTree, err error) {
	return v.ownerTreeOfRefHelper(ctx, ref, nil)
}

func (v OwnerFetcher) ownerTreeOfRefHelper(ctx context.Context, ref v1.ObjectReference, path []types.UID) (result ObjectRefTree, err error) {
	uid := ref.UID
	if uid == "" {
		return ObjectRefTree{}, fmt.Errorf("Can only get owners of deployed entities")
	}

	promise, ok := v.getOrCreatePromise(uid)
	if ok {
		return promise.wait()
	}

	defer func() {
		if err != nil {
			promise.reject(err)
			v.evictPromise(uid)
		} else {
			promise.resolve(result)
		}
	}()

	meta, err := v.getMetaByReference(ctx, ref)
	if err != nil {
		if errors.IsNotFound(err) {
			logger.Get(ctx).Debugf("owner_fetcher: %s %s/%s (uid=%s) not found; owner tree will be truncated",
				ref.Kind, ref.Namespace, ref.Name, ref.UID)
			return ObjectRefTree{Ref: ref}, nil
		}
		return ObjectRefTree{}, err
	}
	return v.ownerTreeOfHelper(ctx, ref, meta, path)
}

func (v OwnerFetcher) getMetaByReference(ctx context.Context, ref v1.ObjectReference) (metav1.Object, error) {
	gvk := ReferenceGVK(ref)
	v.ensureResourceFetched(gvk, Namespace(ref.Namespace))

	v.mu.Lock()
	meta, ok := v.metaCache[ref.UID]
	v.mu.Unlock()

	if ok {
		return meta, nil
	}

	return v.cli.GetMetaByReference(ctx, ref)
}

func (v OwnerFetcher) OwnerTreeOf(ctx context.Context, entity K8sEntity) (result ObjectRefTree, err error) {
	meta := entity.Meta()
	uid := meta.GetUID()
	if uid == "" {
		return ObjectRefTree{}, fmt.Errorf("Can only get owners of deployed entities")
	}

	promise, ok := v.getOrCreatePromise(uid)
	if ok {
		return promise.wait()
	}

	defer func() {
		if err != nil {
			promise.reject(err)
			v.evictPromise(uid)
		} else {
			promise.resolve(result)
		}
	}()

	ref := entity.ToObjectReference()
	return v.ownerTreeOfHelper(ctx, ref, meta, nil)
}

func (v OwnerFetcher) ownerTreeOfHelper(ctx context.Context, ref v1.ObjectReference, meta metav1.Object, path []types.UID) (ObjectRefTree, error) {
	tree := ObjectRefTree{Ref: ref, CreationTimestamp: meta.GetCreationTimestamp()}
	owners := meta.GetOwnerReferences()
	for _, owner := range owners {
		// TODO: Owner references can also exist at cluster scope, for which this incorrectly propagates the parent ref's Namespace.
		ownerRef := OwnerRefToObjectRef(owner, meta.GetNamespace())
		if slices.Contains(path, owner.UID) {
			// break circular dependencies
			continue
		}
		ownerTree, err := v.ownerTreeOfRefHelper(ctx, ownerRef, append(path, ref.UID))
		if err != nil {
			return ObjectRefTree{}, err
		}
		tree.Owners = append(tree.Owners, ownerTree)
	}
	return tree, nil
}

func OwnerRefToObjectRef(owner metav1.OwnerReference, namespace string) v1.ObjectReference {
	return v1.ObjectReference{
		APIVersion: owner.APIVersion,
		Kind:       owner.Kind,
		Namespace:  namespace,
		Name:       owner.Name,
		UID:        owner.UID,
	}
}

func RuntimeObjToOwnerRef(obj runtime.Object) metav1.OwnerReference {
	e := NewK8sEntity(obj)
	ref := e.ToObjectReference()
	return metav1.OwnerReference{
		APIVersion: ref.APIVersion,
		Kind:       ref.Kind,
		Name:       ref.Name,
		UID:        ref.UID,
	}
}

type objectTreePromise struct {
	tree ObjectRefTree
	err  error
	done chan struct{}
}

func newObjectTreePromise() *objectTreePromise {
	return &objectTreePromise{
		done: make(chan struct{}),
	}
}

func (e *objectTreePromise) resolve(tree ObjectRefTree) {
	e.tree = tree
	close(e.done)
}

func (e *objectTreePromise) reject(err error) {
	e.err = err
	close(e.done)
}

func (e *objectTreePromise) wait() (ObjectRefTree, error) {
	<-e.done
	return e.tree, e.err
}
