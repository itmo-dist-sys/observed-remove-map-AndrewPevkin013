package node

import (
	"context"
	"sync"
	"time"

	"github.com/nikitakosatka/hive/pkg/hive"
)

// Version is a logical LWW version for one key.
// Ordering is lexicographic: (Counter, NodeID).
type Version struct {
	Counter uint64
	NodeID  string
}

// StateEntry stores one OR-Map key state.
type StateEntry struct {
	Value     string
	Tombstone bool
	Version   Version
}

// MapState is an exported snapshot representation used by Merge.
type MapState map[string]StateEntry

// CRDTMapNode is a state-based OR-Map with LWW values.
type CRDTMapNode struct {
	*hive.BaseNode

	mu       sync.RWMutex
	state    MapState
	counter  uint64
	allNodes []string

	stopCh chan struct{}
	wg     sync.WaitGroup
}

func NewCRDTMapNode(id string, allNodeIDs []string) *CRDTMapNode {
	return &CRDTMapNode{
		BaseNode: hive.NewBaseNode(id),
		state:    make(MapState),
		counter:  0,
		allNodes: allNodeIDs,
		stopCh:   make(chan struct{}),
	}
}

func (n *CRDTMapNode) Start(ctx context.Context) error {
	if err := n.BaseNode.Start(ctx); err != nil {
		return err
	}
	n.startAntiEntropy()
	return nil
}

func (n *CRDTMapNode) startAntiEntropy() {
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				n.broadcastState()
			case <-n.stopCh:
				return
			case <-n.Context().Done():
				return
			}
		}
	}()
}

func (n *CRDTMapNode) broadcastState() {
	state := n.State()
	for _, nodeID := range n.allNodes {
		if nodeID == n.ID() {
			continue
		}
		msg := hive.NewMessage(n.ID(), nodeID, state)
		_ = n.SendMessage(msg)
	}
}

func (n *CRDTMapNode) Put(k, v string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.counter++
	ver := Version{Counter: n.counter, NodeID: n.ID()}
	n.state[k] = StateEntry{
		Value:     v,
		Tombstone: false,
		Version:   ver,
	}
}

func (n *CRDTMapNode) Get(k string) (string, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	entry, ok := n.state[k]
	if !ok || entry.Tombstone {
		return "", false
	}
	return entry.Value, true
}

func (n *CRDTMapNode) Delete(k string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.counter++
	ver := Version{Counter: n.counter, NodeID: n.ID()}
	n.state[k] = StateEntry{
		Value:     "",
		Tombstone: true,
		Version:   ver,
	}
}

func (n *CRDTMapNode) Merge(remote MapState) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for k, remoteEntry := range remote {
		localEntry, exists := n.state[k]
		if !exists || n.isNewer(remoteEntry.Version, localEntry.Version) {
			n.state[k] = remoteEntry
		}
	}
}

func (n *CRDTMapNode) isNewer(a, b Version) bool {
	if a.Counter != b.Counter {
		return a.Counter > b.Counter
	}
	return a.NodeID > b.NodeID
}

func (n *CRDTMapNode) State() MapState {
	n.mu.RLock()
	defer n.mu.RUnlock()

	copyState := make(MapState)
	for k, v := range n.state {
		copyState[k] = v
	}
	return copyState
}

func (n *CRDTMapNode) ToMap() map[string]string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	result := make(map[string]string)
	for k, entry := range n.state {
		if !entry.Tombstone {
			result[k] = entry.Value
		}
	}
	return result
}

func (n *CRDTMapNode) Stop() error {
	close(n.stopCh)
	n.wg.Wait()
	return n.BaseNode.Stop()
}

func (n *CRDTMapNode) Receive(msg *hive.Message) error {
	remoteState, ok := msg.Payload.(MapState)
	if !ok {
		return nil
	}
	n.Merge(remoteState)
	return nil
}
