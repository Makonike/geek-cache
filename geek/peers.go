package geek

import (
	"fmt"
	"github.com/Makonike/geek-cache/geek/consistenthash"
	"log"
	"sync"
)

// PeerPicker must be implemented to locate the peer that owns a specific key
type PeerPicker interface {
	PickPeer(key string) (peer PeerGetter, ok bool)
}

// PeerGetter must be implemented by a peer
type PeerGetter interface {
	Get(group string, key string) ([]byte, error)
}

type ClientPicker struct {
	self     string              // self ip
	mu       sync.Mutex          // guards
	consHash *consistenthash.Map // stores the list of peers, selected by specific key
	clients  map[string]*Client  // keyed by e.g. "10.0.0.2:8009"
}

func NewClientPicker(self string) *ClientPicker {
	return &ClientPicker{}
}

// add peer to cluster, create a new Client instance for every peer
func (s *ClientPicker) Set(peers ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.consHash = consistenthash.New(defaultReplicas, nil)
	s.consHash.Add(peers...)
	s.clients = make(map[string]*Client, len(peers))
	for _, peer := range peers {
		s.clients[peer], _ = NewClient(peer)
	}
}

// PickPeer pick a peer with the consistenthash algorithm
func (s *ClientPicker) PickPeer(key string) (PeerGetter, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if peer := s.consHash.Get(key); peer != "" && peer != s.self {
		s.Log("Pick peer %s", peer)
		return s.clients[peer], true
	}
	return nil, false
}

func (s *ClientPicker) SetWithReplicas(replicas int, peers ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.consHash = consistenthash.New(replicas, nil)
	s.consHash.Add(peers...)
	s.clients = make(map[string]*Client, len(peers))
	for _, peer := range peers {
		s.clients[peer], _ = NewClient(peer)
	}
}

// Log info
func (s *ClientPicker) Log(format string, path ...interface{}) {
	log.Printf("[Server %s] %s", s.self, fmt.Sprintf(format, path...))
}
