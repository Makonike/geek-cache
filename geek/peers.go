package geek

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/Makonike/geek-cache/geek/consistenthash"
	registry "github.com/Makonike/geek-cache/geek/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// PeerPicker must be implemented to locate the peer that owns a specific key
type PeerPicker interface {
	PickPeer(key string) (peer PeerGetter, ok bool, isSelf bool)
}

// PeerGetter must be implemented by a peer
type PeerGetter interface {
	Get(group string, key string) ([]byte, error)
	Delete(group string, key string) (bool, error)
}

type ClientPicker struct {
	self        string // self ip
	serviceName string
	mu          sync.RWMutex        // guards
	consHash    *consistenthash.Map // stores the list of peers, selected by specific key
	clients     map[string]*Client  // keyed by e.g. "10.0.0.2:8009"
	stopSignal  chan error          // signal to stop
}

func NewClientPicker(self string, opts ...PickerOptions) *ClientPicker {
	picker := ClientPicker{
		self:        self,
		serviceName: defaultServiceName,
		clients:     make(map[string]*Client),
		mu:          sync.RWMutex{},
		consHash:    consistenthash.New(),
	}
	for _, opt := range opts {
		opt(&picker)
	}

	// 增量更新
	// TODO: watch closed
	picker.set(picker.self)
	go func() {
		cli, err := clientv3.New(*registry.GlobalClientConfig)
		if err != nil {
			log.Fatal(err)
			return
		}
		defer cli.Close()
		// watcher will watch for changes of the service node
		watcher := clientv3.NewWatcher(cli)
		watchCh := watcher.Watch(context.Background(), picker.serviceName, clientv3.WithPrefix())
		for {
			a := <-watchCh
			go func() {
				picker.mu.Lock()
				defer picker.mu.Unlock()
				for _, x := range a.Events {
					// x: geek-cache/127.0.0.1:8004
					key := string(x.Kv.Key)
					idx := strings.Index(key, picker.serviceName)
					addr := key[idx+len(picker.serviceName)+1:]
					if addr == picker.self {
						continue
					}
					if x.IsCreate() {
						if _, ok := picker.clients[addr]; !ok {
							picker.set(addr)
						}
					} else if x.Type == clientv3.EventTypeDelete {
						if _, ok := picker.clients[addr]; ok {
							picker.remove(addr)
						}
					}
				}
			}()
		}
	}()

	// 全量更新
	go func() {
		picker.mu.Lock()
		cli, err := clientv3.New(*registry.GlobalClientConfig)
		if err != nil {
			log.Fatal(err)
			return
		}
		defer cli.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		resp, err := cli.Get(ctx, picker.serviceName, clientv3.WithPrefix())
		if err != nil {
			log.Panic("[Event] full copy request failed")
		}
		kvs := resp.OpResponse().Get().Kvs

		defer picker.mu.Unlock()
		for _, kv := range kvs {
			key := string(kv.Key)
			idx := strings.Index(key, picker.serviceName)
			addr := key[idx+len(picker.serviceName)+1:]

			if _, ok := picker.clients[addr]; !ok {
				picker.set(addr)
			}

		}
	}()

	// register itself
	go func() {
		err := registry.Register(picker.serviceName, picker.self, picker.stopSignal)
		if err != nil {
			log.Fatalf(err.Error())
		}
		close(picker.stopSignal)
		if err != nil {
			log.Fatalf(err.Error())
		}
		log.Printf("[%s] Revoke service and close tcp socket ok", picker.self)
	}()

	return &picker
}

type PickerOptions func(*ClientPicker)

func PickerServiceName(serviceName string) PickerOptions {
	return func(picker *ClientPicker) {
		picker.serviceName = serviceName
	}
}

func ConsHashOptions(opts ...consistenthash.ConsOptions) PickerOptions {
	return func(picker *ClientPicker) {
		picker.consHash = consistenthash.New(opts...)
	}
}

func (p *ClientPicker) set(addr string) {
	p.consHash.Add(addr)
	p.clients[addr] = NewClient(addr, p.serviceName)
}

func (p *ClientPicker) remove(addr string) {
	p.consHash.Remove(addr)
	delete(p.clients, addr)
}

// PickPeer pick a peer with the consistenthash algorithm
func (s *ClientPicker) PickPeer(key string) (PeerGetter, bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if peer := s.consHash.Get(key); peer != "" {
		s.Log("Pick peer %s", peer)
		return s.clients[peer], true, peer == s.self
	}
	return nil, false, false
}

// Log info
func (s *ClientPicker) Log(format string, path ...interface{}) {
	log.Printf("[Server %s] %s", s.self, fmt.Sprintf(format, path...))
}
