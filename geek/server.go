package geek

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"

	pb "github.com/Makonike/geek-cache/geek/pb"
	"github.com/Makonike/geek-cache/geek/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	defaultServiceName = "geek-cache"
	defaultAddr        = "127.0.0.1:7654"
)

type Server struct {
	pb.UnimplementedGroupCacheServer
	self   string     // self ip
	status bool       // true if the server is running
	mu     sync.Mutex // guards
}

type ServerOptions func(*Server)

func NewServer(self string, opts ...ServerOptions) (*Server, error) {
	if self == "" {
		self = defaultAddr
	} else if !utils.ValidPeerAddr(self) {
		return nil, fmt.Errorf("invalid address: %v", self)
	}
	s := Server{
		self: self,
	}
	for _, opt := range opts {
		opt(&s)
	}
	return &s, nil
}

func (s *Server) Get(ctx context.Context, in *pb.Request) (*pb.ResponseForGet, error) {
	group, key := in.GetGroup(), in.GetKey()
	out := &pb.ResponseForGet{}
	log.Printf("[Geek-Cache %s] Recv RPC Request for get- (%s)/(%s)", s.self, group, key)

	if key == "" {
		return out, fmt.Errorf("key required")
	}
	g := GetGroup(group)
	if g == nil {
		return out, fmt.Errorf("group not found")
	}
	view, err := g.Get(key)
	if err != nil {
		return out, err
	}
	out.Value = view.ByteSLice()
	return out, nil
}

func (s *Server) Delete(ctx context.Context, in *pb.Request) (*pb.ResponseForDelete, error) {
	group, key := in.GetGroup(), in.GetKey()
	out := &pb.ResponseForDelete{}
	log.Printf("[Geek-Cache %s] Recv RPC Request for delete - (%s)/(%s)", s.self, group, key)

	if key == "" {
		return out, fmt.Errorf("key required")
	}
	g := GetGroup(group)
	if g == nil {
		return out, fmt.Errorf("group not found")
	}
	isSuccess, err := g.Delete(key)
	if err != nil {
		return out, err
	}
	out.Value = isSuccess
	return out, nil
}

func (s *Server) Start() error {
	s.mu.Lock()
	if s.status {
		s.mu.Unlock()
		return fmt.Errorf("server already running")
	}
	s.status = true

	port := strings.Split(s.self, ":")[1]
	l, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", port, err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterGroupCacheServer(grpcServer, s)
	// 启动 reflection 反射服务
	reflection.Register(grpcServer)

	s.mu.Unlock()
	if err := grpcServer.Serve(l); s.status && err != nil {
		return fmt.Errorf("failed to serve on %s: %v", port, err)
	}
	return nil
}
