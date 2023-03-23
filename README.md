# geek-cache

a distributed read-only cache, based [groupcache](https://github.com/golang/groupcache), using etcd as a registry, supports efficient concurrent reading.

## Install

The package supports 3 last Go versions and requires a Go version with modules support.

`go get github.com/Makonike/geek-cache`

## Usage

Be sure to install **etcd v3**(port 2379), grpcurl(for your tests), protobuf v3.

## Config

In your application, you can configure like following:

- Client

```go
registry.GlobalClientConfig = &clientv3.Config{
	Endpoints:   []string{"127.0.0.1:2379"}, // etcd address
	DialTimeout: 5 * time.Second, // the timeout for failing to establish a connection
}
```

- Picker and Consistent Hash

```go
picker := geek.NewClientPicker(addr, geek.PickerServiceName("geek-cache"), geek.ConsHashOptions(consistenthash.HashFunc(crc32.ChecksumIEEE), consistenthash.Replicas(150)))
```

## Test

Write the following code for testing.

main.go

```go
package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/Makonike/geek-cache/geek"
)

func main() {
	var port int
	flag.IntVar(&port, "port", 8001, "Geecache server port")
	flag.Parse()
	// mock database or other dataSource
	var mysql = map[string]string{
		"Tom":  "630",
		"Tom1": "631",
		"Tom2": "632",
	}
	// NewGroup create a Group which means a kind of sources
	// contain a func that used when misses cache
	g := geek.NewGroup("scores", 2<<10, geek.GetterFunc(
		func(key string) ([]byte, error) {
			log.Println("[SlowDB] search key", key)
			if v, ok := mysql[key]; ok {
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s not found", key)
		}))

	addrMap := map[int]string{
		8001: "8001",
		8002: "8002",
		8003: "8003",
	}
	var addr string = "127.0.0.1:" + addrMap[port]

	server, err := geek.NewServer(addr)
	if err != nil {
		log.Fatal(err)
	}

	addrs := make([]string, 0)
	for _, addr := range addrMap {
		addrs = append(addrs, "127.0.0.1:"+addr)
	}

	// set client address
	picker := geek.NewClientPicker(addr)
	picker.SetSimply(addrs...)
	g.RegisterPeers(picker)

	for {
		err = server.Start()
		if err != nil {
			log.Println(err.Error())
			return
		}
	}
}
```

a.sh

```shell
#!/bin/bash
trap "rm server;kill 0" EXIT

go build -o server
./server -port=8001 &
./server -port=8002 &
./server -port=8003 &

sleep 3

echo ">>> start test"
grpcurl -plaintext -d '{"group":"scores", "key": "Tom"}' 127.0.0.1:8001 pb.GroupCache/Get 
grpcurl -plaintext -d '{"group":"scores", "key": "Tom1"}' 127.0.0.1:8001 pb.GroupCache/Get 
grpcurl -plaintext -d '{"group":"scores", "key": "Tom2"}' 127.0.0.1:8001 pb.GroupCache/Get 
grpcurl -plaintext -d '{"group":"scores", "key": "Tom"}' 127.0.0.1:8002 pb.GroupCache/Get 
grpcurl -plaintext -d '{"group":"scores", "key": "Tom1"}' 127.0.0.1:8002 pb.GroupCache/Get 
grpcurl -plaintext -d '{"group":"scores", "key": "Tom2"}' 127.0.0.1:8002 pb.GroupCache/Get 
grpcurl -plaintext -d '{"group":"scores", "key": "Tom"}' 127.0.0.1:8003 pb.GroupCache/Get 
grpcurl -plaintext -d '{"group":"scores", "key": "Tom1"}' 127.0.0.1:8003 pb.GroupCache/Get 
grpcurl -plaintext -d '{"group":"scores", "key": "Tom2"}' 127.0.0.1:8003 pb.GroupCache/Get 

kill -9 `lsof -ti:8002`;

sleep 3

grpcurl -plaintext -d '{"group":"scores", "key": "Tom"}' 127.0.0.1:8001 pb.GroupCache/Get 
grpcurl -plaintext -d '{"group":"scores", "key": "Tom"}' 127.0.0.1:8003 pb.GroupCache/Get 

wait
```

Running the shell, then you can see the results following.

```bash
$ ./a.sh 
2023/02/21 10:33:04 [127.0.0.1:8002] register service success
2023/02/21 10:33:04 [127.0.0.1:8003] register service success
2023/02/21 10:33:04 [127.0.0.1:8001] register service success
>>> start test
2023/02/21 10:33:10 [Geek-Cache 127.0.0.1:8001] Recv RPC Request - (scores)/(Tom)
2023/02/21 10:33:10 [Server 127.0.0.1:8001] Pick peer 127.0.0.1:8002
2023/02/21 10:33:10 [Geek-Cache 127.0.0.1:8002] Recv RPC Request - (scores)/(Tom)
2023/02/21 10:33:10 [SlowDB] search key Tom
{
  "value": "NjMw"
}
2023/02/21 10:33:10 [Geek-Cache 127.0.0.1:8001] Recv RPC Request - (scores)/(Tom1)
2023/02/21 10:33:10 [Server 127.0.0.1:8001] Pick peer 127.0.0.1:8003
2023/02/21 10:33:10 [Geek-Cache 127.0.0.1:8003] Recv RPC Request - (scores)/(Tom1)
2023/02/21 10:33:10 [SlowDB] search key Tom1
{
  "value": "NjMx"
}
2023/02/21 10:33:10 [Geek-Cache 127.0.0.1:8001] Recv RPC Request - (scores)/(Tom2)
2023/02/21 10:33:10 [Server 127.0.0.1:8001] Pick peer 127.0.0.1:8003
2023/02/21 10:33:10 [Geek-Cache 127.0.0.1:8003] Recv RPC Request - (scores)/(Tom2)
2023/02/21 10:33:10 [SlowDB] search key Tom2
{
  "value": "NjMy"
}
2023/02/21 10:33:10 [Geek-Cache 127.0.0.1:8002] Recv RPC Request - (scores)/(Tom)
2023/02/21 10:33:10 [Geek-Cache] hit
{
  "value": "NjMw"
}
2023/02/21 10:33:10 [Geek-Cache 127.0.0.1:8002] Recv RPC Request - (scores)/(Tom1)
2023/02/21 10:33:10 [Server 127.0.0.1:8002] Pick peer 127.0.0.1:8003
2023/02/21 10:33:10 [Geek-Cache 127.0.0.1:8003] Recv RPC Request - (scores)/(Tom1)
2023/02/21 10:33:10 [Geek-Cache] hit
{
  "value": "NjMx"
}
2023/02/21 10:33:10 [Geek-Cache 127.0.0.1:8002] Recv RPC Request - (scores)/(Tom2)
2023/02/21 10:33:10 [Server 127.0.0.1:8002] Pick peer 127.0.0.1:8003
2023/02/21 10:33:10 [Geek-Cache 127.0.0.1:8003] Recv RPC Request - (scores)/(Tom2)
2023/02/21 10:33:10 [Geek-Cache] hit
{
  "value": "NjMy"
}
2023/02/21 10:33:10 [Geek-Cache 127.0.0.1:8003] Recv RPC Request - (scores)/(Tom)
2023/02/21 10:33:10 [Server 127.0.0.1:8003] Pick peer 127.0.0.1:8002
2023/02/21 10:33:10 [Geek-Cache 127.0.0.1:8002] Recv RPC Request - (scores)/(Tom)
2023/02/21 10:33:10 [Geek-Cache] hit
{
  "value": "NjMw"
}
2023/02/21 10:33:10 [Geek-Cache 127.0.0.1:8003] Recv RPC Request - (scores)/(Tom1)
2023/02/21 10:33:10 [Geek-Cache] hit
{
  "value": "NjMx"
}
2023/02/21 10:33:10 [Geek-Cache 127.0.0.1:8003] Recv RPC Request - (scores)/(Tom2)
2023/02/21 10:33:10 [Geek-Cache] hit
{
  "value": "NjMy"
}
./a.sh：行 23: 36214 已杀死               ./server -port=8002
2023/02/21 10:33:16 [Geek-Cache 127.0.0.1:8001] Recv RPC Request - (scores)/(Tom)
2023/02/21 10:33:16 [SlowDB] search key Tom
{
  "value": "NjMw"
}
2023/02/21 10:33:16 [Geek-Cache 127.0.0.1:8003] Recv RPC Request - (scores)/(Tom)
2023/02/21 10:33:16 [Server 127.0.0.1:8003] Pick peer 127.0.0.1:8001
2023/02/21 10:33:16 [Geek-Cache 127.0.0.1:8001] Recv RPC Request - (scores)/(Tom)
2023/02/21 10:33:16 [Geek-Cache] hit
{
  "value": "NjMw"
}
```

## Tech Stack

Golang+grpc+etcd

## Feature

- 使用一致性哈希解决缓存副本冗余、rehashing开销大、缓存雪崩的问题
- 使用singleFlight解决缓存击穿问题
- 使用protobuf进行节点间通信，编码报文，提高效率
- 构造虚拟节点使得请求映射负载均衡
- 使用LRU缓存淘汰算法解决资源限制的问题
- 使用etcd服务发现动态更新哈希环
- 支持并发读

## TODO List

- 添加多种缓存淘汰策略
- 支持多协议通信
