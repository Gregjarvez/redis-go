package main

import (
	"flag"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/store"
	"github.com/codecrafters-io/redis-starter-go/app/tcp"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Starting server...")
	flag.Parse()

	addr := fmt.Sprintf("%s:%v", *config.Config.Host, *config.Config.Port)
	server, err := NewTcpServer(addr)

	if err != nil {
		log.Fatal(err)
	}

	server.Start()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan

	fmt.Println("Shutting down server...")
	server.Stop()
	fmt.Println("Server stopped.")

}

func NewTcpServer(listAddr string) (tcp.Server, error) {
	ln, err := net.Listen("tcp", listAddr)
	info := config.NewInfo(config.Config)

	if err != nil {
		return nil, err
	}
	s := store.NewMemory()
	s.Hydrate()

	baseServer := &tcp.BaseServer{
		ListAddr:   listAddr,
		Listener:   ln,
		Connection: make(chan net.Conn),
		Shutdown:   make(chan struct{}),
		Datastore:  s,
		Info:       info,
	}

	switch info.Role {
	case config.Master:
		return &tcp.MasterServer{
			BaseServer: baseServer,
		}, nil
	case config.Slave:
		return &tcp.SlaveServer{
			BaseServer: baseServer,
		}, nil
	default:
		return nil, fmt.Errorf("unknown role: %s", info.Role)
	}
}
