package main

import (
	"flag"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/services"
	"github.com/codecrafters-io/redis-starter-go/app/store"
	"github.com/codecrafters-io/redis-starter-go/app/tcp"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Starting server...")
	flag.Parse()

	addr := fmt.Sprintf("%s:%v", *services.Config.Host, *services.Config.Port)
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
	replication := services.NewReplicationService(services.Config)

	if err != nil {
		return nil, err
	}
	s := store.NewMemory()

	var (
		dir        = *services.Config.Dir
		dbFilename = *services.Config.DbFilename
	)

	if dir != "" && dbFilename != "" {
		dumpFile := filepath.Join(dir, dbFilename)
		_, err := os.Stat(dumpFile)

		if err == nil || !os.IsNotExist(err) {
			fmt.Println("Hydrating memory store from dumpFile file")
			f, err := os.Open(dumpFile)

			if err == nil {
				defer f.Close()
				s.Hydrate(f)
			}
		}
	}

	baseServer := &tcp.BaseServer{
		ListAddr:    listAddr,
		Listener:    ln,
		Connections: make(chan net.Conn),
		Shutdown:    make(chan struct{}),
		Datastore:   s,
		Replication: replication,

		Transactions: services.NewTransactionService(),
	}

	if replication.IsMaster() {
		baseServer.CommandsChannel = make(chan []byte, 100)
	}

	switch replication.Role {
	case services.Master:
		return &tcp.MasterServer{
			BaseServer: baseServer,
		}, nil
	case services.Slave:
		return &tcp.SlaveServer{
			BaseServer: baseServer,
		}, nil
	default:
		return nil, fmt.Errorf("unknown role: %s", replication.Role)
	}
}
