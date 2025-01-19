package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/commands"
	"github.com/codecrafters-io/redis-starter-go/app/commands/resp"
	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/store"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Server struct {
	listAddr   string
	listener   net.Listener
	shutdown   chan struct{}
	datastore  store.DataStore
	wg         sync.WaitGroup
	connection chan net.Conn
	info       config.Info
}

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

func NewTcpServer(listAddr string) (*Server, error) {
	ln, err := net.Listen("tcp", listAddr)

	if err != nil {
		return nil, err
	}
	s := store.NewMemory()
	s.Hydrate()

	return &Server{
		listAddr:   listAddr,
		listener:   ln,
		connection: make(chan net.Conn),
		shutdown:   make(chan struct{}),
		datastore:  s,
		info:       config.NewInfo(config.Config),
	}, nil
}

func (s *Server) Start() {
	s.wg.Add(2)
	go s.acceptConnections()
	go s.handleConnections()
}

func (s *Server) Stop() {
	close(s.shutdown)
	s.listener.Close()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return
	case <-time.After(time.Second):
		fmt.Println("Timed out waiting for connections to finish.")
		return
	}
}

func (s *Server) acceptConnections() {
	defer s.wg.Done()
	for {
		select {
		case <-s.shutdown:
			return
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				continue
			}
			s.connection <- conn
		}
	}
}

func (s *Server) handleConnections() {
	defer s.wg.Done()

	for {
		select {
		case <-s.shutdown:
			return
		case conn := <-s.connection:
			go s.handleConnection(conn)
		default:
			// do nothing
		}
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	var content bytes.Buffer
	buf := make([]byte, 256)

	for {
		n, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Client disconnected")
				break
			}
			fmt.Println("Read error:", err)
			return
		}

		content.Write(buf[:n])

		for {
			value, _, err := resp.NewReader(&content).ReadValue()
			if err != nil {
				break
			}

			// Process the command
			com, err := commands.NewCommand(value)
			if err != nil {
				_, werr := conn.Write([]byte(fmt.Sprintf("-ERR %v\r\n", err.Error())))
				fmt.Println("Error writing to connection:", werr)
				continue
			}

			result, execErr := com.Execute(commands.DefaultHandlers, commands.ServerContext{
				Store: s.datastore,
				Info:  s.info,
			})

			if execErr != nil {
				_, werr := conn.Write([]byte(fmt.Sprintf("-ERR %v\r\n", execErr.Error())))
				fmt.Println("Error writing to connection:", werr)
				continue
			}

			rp, marshalErr := result.Marshal()
			if marshalErr != nil {
				_, werr := conn.Write([]byte(fmt.Sprintf("-ERR %v\r\n", marshalErr.Error())))
				fmt.Println("Error writing to connection:", werr)
				continue
			}

			// Send the response
			conn.Write(rp)
			content.Reset()
		}
	}
}
