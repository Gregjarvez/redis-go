package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

type Server struct {
	wg         sync.WaitGroup
	listAddr   string
	listener   net.Listener
	shutdown   chan struct{}
	connection chan net.Conn
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	server, err := NewTcpServer("0.0.0.0:6379")

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

	return &Server{
		listAddr:   listAddr,
		listener:   ln,
		connection: make(chan net.Conn),
		shutdown:   make(chan struct{}),
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
			fmt.Println("Server shutting down...")
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
			fmt.Println("Server shutting down...")
			return
		case conn := <-s.connection:
			go s.handleConnection(conn)
		default:
			// do nothing
		}
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	_, err := conn.Write([]byte("+PONG\r\n"))

	if err != nil {
		fmt.Println(err)
	}
}
