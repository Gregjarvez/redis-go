package tcp

import (
	"bytes"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/commands"
	"github.com/codecrafters-io/redis-starter-go/app/commands/resp"
	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/store"
	"io"
	"net"
	"sync"
	"time"
)

type Server interface {
	Start()
	Stop()
}

type BaseServer struct {
	ListAddr   string
	Listener   net.Listener
	Shutdown   chan struct{}
	Datastore  store.DataStore
	wg         sync.WaitGroup
	Connection chan net.Conn
	Info       config.Info
}

func (s *BaseServer) StartListener() {
	s.wg.Add(2)
	go s.acceptConnections()
	go s.handleConnections()
}

func (s *BaseServer) StopListener() {
	close(s.Shutdown)
	s.Listener.Close()

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

func (s *BaseServer) acceptConnections() {
	defer s.wg.Done()
	for {
		select {
		case <-s.Shutdown:
			return
		default:
			conn, err := s.Listener.Accept()
			if err != nil {
				continue
			}
			s.Connection <- conn
		}
	}
}

func (s *BaseServer) handleConnections() {
	defer s.wg.Done()

	for {
		select {
		case <-s.Shutdown:
			return
		case conn := <-s.Connection:
			go s.handleConnection(conn)
		default:
			// do nothing
		}
	}
}

func (s *BaseServer) handleConnection(conn net.Conn) {
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
				Store: s.Datastore,
				Info:  s.Info,
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
