package tcp

import (
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/commands"
	"github.com/codecrafters-io/redis-starter-go/app/commands/resp"
	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/store"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

type Server interface {
	Start()
	Stop()
}

type BaseServer struct {
	ListAddr  string
	Listener  net.Listener
	Shutdown  chan struct{}
	Datastore store.DataStore

	wg          sync.WaitGroup
	Connections chan net.Conn

	Info config.Info

	CommandsChannel chan []byte
}

func (s *BaseServer) StartListener(handleConnection func(conn net.Conn)) {
	s.wg.Add(2)
	go s.acceptConnections()
	go s.handleConnections(handleConnection)
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
			s.Connections <- conn
		}
	}
}

func (s *BaseServer) handleConnections(handleConnection func(conn net.Conn)) {
	defer s.wg.Done()

	for {
		select {
		case <-s.Shutdown:
			return
		case conn := <-s.Connections:
			go handleConnection(conn)
		default:
			// do nothing
		}
	}
}

func (s *BaseServer) ExecuteCommand(r io.Reader, conn *net.Conn) ([][]byte, *commands.Command, int, error) {
	var (
		results [][]byte
		n       int
		com     commands.Command
		err     error
	)

	for {
		value, n, err := resp.NewReader(r).ReadValue()

		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Failed to read value: ", err)
			return nil, nil, n, err
		}

		com, err := commands.NewCommand(value)

		if err != nil {
			fmt.Println("Failed to create command: ", err)
			return nil, nil, 0, err
		}

		fmt.Printf("[%s] Processed - %s \n", strings.ToUpper(string(s.Info.Role)), com.String())

		results, err = com.Execute(commands.DefaultHandlers, commands.RequestContext{
			Store: s.Datastore,
			Info:  &s.Info,
			Conn:  conn,
		})

		if err != nil {
			fmt.Println("Failed to execute command: ", err)
			return nil, nil, n, err
		}
	}

	return results, &com, n, err
}
