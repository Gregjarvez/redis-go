package tcp

import (
	"bufio"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/commands"
	"github.com/codecrafters-io/redis-starter-go/app/commands/resp"
	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/store"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Server interface {
	Start()
	Stop()
}

type ExecutionResult struct {
	Results [][]byte
	Command *commands.Command
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

func (s *BaseServer) StartListener(handleConnection func(conn io.ReadWriter)) {
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

func (s *BaseServer) handleConnections(handleConnection func(conn io.ReadWriter)) {
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

func (s *BaseServer) ExecuteCommands(r io.Reader, conn net.Conn) ([]ExecutionResult, error) {
	fmt.Println("Executing commands")
	var (
		results []ExecutionResult
	)

	reader := resp.NewReader(r)
	for {
		value, _, err := reader.ReadValue()

		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Failed to read value: ", err)
			return nil, err
		}

		com, err := commands.NewCommand(value)

		if err != nil {
			fmt.Println("Failed to create command: ", err)
			return nil, err
		}

		rs, err := com.Execute(commands.DefaultHandlers, commands.RequestContext{
			Store: s.Datastore,
			Info:  &s.Info,
			Conn:  &conn,
		})

		fmt.Printf("[%s] Processed - %s \n", strings.ToUpper(string(s.Info.Role)), com.String())

		if err != nil {
			fmt.Println("Failed to execute command: ", err)
			return nil, err
		}

		results = append(results, ExecutionResult{
			Results: rs,
			Command: &com,
		})
	}

	return results, nil
}

func (s *BaseServer) WriteResults(w bufio.Writer, results [][]byte) error {
	if len(results) == 0 {
		return nil
	}

	main := results[0]

	fmt.Println("Sending result: ", strconv.Quote(string(main)))

	if _, err := w.Write(main); err != nil {
		return fmt.Errorf("write main result: %w", err)
	}

	fmt.Println("Flushing")
	if err := w.Flush(); err != nil {
		return fmt.Errorf("flush main result: %w", err)
	}

	for _, r := range results[1:] {
		fmt.Println("Sending result remaining: ", strconv.Quote(string(r)))
		if _, err := w.Write(r); err != nil {
			return fmt.Errorf("write additional result: %w", err)
		}

		if err := w.Flush(); err != nil {
			return fmt.Errorf("flush additional result: %w", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	return nil
}
