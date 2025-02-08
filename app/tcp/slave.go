package tcp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/commands"
	"github.com/codecrafters-io/redis-starter-go/app/commands/resp"
	"github.com/codecrafters-io/redis-starter-go/app/config"
	"io"
	"net"
	"slices"
	"strconv"
	"strings"
)

var connectionError = errors.New("error connecting to master")

var PropagatedCommand = []string{
	"SET",
	"DEL",
}

type SlaveServer struct {
	*BaseServer
}

func (ss *SlaveServer) Start() {
	ss.StartListener(ss.handleConnection)
	ss.connectToMaster()
}

func (ss *SlaveServer) Stop() {
	ss.StopListener()
}

func (ss *SlaveServer) handleConnection(conn net.Conn) {
	fmt.Println("Slave - New connection from: ", conn.RemoteAddr())
	var (
		content bytes.Buffer
	)

	for {
		buf := make([]byte, 1024)
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

		results, err := ss.ExecuteCommands(&content, &conn)

		if err != nil {
			fmt.Println("Error executing command: ", err)
			continue
		}

		c := bufio.NewWriter(conn)
		for _, exec := range results {
			result := exec.Results
			com := exec.Command

			if shouldRespondToCommand(com) {
				err = ss.WriteResults(*c, result)

				if err != nil {
					fmt.Println("Error writing results: ", err)
					continue
				}
			}
		}
	}
}

func (ss *SlaveServer) connectToMaster() {
	var conn net.Conn
	var err error

	s := strings.Split(*config.Config.ReplicaOf, " ")
	if conn, err = net.Dial("tcp", fmt.Sprintf("%s:%s", s[0], s[1])); err != nil {
		panic(connectionError)
	}

	fmt.Println("Initializing Handshake: ", conn.RemoteAddr())

	c := *bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	reader := resp.NewReader(c)
	ss.Ping(c, reader)
	ss.ReplConf(c, reader, "listening-port", strconv.Itoa(*config.Config.Port))
	ss.ReplConf(c, reader, "capa", "psync2")
	ss.Psync(c, reader)

	file, err := getRDBContent(c)

	if err != nil {
		fmt.Println("Error ignoring RDB file: ", err)
		return
	}

	ss.Datastore.Hydrate(bytes.NewReader(file))
	go ss.handleConnection(conn)
}

func (ss *SlaveServer) ReplConf(conn bufio.ReadWriter, reader *resp.Reader, params ...string) {
	args := make([]resp.Value, 0, len(params)+1)
	args = append(args, resp.BulkStringValue("REPLCONF"))

	for _, p := range params {
		args = append(args, resp.BulkStringValue(p))
	}

	c := resp.ArrayValue(
		args...,
	)

	response, _ := c.Marshal()

	if err := ss.WriteResults(*conn.Writer, [][]byte{response}); err != nil {
		fmt.Println("Error writing ping response: ", err)
		return
	}

	r, _, err := reader.ReadValue()

	if err != nil {
		panic(err)
	}

	fmt.Println("REPLCONF response: ", r.String())

	if r.String() != "OK" {
		fmt.Println("repl conf failed - invalid response")
	}
}

func (ss *SlaveServer) Psync(conn bufio.ReadWriter, reader *resp.Reader) {
	p := resp.ArrayValue(
		resp.BulkStringValue("PSYNC"),
		resp.BulkStringValue("?"),
		resp.BulkStringValue("-1"),
	)
	response, _ := p.Marshal()

	if err := ss.WriteResults(*conn.Writer, [][]byte{response}); err != nil {
		fmt.Println("Error writing PSYNC response: ", err)
		return
	}

	r, _, err := reader.ReadValue()

	if err != nil {
		panic(err)
	}

	fmt.Println("PSYNC response: ", r.String())
}

func (ss *SlaveServer) Ping(conn bufio.ReadWriter, reader *resp.Reader) {
	ping := resp.ArrayValue(
		resp.BulkStringValue("PING"),
	)
	s, _ := ping.Marshal()

	if err := ss.WriteResults(*conn.Writer, [][]byte{s}); err != nil {
		fmt.Println("Error writing ping response: ", err)
		return
	}

	r, _, err := reader.ReadValue()

	if err != nil {
		panic(err)
	}

	fmt.Println("Ping response: ", r.String())

	if r.String() != "PONG" {
		fmt.Println("Ping failed - invalid response")
	}
}

func getRDBContent(reader bufio.ReadWriter) ([]byte, error) {
	for {
		n, err := reader.Peek(1)

		if err != nil {
			return nil, fmt.Errorf("failed to peek RDB length: %v", err)
		}

		if string(n[0]) != "$" {
			continue
		}

		reader.Discard(1) // discard the bulk prefix

		l, err := reader.ReadString('\n')

		if err != nil {
			return nil, fmt.Errorf("failed to read RDB length: %v", err)
		}

		length, err := strconv.Atoi(strings.TrimSpace(l))

		if err != nil {
			return nil, fmt.Errorf("invalid RDB length: %v", err)
		}

		fmt.Println("RDB file length: ", length)
		buf := make([]byte, length)
		_, err = reader.Read(buf)

		if err != nil {
			return nil, fmt.Errorf("failed to skip RDB file: %v", err)
		}

		return buf, nil
	}
}

func shouldRespondToCommand(c *commands.Command) bool {
	return !slices.Contains(PropagatedCommand, strings.ToUpper(c.Type))
}
