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

func (ss *SlaveServer) handleConnection(rw io.ReadWriter) {
	var conn *net.TCPConn

	if conn, ok := rw.(*net.TCPConn); ok {
		fmt.Println("Slave - New connection from: ", conn.RemoteAddr())
	}

	var (
		content bytes.Buffer
	)

	for {
		buf := make([]byte, 1024)
		n, err := rw.Read(buf)

		if err != nil {
			if err == io.EOF {
				fmt.Println("Client disconnected")
				break
			}
			fmt.Println("Read error:", err)
			return
		}

		content.Write(buf[:n])

		results, err := ss.ExecuteCommands(&content, conn)

		if err != nil {
			fmt.Println("Error executing command: ", err)
			continue
		}

		c := bufio.NewWriter(rw)

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

	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	ss.Ping(*rw)
	ss.ReplConf(*rw, "listening-port", strconv.Itoa(*config.Config.Port))
	ss.ReplConf(*rw, "capa", "eof")
	ss.Psync(*rw)

	file, err := getRDBContent(*rw)

	if err != nil {
		fmt.Println("Error reading RDB file: ", err)
		return
	}

	if err = ss.Datastore.Hydrate(bytes.NewReader(file)); err != nil {
		fmt.Println("Error hydrating datastore: ", err)
	}

	go ss.handleConnection(rw)
}

func (ss *SlaveServer) ReplConf(rw bufio.ReadWriter, params ...string) {
	args := make([]resp.Value, 0, len(params)+1)
	args = append(args, resp.BulkStringValue("REPLCONF"))

	for _, p := range params {
		args = append(args, resp.BulkStringValue(p))
	}

	c := resp.ArrayValue(
		args...,
	)

	response, _ := c.Marshal()

	if err := ss.WriteResults(*rw.Writer, [][]byte{response}); err != nil {
		fmt.Println("Error writing ping response: ", err)
		return
	}

	r, err := rw.ReadString('\n')

	if err != nil {
		panic(err)
	}

	fmt.Println("REPLCONF response: ", r)

	if strings.TrimSpace(r) != "+OK" {
		fmt.Println("repl conf failed - invalid response")
	}
}

func (ss *SlaveServer) Psync(conn bufio.ReadWriter) {
	p := resp.ArrayValue(
		resp.BulkStringValue("PSYNC"),
		resp.BulkStringValue("?"),
		resp.BulkStringValue("-1"),
	)
	m, _ := p.Marshal()

	if err := ss.WriteResults(*conn.Writer, [][]byte{m}); err != nil {
		fmt.Println("Error writing PSYNC response: ", err)
		return
	}

	r, err := conn.ReadString('\n')

	if err != nil {
		fmt.Println("Error reading PSYNC response: ", err)
		panic(err)
	}

	fmt.Println("PSYNC response: ", r)

	if !strings.Contains(r, "FULLRESYNC") {
		fmt.Println("PSYNC failed - invalid response")
		panic("PSYNC failed - invalid response")
	}
}

func (ss *SlaveServer) Ping(rw bufio.ReadWriter) {
	ping := resp.ArrayValue(
		resp.BulkStringValue("PING"),
	)
	s, _ := ping.Marshal()

	if err := ss.WriteResults(*rw.Writer, [][]byte{s}); err != nil {
		fmt.Println("Error writing ping response: ", err)
		return
	}

	r, err := rw.ReadString('\n')

	if err != nil {
		panic(err)
	}

	fmt.Println("Ping response: ", r)

	if strings.TrimSpace(r) != "+PONG" {
		fmt.Println("Ping failed - invalid response")
	}
}

func getRDBContent(rw bufio.ReadWriter) ([]byte, error) {
	fmt.Println("Reading RDB file")

	prefix, err := rw.ReadByte()

	if err != nil {
		return nil, fmt.Errorf("failed to peek RDB length: %v", err)
	}

	if string(prefix) != "$" {
		return nil, fmt.Errorf("expected $ prefix, got %c", prefix)
	}

	l, err := rw.ReadString('\n')

	if err != nil {
		return nil, fmt.Errorf("failed to read RDB length: %v", err)
	}

	length, err := strconv.Atoi(strings.TrimSpace(l))

	if err != nil {
		return nil, fmt.Errorf("invalid RDB length: %v", err)
	}

	fmt.Println("RDB file length: ", length)
	buf := make([]byte, length)
	_, err = io.ReadFull(rw, buf)

	if err != nil {
		return nil, fmt.Errorf("failed to skip RDB file: %v", err)
	}

	return buf, nil
}

func shouldRespondToCommand(c *commands.Command) bool {
	return !slices.Contains(PropagatedCommand, strings.ToUpper(c.Type))
}
