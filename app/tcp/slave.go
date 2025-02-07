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

var RespondToCommand = []string{
	"SET",
	"DEL",
}

type SlaveServer struct {
	*BaseServer
}

func (ss *SlaveServer) Start() {
	ss.StartListener(ss.handleConnection)
	go ss.connectToMaster()
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
			break
		}

		c := bufio.NewWriter(conn)
		for _, exec := range results {
			result := exec.Results
			com := exec.Command

			if ss.ShouldRespondToCommand(com) {
				for _, r := range result {
					fmt.Println("Sending result: ", strconv.Quote(string(r)))
					c.Write(r)
					c.Flush()
				}
			}
		}

		content.Reset()
	}
}

func (ss *SlaveServer) connectToMaster() {
	s := strings.Split(*config.Config.ReplicaOf, " ")
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", s[0], s[1]))
	fmt.Println("Initializing Handshake: ", conn.RemoteAddr())

	if err != nil {
		panic(connectionError)
	}

	c := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	reader := resp.NewReader(c)
	ss.Ping(c, reader)
	ss.ReplConf(c, reader, "listening-port", strconv.Itoa(*config.Config.Port))
	ss.ReplConf(c, reader, "capa", "psync2")
	ss.Psync(c, reader)

	err = ignoreRDB(c)

	if err != nil {
		fmt.Println("Error ignoring RDB file: ", err)
		return
	}

	go ss.handleConnection(conn)
}

func (ss *SlaveServer) ReplConf(conn *bufio.ReadWriter, reader *resp.Reader, params ...string) {
	args := make([]resp.Value, 0, len(params)+1)
	args = append(args, resp.BulkStringValue("REPLCONF"))

	for _, p := range params {
		args = append(args, resp.BulkStringValue(p))
	}

	c := resp.ArrayValue(
		args...,
	)

	response, _ := c.Marshal()
	_, err := conn.Write(response)
	conn.Flush()

	r, _, err := reader.ReadValue()

	if err != nil {
		panic(err)
	}

	fmt.Println("REPLCONF response: ", r.String())

	if r.String() != "OK" {
		fmt.Println("repl conf failed - invalid response")
	}
}

func (ss *SlaveServer) Psync(conn *bufio.ReadWriter, reader *resp.Reader) {
	p := resp.ArrayValue(
		resp.BulkStringValue("PSYNC"),
		resp.BulkStringValue("?"),
		resp.BulkStringValue("-1"),
	)
	response, _ := p.Marshal()
	conn.Write(response)
	conn.Flush()

	r, _, err := reader.ReadValue()

	if err != nil {
		panic(err)
	}

	fmt.Println("PSYNC response: ", r.String())
}

func (ss *SlaveServer) Ping(conn *bufio.ReadWriter, reader *resp.Reader) {
	ping := resp.ArrayValue(
		resp.BulkStringValue("PING"),
	)
	s, _ := ping.Marshal()
	conn.Write(s)
	conn.Flush()

	r, _, err := reader.ReadValue()

	if err != nil {
		panic(err)
	}

	fmt.Println("Ping response: ", r.String())

	if r.String() != "PONG" {
		fmt.Println("Ping failed - invalid response")
	}
}

func ignoreRDB(reader *bufio.ReadWriter) error {
	for {
		n, err := reader.Peek(1)

		if err != nil {
			return fmt.Errorf("failed to peek RDB length: %v", err)
		}

		if string(n[0]) != "$" {
			continue
		}

		fmt.Println("Reading RDB file length ", n)
		reader.Discard(1) // discard the bulk prefix

		l, err := reader.ReadString('\n')

		if err != nil {
			return fmt.Errorf("failed to read RDB length: %v", err)
		}

		length, err := strconv.Atoi(strings.TrimSpace(l))

		if err != nil {
			return fmt.Errorf("invalid RDB length: %v", err)
		}

		fmt.Println("RDB file length: ", length)

		_, err = io.CopyN(io.Discard, reader, int64(length))

		if err != nil {
			return fmt.Errorf("failed to skip RDB file: %v", err)
		}

		fmt.Println("RDB file ignored successfully")
		return nil
	}
}

func (s *SlaveServer) ShouldRespondToCommand(c *commands.Command) bool {
	return !slices.Contains(RespondToCommand, strings.ToUpper(c.Type))
}
