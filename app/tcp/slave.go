package tcp

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/commands/resp"
	"github.com/codecrafters-io/redis-starter-go/app/config"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

type SlaveServer struct {
	*BaseServer
}

func (m *SlaveServer) Start() {
	m.StartListener()
	m.connectToMaster()
}

func (m *SlaveServer) Stop() {
	m.StopListener()
}

var connectionError = errors.New("error connecting to master")

func (m *SlaveServer) connectToMaster() {
	s := strings.Split(*config.Config.ReplicaOf, " ")
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%s", s[0], s[1]), 5*time.Second)

	if err != nil {
		panic(connectionError)
	}

	ping(conn)
	replConf(conn, "listening-port", strconv.Itoa(*config.Config.Port))
	replConf(conn, "capa", "eof", "capa", "psync2")
	psync(conn, "?", "-1")

	m.handleConnection(conn)
}

func replConf(conn net.Conn, params ...string) {
	args := make([]resp.Value, 0, len(params)+1)
	args = append(args, resp.BulkStringValue("REPLCONF"))

	for _, p := range params {
		args = append(args, resp.BulkStringValue(p))
	}

	writer := bufio.NewWriter(conn)
	repleConf := resp.ArrayValue(
		args...,
	)

	response, _ := repleConf.Marshal()
	_, err := writer.Write(response)
	writer.Flush()

	r, _, err := resp.NewReader(conn).ReadSimpleValue(resp.SimpleString)

	if err != nil {
		panic(err)
	}

	fmt.Println("REPLCONF response: ", r.String())

	if r.String() != "+OK" {
		fmt.Println("Ping failed - invalid response")
	}
}

func psync(conn net.Conn, replid, offset string) {
	writer := bufio.NewWriter(conn)
	psync := resp.ArrayValue(
		resp.BulkStringValue("PSYNC"),
		resp.BulkStringValue(replid),
		resp.BulkStringValue(offset),
	)
	response, _ := psync.Marshal()
	writer.Write(response)
	writer.Flush()

	r, _, err := resp.NewReader(conn).ReadSimpleValue(resp.SimpleString)

	if err != nil {
		panic(err)
	}

	fmt.Println("PSYNC response: ", r.String())

	ignoreRDB(conn)
}

func ping(conn net.Conn) {
	writer := bufio.NewWriter(conn)
	ping := resp.ArrayValue(
		resp.BulkStringValue("PING"),
	)
	response, _ := ping.Marshal()
	writer.Write(response)
	writer.Flush()

	r, _, err := resp.NewReader(conn).ReadSimpleValue(resp.SimpleString)

	if err != nil {
		panic(err)
	}

	fmt.Println("Ping response: ", r.String())

	if r.String() != "+PONG" {
		fmt.Println("Ping failed - invalid response")
	}
}

func ignoreRDB(conn net.Conn) error {
	reader := bufio.NewReader(conn)

	firstLine, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read RDB length: %v", err)
	}

	if !strings.HasPrefix(firstLine, "$") {
		return fmt.Errorf("unexpected response, not a bulk RDB file: %s", firstLine)
	}

	length, err := strconv.Atoi(strings.TrimPrefix(strings.TrimSpace(firstLine), "$"))

	if err != nil {
		return fmt.Errorf("invalid RDB length: %v", err)
	}

	fmt.Printf("RDB file length: %d\n", length)

	_, err = io.CopyN(io.Discard, reader, int64(length))
	if err != nil {
		return fmt.Errorf("failed to skip RDB file: %v", err)
	}

	// Read the final \r\n after the bulk string
	_, err = reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to consume final line after RDB: %v", err)
	}

	fmt.Println("RDB file ignored successfully")
	return nil
}
