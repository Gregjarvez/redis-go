package tcp

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/commands/resp"
	"github.com/codecrafters-io/redis-starter-go/app/config"
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
	m.connnectToMaster()
}

func (m *SlaveServer) Stop() {
	m.StopListener()
}

var connectionError = errors.New("error connecting to master")

func (m *SlaveServer) connnectToMaster() {
	s := strings.Split(*config.Config.ReplicaOf, " ")
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%s", s[0], s[1]), 5*time.Second)

	if err != nil {
		panic(connectionError)
	}
	defer conn.Close()

	writer := bufio.NewWriter(conn)

	var response string

	ping := resp.ArrayValue(resp.BulkStringValue("PING"))
	r, _ := ping.Marshal()
	writer.Write(r)
	writer.Flush()

	response = readResponse(conn)

	if response != "+PONG" {
		panic(connectionError)
	}

	sendREPLCONF(conn, "listening-port", strconv.Itoa(*config.Config.Port))
	sendREPLCONF(conn, "capa", "eof", "capa", "psync2")
	sendPSYNC(conn, "?", "-1")
}

func sendREPLCONF(conn net.Conn, params ...string) {
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

	if err != nil {
		return
	}

	err = writer.Flush()
	if err != nil {
		return
	}

	r := readResponse(conn)
	if r != "+OK" {
		panic(connectionError)
	}
}

func sendPSYNC(conn net.Conn, replid, offset string) {
	writer := bufio.NewWriter(conn)
	psync := resp.ArrayValue(
		resp.BulkStringValue("PSYNC"),
		resp.BulkStringValue(replid),
		resp.BulkStringValue(offset),
	)
	response, _ := psync.Marshal()
	writer.Write(response)
	writer.Flush()

	_ = readResponse(conn)
}

func readResponse(conn net.Conn) string {
	reader := bufio.NewReader(conn)
	line, _, _ := reader.ReadLine()
	return string(line)
}
