package tcp

import (
	"bufio"
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

func (m *SlaveServer) connnectToMaster() {
	s := strings.Split(*config.Config.ReplicaOf, " ")
	mastrAddr := fmt.Sprintf("%s:%s", s[0], s[1])

	conn, err := net.DialTimeout("tcp", mastrAddr, 5*time.Second)

	if err != nil {
		fmt.Println("Error connecting to master")
		return
	}
	defer conn.Close()

	writer := bufio.NewWriter(conn)

	var response string

	ping := resp.ArrayValue(resp.BulkStringValue("PING"))
	r, _ := ping.Marshal()
	writer.Write(r)
	writer.Flush()

	response = readResponse(conn)

	println(response)
	if response != "+PONG" {
		panic("Error connecting to master")
	}

	sendREPLCONF(writer, "listening-port", strconv.Itoa(*config.Config.Port))
	response = readResponse(conn)
	if response != "+OK" {
		panic("Error connecting to master")
	}

	sendREPLCONF(writer, "capa", "psync2")
	response = readResponse(conn)
	if response != "+OK" {
		panic("Error connecting to master")
	}
}

func sendREPLCONF(conn *bufio.Writer, key, value string) {
	repleConf := resp.ArrayValue(
		resp.BulkStringValue("REPLCONF"),
		resp.BulkStringValue(key),
		resp.BulkStringValue(value),
	)
	response, _ := repleConf.Marshal()
	conn.Write(response)
	conn.Flush()
}

func readResponse(conn net.Conn) string {
	reader := bufio.NewReader(conn)
	line, _, _ := reader.ReadLine()
	return string(line)
}
