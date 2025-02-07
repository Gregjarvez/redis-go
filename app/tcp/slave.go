package tcp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/commands/resp"
	"github.com/codecrafters-io/redis-starter-go/app/config"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

var connectionError = errors.New("error connecting to master")

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
		fmt.Printf("[%s] Received data: - %s \n", strings.ToUpper(string(ss.Info.Role)), string(buf))

		results, comm, _, execErr := ss.ExecuteCommand(&content, &conn)

		if execErr != nil {
			fmt.Println("Error executing command: ", execErr)
			break
		}

		// we should not respond to a propagated command.  rename this field to better communicate intent
		if !comm.Propagatable {
			c := bufio.NewWriter(conn)
			for _, result := range results {
				fmt.Println("\n Sending result: ", string(result))
				c.Write(result)
				c.Flush()
			}
		}

		content.Reset()
	}
}

func (ss *SlaveServer) connectToMaster() {
	s := strings.Split(*config.Config.ReplicaOf, " ")
	fmt.Println("Connecting to master: ", s[0], ":", s[1])
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", s[0], s[1]))

	if err != nil {
		panic(connectionError)
	}

	c := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	ss.Ping(c)
	ss.ReplConf(c, "listening-port", strconv.Itoa(*config.Config.Port))
	ss.ReplConf(c, "capa", "psync2")
	ss.Psync(c, "?", "-1")

	ignoreRDB(c)
	ss.Connections <- conn
}

func (ss *SlaveServer) ReplConf(conn *bufio.ReadWriter, params ...string) {
	args := make([]resp.Value, 0, len(params)+1)
	args = append(args, resp.BulkStringValue("REPLCONF"))

	for _, p := range params {
		args = append(args, resp.BulkStringValue(p))
	}

	repleConf := resp.ArrayValue(
		args...,
	)

	response, _ := repleConf.Marshal()
	_, err := conn.Write(response)
	conn.Flush()

	r, _, err := resp.NewReader(conn).ReadSimpleValue(resp.SimpleString)

	if err != nil {
		panic(err)
	}

	v, err := r.Marshal()
	fmt.Println("REPLCONF response: ", string(v))

	if r.String() != "OK" {
		fmt.Println("Ping failed - invalid response")
	}
}

func (ss *SlaveServer) Psync(conn *bufio.ReadWriter, replid, offset string) {
	psync := resp.ArrayValue(
		resp.BulkStringValue("PSYNC"),
		resp.BulkStringValue(replid),
		resp.BulkStringValue(offset),
	)
	response, _ := psync.Marshal()
	conn.Write(response)
	conn.Flush()

	r, _, err := resp.NewReader(conn).ReadSimpleValue(resp.SimpleString)

	if err != nil {
		panic(err)
	}

	fmt.Println("PSYNC response: ", r.String())
}

func (ss *SlaveServer) Ping(conn *bufio.ReadWriter) {
	ping := resp.ArrayValue(
		resp.BulkStringValue("PING"),
	)
	s, _ := ping.Marshal()
	conn.Write(s)
	conn.Flush()

	r, _, err := resp.NewReader(conn).ReadSimpleValue(resp.SimpleString)

	if err != nil {
		panic(err)
	}

	fmt.Println("Ping response: ", string(r.Raw))

	if r.String() != "+PONG" {
		fmt.Println("Ping failed - invalid response")
	}
}

func ignoreRDB(reader *bufio.ReadWriter) error {
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

	_, err = io.CopyN(os.Stdout, reader, int64(length))

	if err != nil {
		return fmt.Errorf("failed to skip RDB file: %v", err)
	}

	fmt.Println("RDB file ignored successfully")
	return nil
}
