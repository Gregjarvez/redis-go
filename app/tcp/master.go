package tcp

import (
	"bytes"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/commands/resp"
	"github.com/codecrafters-io/redis-starter-go/app/config"
	"io"
	"net"
	"strings"
	"time"
)

type MasterServer struct {
	*BaseServer
}

func (m *MasterServer) Start() {
	m.StartListener(m.handleConnection)
	go m.BroadCastCommands()
}

func (m *MasterServer) Stop() {
	m.StopListener()
}

func (m *MasterServer) handleConnection(rw io.ReadWriter) {
	var (
		content             bytes.Buffer
		isReplicaConnection bool
	)

	if conn, ok := rw.(*net.TCPConn); ok {
		fmt.Println("Slave - New connection from: ", conn.RemoteAddr())
	}

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

		// Process the command
		results, err := m.ExecuteCommands(&content)

		if err != nil {
			fmt.Println("Error executing command: ", err)
			continue
		}

		for _, exec := range results {
			result := exec.Results
			com := exec.Command

			if strings.ToUpper(com.Type) == "PSYNC" {
				isReplicaConnection = true
			}

			err = m.WriteResults(rw, result)

			if err != nil {
				fmt.Println("Error writing results: ", err)
				continue
			}

			if com.Propagate {
				fmt.Println("Propagating command to replicas ", com.String())
				m.CommandsChannel <- com.Raw
			}
		}

		content.Reset()
		// If it's a replica connection, exit the loop
		//if isReplicaConnection {
		//	//go m.Ack(conn)
		//	break
		//}
	}
}

func (m *MasterServer) BroadCastCommands() {
	fmt.Println("Broadcasting commands")
	for {
		select {
		case command := <-m.CommandsChannel:
			m.Broadcast(command)
		case <-m.Shutdown:
			return
		}
	}
}

func (m *MasterServer) Broadcast(command []byte) {
	m.Info.ReplicaMutex.RLock()
	defer m.Info.ReplicaMutex.RUnlock()

	for _, replica := range m.Info.Replicas {
		fmt.Println("Broadcasting command to:", (*replica.Conn).RemoteAddr())

		go func(r *config.Replica) {
			defer func() {
				if r := recover(); r != nil {
					fmt.Println("Recovered from panic:", r)
				}
			}()

			select {
			case r.Queue <- command:
			default:
				fmt.Println("Replica write queue full:", (*r.Conn).RemoteAddr())
			}

		}(replica)
	}
}

func (m *MasterServer) Ack(conn net.Conn) {
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if err := tcpConn.SetKeepAlive(true); err != nil {
			fmt.Println("Error setting keep alive: ", err)
		}

		if err := tcpConn.SetKeepAlivePeriod(3 * time.Second); err != nil {
			fmt.Println("Error setting keep alive period: ", err)
		}
	}

	fmt.Println("ACK")
	v := resp.ArrayValue(
		resp.BulkStringValue("REPLCONF"),
		resp.BulkStringValue("GETACK"),
		resp.BulkStringValue("*"),
	)
	ack, _ := v.Marshal()

	if err := m.WriteResults(conn, [][]byte{ack}); err != nil {
		fmt.Println("Error writing ack response: ", err)
		return
	}
}
