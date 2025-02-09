package tcp

import (
	"bytes"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/commands/resp"
	"github.com/codecrafters-io/redis-starter-go/app/services"
	"io"
	"net"
	"strings"
)

type MasterServer struct {
	*BaseServer
}

func (m *MasterServer) Start() {
	m.StartListener(m.handleConnection)
	m.BroadCastCommands()
}

func (m *MasterServer) Stop() {
	m.StopListener()
}

func (m *MasterServer) handleConnection(rw io.ReadWriter) {
	var (
		content             bytes.Buffer
		isReplicaConnection bool
	)
	var conn net.Conn

	if connection, ok := rw.(*net.TCPConn); ok {
		fmt.Println("Master - New connection from: ", connection.RemoteAddr())
		conn = connection
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
				m.Replication.AddReplica(conn, &m.CommandsChannel)
				isReplicaConnection = true
			}

			err = m.WriteResults(rw, result)

			if err != nil {
				fmt.Println("Error writing results: ", err)
				continue
			}

			if com.Propagate {
				fmt.Println("Propagating to replicas ", com.String())
				m.CommandsChannel <- com.Raw
			}
		}

		content.Reset()
		// If it's a replica connection, exit the loop
		if isReplicaConnection {
			fmt.Println("Replica connection detected, exiting read loop")
			break
		}
	}
}

func (m *MasterServer) BroadCastCommands() {
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
	m.Replication.ReplicaMutex.RLock()
	defer m.Replication.ReplicaMutex.RUnlock()

	for _, replica := range m.Replication.Replicas {
		go func(r *services.Replica) {
			defer func() {
				if r := recover(); r != nil {
					fmt.Println("Recovered from panic:", r)
				}
			}()

			select {
			case r.Queue <- command:
				fmt.Println("Command sent to replica:", r.Conn.RemoteAddr())
			default:
				fmt.Println("Replica write queue full:", r.Conn.RemoteAddr())
			}

		}(replica)
	}
}

func (m *MasterServer) Ack(conn net.Conn) {
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
