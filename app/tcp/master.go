package tcp

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/config"
	"io"
	"net"
	"strings"
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

func (m *MasterServer) handleConnection(conn net.Conn) {
	var (
		content             bytes.Buffer
		isReplicaConnection bool
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

		// Process the command
		results, err := m.ExecuteCommands(&content, &conn)

		if err != nil {
			fmt.Println("Error executing command: ", err)
			continue
		}

		c := bufio.NewWriter(conn)
		fmt.Println("Writing results", results)
		for _, exec := range results {
			result := exec.Results
			com := exec.Command

			if strings.ToUpper(com.Type) == "PSYNC" {
				isReplicaConnection = true
			}

			err = m.WriteResults(*c, result)

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
		if isReplicaConnection {
			break
		}
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
