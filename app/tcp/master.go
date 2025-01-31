package tcp

import (
	"fmt"
	"io"
	"net"
)

type MasterServer struct {
	*BaseServer
}

func (m *MasterServer) Start() {
	m.StartListener()
	m.BroadCastCommands()
}

func (m *MasterServer) Stop() {
	m.StopListener()
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
	for k, c := range m.Info.Replicas {
		go func(k string, c *net.Conn) {
			defer func() {
				if r := recover(); r != nil {
					fmt.Println("Recovered from panic:", r)
				}
			}()

			_, err := (*c).Write(command)
			if err != nil {
				if err == io.EOF {
					m.Info.RemoveReplica(k)
				}
				fmt.Println("Error writing to replica: ", err)
			}
		}(k, c)
	}
}
