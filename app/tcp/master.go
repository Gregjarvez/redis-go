package tcp

import (
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/config"
)

type MasterServer struct {
	*BaseServer
}

func (m *MasterServer) Start() {
	m.StartListener()
	go m.BroadCastCommands()
}

func (m *MasterServer) Stop() {
	m.StopListener()
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
				// Successfully queued
			default:
				fmt.Println("Replica write queue full:", (*r.Conn).RemoteAddr())
			}

		}(replica)
	}
}
