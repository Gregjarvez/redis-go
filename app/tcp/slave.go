package tcp

import (
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/commands/resp"
	"github.com/codecrafters-io/redis-starter-go/app/config"
	"net"
	"strings"
)

type SlaveServer struct {
	*BaseServer
}

func (m *SlaveServer) Start() {
	m.StartListener()
	go m.connnectToMaster()
}

func (m *SlaveServer) Stop() {
	m.StopListener()
}

func (m *SlaveServer) connnectToMaster() {
	s := strings.Split(*config.Config.ReplicaOf, " ")
	mastrAddr := fmt.Sprintf("%s:%s", s[0], s[1])

	connection, err := net.Dial("tcp", mastrAddr)
	if err != nil {
		fmt.Println("Error connecting to master")
		return
	}

	response := resp.ArrayValue(resp.BulkStringValue("PING"))
	p, _ := response.Marshal()

	connection.Write(p)
}
