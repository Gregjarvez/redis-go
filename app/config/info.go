package config

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"strings"
	"sync"
)

type Role string

const (
	Master Role = "master"
	Slave  Role = "slave"
)

type Info struct {
	Role             Role
	MasterReplid     string
	MasterReplOffset int64
	Replicas         map[string]*net.Conn
}

func NewInfo(config Configuration) Info {
	var (
		role                = Slave
		masterReplicaId     = ""
		masterReplicaOffset = int64(0)
	)

	if *config.ReplicaOf == "" {
		role = Master
		masterReplicaId, _ = generateReplicationId()
	}

	info := Info{
		Role:             role,
		MasterReplid:     masterReplicaId,
		MasterReplOffset: masterReplicaOffset,
	}

	if role == Master {
		info.Replicas = make(map[string]*net.Conn)
	}

	return info
}

func generateReplicationId() (string, error) {
	bytes := make([]byte, 40)
	_, err := rand.Read(bytes)

	if err != nil {
		return "", err
	}

	return hex.EncodeToString(bytes), nil
}

func (i *Info) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("role:%s\r\n", i.Role))
	sb.WriteString(fmt.Sprintf("master_replid:%s\r\n", i.MasterReplid))
	sb.WriteString(fmt.Sprintf("master_repl_offset:%d\r\n", i.MasterReplOffset))

	return sb.String()
}

func (i *Info) IsMaster() bool {
	return i.Role == Master
}

func (i *Info) AddReplica(conn *net.Conn) {
	if !i.IsMaster() {
		return
	}

	i.Replicas[(*conn).RemoteAddr().String()] = conn
}

func (i *Info) RemoveReplica(k string) {
	var mu sync.Mutex
	(*i.Replicas[k]).Close()
	mu.Lock()

	delete(i.Replicas, k)
	mu.Unlock()

}
