package config

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
)

type Role string

const (
	Master Role = "master"
	Slave  Role = "slave"
)

type Replica struct {
	Mu    sync.Mutex
	Conn  *net.Conn
	Queue chan []byte
}

type Info struct {
	Role             Role
	MasterReplid     string
	MasterReplOffset int64

	ReplicaMutex sync.RWMutex
	Replicas     map[string]*Replica
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
		info.Replicas = make(map[string]*Replica)
	}

	return info
}

func generateReplicationId() (string, error) {
	bytes := make([]byte, 20)
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

func (i *Info) IsSlave() bool {
	return i.Role == Slave
}

func (i *Info) AddReplica(conn *net.Conn) {
	if !i.IsMaster() {
		fmt.Println("Not a master, cannot add replica")
		return
	}

	i.ReplicaMutex.Lock()
	defer i.ReplicaMutex.Unlock()

	key := (*conn).RemoteAddr().String()
	replica := &Replica{Conn: conn, Queue: make(chan []byte, 100)}

	i.Replicas[key] = replica

	go func(r *Replica) {
		conn := *r.Conn

		defer i.RemoveReplica(key)
		writer := bufio.NewWriter(conn)
		for cmd := range r.Queue {
			_, err := writer.Write(cmd)
			writer.Flush()

			if err != nil {
				if err == io.EOF {
					fmt.Println("Replica disconnected:", conn.RemoteAddr())
				} else {
					fmt.Println("Error writing to replica:", err)
				}
				return
			}
		}
	}(replica)
}

func (i *Info) RemoveReplica(k string) {
	i.ReplicaMutex.Lock()
	defer i.ReplicaMutex.Unlock()

	(*i.Replicas[k].Conn).Close()
	close(i.Replicas[k].Queue)
	delete(i.Replicas, k)
}
