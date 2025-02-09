package services

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/commands/resp"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type Role string

const (
	Master Role = "master"
	Slave  Role = "slave"
)

type Replica struct {
	Mu    sync.Mutex
	Conn  net.Conn
	Queue chan []byte
	Ack   chan bool
}

type ReplicationService struct {
	Role             Role
	MasterReplid     string
	MasterReplOffset atomic.Int64

	ReplicaMutex sync.RWMutex
	Replicas     map[string]*Replica

	ReplicaAck chan bool
}

func NewReplicationService(config Configuration) *ReplicationService {
	var (
		role                = Slave
		masterReplicaId     = ""
		masterReplicaOffset atomic.Int64
	)

	if *config.ReplicaOf == "" {
		role = Master
		masterReplicaId, _ = generateReplicationId()
	}

	replication := ReplicationService{
		Role:             role,
		MasterReplid:     masterReplicaId,
		MasterReplOffset: masterReplicaOffset,
	}

	if role == Master {
		replication.Replicas = make(map[string]*Replica)
		replication.ReplicaAck = make(chan bool, 100)
	}

	return &replication
}

func generateReplicationId() (string, error) {
	bytes := make([]byte, 20)
	_, err := rand.Read(bytes)

	if err != nil {
		return "", err
	}

	return hex.EncodeToString(bytes), nil
}

func (i *ReplicationService) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("role:%s\r\n", i.Role))
	sb.WriteString(fmt.Sprintf("master_replid:%s\r\n", i.MasterReplid))
	sb.WriteString(fmt.Sprintf("master_repl_offset:%s\r\n", strconv.FormatInt(i.MasterReplOffset.Load(), 10)))

	return sb.String()
}

func (i *ReplicationService) IsMaster() bool {
	return i.Role == Master
}

func (i *ReplicationService) IsSlave() bool {
	return i.Role == Slave
}

func (i *ReplicationService) AddReplica(conn net.Conn) {
	if !i.IsMaster() {
		fmt.Println("Not a master, cannot add replica")
		return
	}

	fmt.Println("Adding replica:", (conn).RemoteAddr())

	i.ReplicaMutex.Lock()
	defer i.ReplicaMutex.Unlock()

	// i.keepAlive(conn) test if this is required

	key := conn.RemoteAddr().String()
	replica := &Replica{Conn: conn, Queue: make(chan []byte, 100), Ack: make(chan bool)}

	i.Replicas[key] = replica

	go func(r *Replica) {
		defer i.RemoveReplica(key)

		tcpConn := r.Conn
		writer := bufio.NewWriter(tcpConn)

		for cmd := range r.Queue {
			if _, err := writer.Write(cmd); err != nil {
				if err == io.EOF {
					fmt.Println("Replica disconnected:", tcpConn.RemoteAddr())
					break
				}
				fmt.Println("Error writing to replica:", err)
				continue
			}

			if err := writer.Flush(); err != nil {
				fmt.Println("Failed to flush data to replica:", tcpConn.RemoteAddr())
			}
		}
	}(replica)
}

func (i *ReplicationService) RemoveReplica(k string) {
	i.ReplicaMutex.Lock()
	defer i.ReplicaMutex.Unlock()

	if _, ok := i.Replicas[k]; !ok {
		fmt.Println("Replica not found:", k)
		return
	}

	replica := i.Replicas[k]
	fmt.Println("Removing replica:", (replica.Conn).RemoteAddr())

	replica.Conn.Close()
	close(replica.Queue)
	close(replica.Ack)

	delete(i.Replicas, k)
}

func (i *ReplicationService) IncrementReplOffset(delta int) {
	i.MasterReplOffset.Add(int64(delta))
}

func (i *ReplicationService) GetReplOffset() int64 {
	return i.MasterReplOffset.Load()
}

func (i *ReplicationService) GetAck(conn net.Conn) error {
	v := resp.ArrayValue(
		resp.BulkStringValue("REPLCONF"),
		resp.BulkStringValue("GETACK"),
		resp.BulkStringValue("*"),
	)

	ack, _ := v.Marshal()

	fmt.Printf("Requesting ack -> %s \n", conn.RemoteAddr().String())

	if _, err := conn.Write(ack); err != nil {
		fmt.Println("Error writing ack response: ", err)
		return err
	}

	if err := utils.Flush(conn); err != nil {
		fmt.Println("Error flushing ack response: ", err)
		return err
	}

	return nil
}

func (i *ReplicationService) GetReplica(conn net.Conn) *Replica {
	return i.Replicas[conn.RemoteAddr().String()]
}
