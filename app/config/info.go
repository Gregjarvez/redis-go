package config

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
)

type Role string

const (
	Master Role = "master"
	Slave  Role = "slave"
)

type Info struct {
	role             Role
	masterReplid     string
	masterReplOffset int64
}

func NewInfo(config Configuration) Info {
	var (
		role                = Slave
		masterReplicaId     = ""
		masterReplicaOffset = int64(0)
	)

	if !*config.Replica {
		role = Master
		masterReplicaId, _ = generateReplicationId()
	}

	return Info{
		role:             role,
		masterReplid:     masterReplicaId,
		masterReplOffset: masterReplicaOffset,
	}
}

func generateReplicationId() (string, error) {
	bytes := make([]byte, 40)
	_, err := rand.Read(bytes)

	if err != nil {
		return "", err
	}

	return hex.EncodeToString(bytes), nil
}

func (i Info) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("role:%s\r\n", i.role))
	sb.WriteString(fmt.Sprintf("master_replid:%s\r\n", i.masterReplid))
	sb.WriteString(fmt.Sprintf("master_repl_offset:%d\r\n", i.masterReplOffset))

	return sb.String()
}
