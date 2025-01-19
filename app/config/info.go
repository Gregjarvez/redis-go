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
	Role             Role
	MasterReplid     string
	MasterReplOffset int64
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

	return Info{
		Role:             role,
		MasterReplid:     masterReplicaId,
		MasterReplOffset: masterReplicaOffset,
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
	sb.WriteString(fmt.Sprintf("role:%s\r\n", i.Role))
	sb.WriteString(fmt.Sprintf("master_replid:%s\r\n", i.MasterReplid))
	sb.WriteString(fmt.Sprintf("master_repl_offset:%d\r\n", i.MasterReplOffset))

	return sb.String()
}
