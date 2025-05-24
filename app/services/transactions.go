package services

import (
	"fmt"
	"net"
	"sync"
)

type transaction struct {
	queue      []byte
	isExecuted bool
}

type TransactionService struct {
	tmu          sync.Mutex
	transactions map[net.Conn]*transaction
}

func NewTransactionService() *TransactionService {
	return &TransactionService{}
}

func (t *TransactionService) Begin(conn net.Conn) error {
	t.tmu.Lock()
	defer t.tmu.Unlock()

	if t.transactions == nil {
		t.transactions = make(map[net.Conn]*transaction)
	}

	if _, exists := t.transactions[conn]; exists {
		return fmt.Errorf("transaction already started for this connection")
	}

	t.transactions[conn] = &transaction{
		queue:      make([]byte, 0),
		isExecuted: false,
	}
	return nil
}

func (t *TransactionService) IsTransaction(conn net.Conn) bool {
	t.tmu.Lock()
	defer t.tmu.Unlock()

	if t.transactions == nil {
		return false
	}

	_, exists := t.transactions[conn]
	return exists
}
