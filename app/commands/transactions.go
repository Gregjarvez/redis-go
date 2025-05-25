package commands

import (
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/commands/resp"
	"net"
	"sync"
)

type transaction struct {
	queue      []*Command
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
		queue:      make([]*Command, 0),
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

func (t *TransactionService) Commit(conn net.Conn, req RequestContext) ([]resp.Value, error) {
	t.tmu.Lock()
	defer t.tmu.Unlock()

	if transaction, exists := t.transactions[conn]; exists {
		if transaction.isExecuted {
			return nil, fmt.Errorf("transaction already committed for this connection")
		}

		response := make([]resp.Value, 0)

		handler := NewCommandRouter()

		for _, cmd := range transaction.queue {
			resp, err := handler.Handle(*cmd, req)

			if err != nil {
				return nil, fmt.Errorf("error executing command %s: %w", cmd.Type, err)
			}
			response = append(response, resp)
		}

		transaction.isExecuted = true
		delete(t.transactions, conn)

		return response, nil
	}

	return nil, fmt.Errorf("no active transaction for this connection")
}

func (t *TransactionService) AddCommand(conn net.Conn, c *Command) error {
	t.tmu.Lock()
	defer t.tmu.Unlock()

	if transaction, exists := t.transactions[conn]; exists {
		transaction.queue = append(transaction.queue, c)
		return nil
	}

	return fmt.Errorf("no active transaction for this connection")
}
