package database

import (
	"deckofcards/utils"
	"fmt"
)

const (
	WRITE = iota
	READ
)

// / Reponse d'une operation de base de donnees
type DBResponse struct {
	Data interface{}
	Err  error
}

// / Methode a executer dans un worker
type WorkerOperation func() DBResponse

// / Operation de base de donnees
type DBOperation struct {
	response  chan DBResponse
	operation WorkerOperation
}

// / Pool de worker
type WorkerPool struct {
	operations chan DBOperation
	handler    *DBHandler
}

func (w *WorkerPool) Execute(op WorkerOperation) DBResponse {
	resp := make(chan DBResponse, 1)
	w.operations <- DBOperation{
		response:  resp,
		operation: op,
	}
	return <-resp
}

func Init(db *DBHandler) *WorkerPool {
	w := &WorkerPool{
		operations: make(chan DBOperation),
		handler:    db,
	}
	for i := 0; i < utils.WORKER_AMOUNT; i++ {
		go func() {
			for operation := range w.operations {
				func() {
					defer func() {
						if r := recover(); r != nil {
							operation.response <- DBResponse{
								Err: fmt.Errorf("panic: %v", r),
							}
						}
					}()
					operation.response <- operation.operation()
				}()
			}
		}()
	}
	return w
}

func (w *WorkerPool) Close() {
	close(w.operations)
}
