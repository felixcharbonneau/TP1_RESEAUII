package database

import (
	"database/sql"
	"sync"
)

type DBHandler struct {
	db   *sql.DB
	data map[string]interface{}
	mu   sync.RWMutex
}

// / NewDBHandler generer un handler de base de donnees
func NewDBHandler(db *sql.DB) *DBHandler {
	return &DBHandler{
		db:   db,
		data: make(map[string]interface{}),
	}
}

// / Lock barre la base de donnees en ecriture
func (h *DBHandler) Lock() {
	h.mu.Lock()
}

// / RLock debare la base de donnees en lecture
func (h *DBHandler) RLock() {
	h.mu.RLock()
}

// / UnLock debare la base de donnees en ecriture
func (h *DBHandler) UnLock() {
	h.mu.Unlock()
}

// / RUnLock debare la base de donnees en lecture
func (h *DBHandler) RUnLock() {
	h.mu.RUnlock()
}
