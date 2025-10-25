package database

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const schema = `
PRAGMA foreign_keys = ON;

-- Table de decks (clé publique textuelle)

CREATE TABLE IF NOT EXISTS Deck (
  deckId   TEXT PRIMARY KEY,
  topCardId INTEGER,
  shuffled INTEGER DEFAULT 0,
  FOREIGN KEY (topCardId) REFERENCES DeckCard(id) ON DELETE SET NULL
);

-- Table de cartes appartenant à un deck (ordre préservé par "position")
CREATE TABLE IF NOT EXISTS DeckCard (
  id       INTEGER PRIMARY KEY AUTOINCREMENT,
  deckId   TEXT NOT NULL,
  code     TEXT NOT NULL,
  nextId   INTEGER,
  FOREIGN KEY (deckId) REFERENCES Deck(deckId) ON DELETE CASCADE,
  FOREIGN KEY (nextId) REFERENCES DeckCard(id) ON DELETE SET NULL
);

-- Piles nommées dans un deck
CREATE TABLE IF NOT EXISTS Pile (
  id     INTEGER PRIMARY KEY AUTOINCREMENT,
  deckId TEXT NOT NULL,
  name   TEXT NOT NULL,
  topCardId INTEGER,
  FOREIGN KEY (deckId) REFERENCES Deck(deckId) ON DELETE CASCADE,
  FOREIGN KEY (topCardId) REFERENCES DeckCard(id) ON DELETE SET NULL,
  UNIQUE(deckId, name)                                
);
-- Cartes dans une pile (liste doublement chaînée optionnelle)
CREATE TABLE IF NOT EXISTS PileCard (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  pileId       INTEGER NOT NULL,
  code         TEXT NOT NULL,
  nextCardId   INTEGER,
  FOREIGN KEY (pileId)   REFERENCES Pile(id)      ON DELETE CASCADE,
  FOREIGN KEY (nextCardId) REFERENCES PileCard(id) ON DELETE SET NULL
);
CREATE TABLE IF NOT EXISTS DeckEntry (
  id      INTEGER PRIMARY KEY AUTOINCREMENT,
  deckId  TEXT NOT NULL,
  code    TEXT NOT NULL,
  total   INTEGER NOT NULL,
  inDeck  INTEGER NOT NULL,
  inPile  INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY (deckId) REFERENCES Deck(deckId) ON DELETE CASCADE,
  UNIQUE(deckId, code)
);

`

// NewDB crée/ouvre la base et applique le schéma.
// filepath chemin d'accès à la bd
func NewDB(filepath string) (*DBHandler, error) {
	dsn := fmt.Sprintf("%s?_foreign_keys=on&_busy_timeout=5000", filepath)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("erreur d'ouverture: %w", err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connexion échouée: %w", err)
	}
	if _, err := db.Exec(`PRAGMA journal_mode=WAL;`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("activation WAL: %w", err)
	}
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("application du schéma: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	return NewDBHandler(db), nil
}
