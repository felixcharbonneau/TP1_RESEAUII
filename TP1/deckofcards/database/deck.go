package database

import (
	"crypto/rand"
	"database/sql"
	"deckofcards/models"
	"errors"
	"fmt"
	mathrand "math/rand"
	"time"
)

const base62 = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randomBase62(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("rand.Read: %w", err)
	}
	out := make([]byte, n)
	for i := range b {
		out[i] = base62[int(b[i])%len(base62)]
	}
	return string(out), nil
}

// InsertDeck Insert un deck
func (w *WorkerPool) InsertDeck(deck *models.Deck) (string, error) {
	resp := w.Execute(func() DBResponse {
		db := w.handler.db
		w.handler.Lock()
		defer w.handler.UnLock()
		var deckToken string
		for tries := 0; tries < 30; tries++ {
			id, err := randomBase62(12)
			if err != nil {
				return DBResponse{Err: fmt.Errorf("randomBase62: %w", err)}
			}

			var count int

			row := db.QueryRow(`SELECT COUNT(deckId) FROM Deck WHERE deckId = ?`, id)
			if err := row.Scan(&count); err != nil {
				return DBResponse{Err: fmt.Errorf("Echec de lecture de resultat de requete: %w", err)}
			}
			if count == 0 {
				deckToken = id
				break
			}
		}
		if deckToken == "" {
			return DBResponse{Err: fmt.Errorf("Impossible de generer un id unique pour le deck")}
		}

		tx, err := db.Begin()
		if err != nil {
			return DBResponse{Err: fmt.Errorf("Echec de demarrage de transaction: %w", err)}
		}
		defer func() {
			if err != nil {
				_ = tx.Rollback()
			}
		}()

		cardIDs := make([]int64, len(deck.Cards))
		cardCounts := make(map[string]int)

		if _, err := tx.Exec(`INSERT INTO Deck(deckId, topCardId) VALUES (?, NULL)`, deckToken); err != nil {
			return DBResponse{Err: fmt.Errorf("échec d'insertion du deck: %w", err)}
		}

		for i, cardCode := range deck.Cards {
			res, err := tx.Exec(`INSERT INTO DeckCard (deckId, code, nextId) VALUES (?, ?, NULL)`, deckToken, cardCode)
			if err != nil {
				return DBResponse{Err: fmt.Errorf("Echec d'insertion de carte: %w", err)}
			}
			cardID, err := res.LastInsertId()
			if err != nil {
				return DBResponse{Err: fmt.Errorf("Echec de lecture de LastInsertId: %w", err)}
			}
			cardIDs[i] = cardID
			cardCounts[cardCode]++
		}

		for i := 0; i < len(cardIDs)-1; i++ {
			if _, err := tx.Exec(`UPDATE DeckCard SET nextId = ? WHERE id = ?`, cardIDs[i+1], cardIDs[i]); err != nil {
				return DBResponse{Err: fmt.Errorf("Echec de mise a jour de nextId: %w", err)}
			}
		}

		if len(cardIDs) > 0 {
			if _, err := tx.Exec(`UPDATE Deck SET topCardId = ? WHERE deckId = ?`, cardIDs[0], deckToken); err != nil {
				return DBResponse{Err: fmt.Errorf("echec de mise a jour du topCardId: %w", err)}
			}
		}

		for code, count := range cardCounts {
			if _, err := tx.Exec(`INSERT INTO DeckEntry (deckId, code, total, inDeck) VALUES (?, ?, ?, ?) ON CONFLICT(deckId, code) DO UPDATE SET total = excluded.total, inDeck = excluded.inDeck`,
				deckToken, code, count, count); err != nil {
				return DBResponse{Err: fmt.Errorf("echec d'insertion de DeckEntry: %w", err)}
			}
		}

		if err := tx.Commit(); err != nil {
			return DBResponse{Err: fmt.Errorf("echec de commit: %w", err)}
		}

		return DBResponse{Data: deckToken}
	})

	if resp.Err != nil {
		return "", resp.Err
	}
	token, ok := resp.Data.(string)
	if !ok {
		return "", fmt.Errorf("internal: expected string deck token")
	}
	return token, nil
}

// InsertIntoPile Rajoute des cartes dans une pile, si la pile n'existe pas elle est creee
func (w *WorkerPool) InsertIntoPile(name string, deckId string, codes []string) (models.Deck, error) {
	resp := w.Execute(func() DBResponse {
		db := w.handler.db
		w.handler.Lock()
		defer w.handler.UnLock()

		tx, err := db.Begin()
		if err != nil {
			return DBResponse{Err: fmt.Errorf("echec de demarrage de transaction: %w", err)}
		}
		defer func() {
			if err != nil {
				_ = tx.Rollback()
			}
		}()

		for i := 0; i < len(codes); i++ {
			var total, inDeck, inPile int64
			row := tx.QueryRow(`SELECT total, inDeck, inPile FROM DeckEntry WHERE deckId = ? AND code = ?`, deckId, codes[i])
			if err := row.Scan(&total, &inDeck, &inPile); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return DBResponse{Err: fmt.Errorf("carte non trouvee dans le deck")}
				}
				return DBResponse{Err: fmt.Errorf("echec de lecture: %w", err)}
			}

			if total < inDeck+inPile+1 {
				return DBResponse{Err: fmt.Errorf("carte non pigée, non présente dans le deck, ou déjà dans les piles")}
			}

			if _, err := tx.Exec(`UPDATE DeckEntry SET inPile = inPile + 1 WHERE deckId = ? AND code = ?`, deckId, codes[i]); err != nil {
				return DBResponse{Err: fmt.Errorf("echec de mise a jour de DeckEntry: %w", err)}
			}
		}

		var pileId int64
		row := tx.QueryRow(`SELECT id FROM Pile WHERE deckId = ? AND name = ?`, deckId, name)
		if err := row.Scan(&pileId); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				result, err := tx.Exec(`INSERT INTO Pile (deckId, name) VALUES (?, ?)`, deckId, name)
				if err != nil {
					return DBResponse{Err: fmt.Errorf("failed to insert Pile: %w", err)}
				}
				pileId, err = result.LastInsertId()
				if err != nil {
					return DBResponse{Err: fmt.Errorf("failed to get inserted ID: %w", err)}
				}
			} else {
				return DBResponse{Err: fmt.Errorf("echec de lecture: %w", err)}
			}
		}
		var topCardId *int64
		row = tx.QueryRow(`SELECT id FROM PileCard WHERE pileId = ? AND id NOT IN (SELECT nextCardId FROM PileCard WHERE nextCardId IS NOT NULL)`, pileId)
		if err := row.Scan(&topCardId); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				topCardId = nil
			} else {
				return DBResponse{Err: fmt.Errorf("echec lecture carte du dessus de la pile: %w", err)}
			}
		}

		for _, code := range codes {
			result, err := tx.Exec(`INSERT INTO PileCard (pileId, code, nextCardId) VALUES (?, ?, ?)`, pileId, code, topCardId)
			if err != nil {
				return DBResponse{Err: fmt.Errorf("echec d'insertion de carte de pile: %w", err)}
			}
			lastId, err := result.LastInsertId()
			if err != nil {
				return DBResponse{Err: fmt.Errorf("echec d'insertion de carte de pile: %w", err)}
			}
			topCardId = &lastId
		}

		if err := tx.Commit(); err != nil {
			return DBResponse{Err: fmt.Errorf("echec de commit: %w", err)}
		}
		return DBResponse{}
	})
	if resp.Err != nil {
		return models.Deck{}, resp.Err
	}

	remaining, err := w.CardsInDeck(deckId)
	if err != nil {
		return models.Deck{}, err
	}
	pileRemaining, err := w.CardsInPile(deckId, name)
	if err != nil {
		return models.Deck{}, err
	}
	piles := make(map[string]*models.Pile)
	piles[name] = &models.Pile{
		Remaining: int(pileRemaining),
		Cards:     nil,
	}

	return models.Deck{
		Remaining: int(remaining),
		Cards:     nil,
		Piles:     piles,
		Id:        deckId,
		NPackets:  0,
	}, nil
}

// GetPileCards Optient les cartes d'une pile
func (w *WorkerPool) GetPileCards(deckId, pileName string) ([]string, int, error) {
	resp := w.Execute(func() DBResponse {
		w.handler.RLock()
		defer w.handler.RUnLock()

		// First, get the pile ID
		var pileId int64
		if err := w.handler.db.QueryRow(`SELECT id FROM Pile WHERE deckId = ? AND name = ?`, deckId, pileName).Scan(&pileId); err != nil {
			return DBResponse{Err: fmt.Errorf("pile not found: %w", err)}
		}

		// Get all cards with their next pointers
		rows, err := w.handler.db.Query(`
			SELECT id, code, nextCardId 
			FROM PileCard 
			WHERE pileId = ?
		`, pileId)
		if err != nil {
			return DBResponse{Err: err}
		}
		defer rows.Close()

		// Build a map of cards
		type cardNode struct {
			id     int64
			code   string
			nextId sql.NullInt64
		}
		cards := make(map[int64]cardNode)
		var topId int64 = -1

		for rows.Next() {
			var node cardNode
			if err := rows.Scan(&node.id, &node.code, &node.nextId); err != nil {
				return DBResponse{Err: err}
			}
			cards[node.id] = node
		}

		if len(cards) == 0 {
			return DBResponse{Data: []string{}}
		}

		// Find the top card (the one not referenced by any nextCardId)
		referenced := make(map[int64]bool)
		for _, card := range cards {
			if card.nextId.Valid {
				referenced[card.nextId.Int64] = true
			}
		}

		for id := range cards {
			if !referenced[id] {
				topId = id
				break
			}
		}

		if topId == -1 {
			return DBResponse{Err: fmt.Errorf("pile has no top card (circular reference?)")}
		}

		// Follow the linked list from top to bottom
		var codes []string
		currentId := topId
		visited := make(map[int64]bool)

		for {
			if visited[currentId] {
				return DBResponse{Err: fmt.Errorf("circular reference detected in pile")}
			}
			visited[currentId] = true

			card, exists := cards[currentId]
			if !exists {
				return DBResponse{Err: fmt.Errorf("broken linked list: card %d not found", currentId)}
			}

			codes = append(codes, card.code)

			if !card.nextId.Valid {
				// Reached the end
				break
			}

			currentId = card.nextId.Int64
		}

		return DBResponse{Data: codes}
	})

	if resp.Err != nil {
		return nil, 0, resp.Err
	}
	codes := resp.Data.([]string)
	return codes, len(codes), nil
}

// / CardsInDeck optiens les cartes d'un deck
func (w *WorkerPool) CardsInDeck(deckId string) (uint64, error) {
	resp := w.Execute(func() DBResponse {
		db := w.handler.db
		w.handler.RLock()
		defer w.handler.RUnLock()
		result := db.QueryRow(
			`SELECT COUNT(*) FROM Deck INNER JOIN DeckCard ON Deck.deckId = DeckCard.deckId WHERE Deck.deckId = ?`, deckId,
		)
		remaining := int64(0)
		err := result.Scan(&remaining)
		if err != nil {
			return DBResponse{Err: fmt.Errorf("echec de lecture des decks: %w", err)}
		}
		return DBResponse{Data: remaining}
	})
	if resp.Err != nil {
		return 0, resp.Err
	}
	return uint64(resp.Data.(int64)), nil
}

// / CardsInPile optiens les cartes d'une pile
func (w *WorkerPool) CardsInPile(deckId string, pileName string) (uint64, error) {
	resp := w.Execute(func() DBResponse {
		db := w.handler.db
		w.handler.RLock()
		defer w.handler.RUnLock()
		result := db.QueryRow(
			`SELECT COUNT(*) FROM Pile INNER JOIN PileCard ON Pile.id = PileCard.pileId WHERE Pile.deckId = ? AND Pile.name =?`, deckId, pileName,
		)
		remaining := int64(0)
		err := result.Scan(&remaining)
		if err != nil {
			return DBResponse{Err: fmt.Errorf("echec de lecture des decks: %w", err)}
		}
		return DBResponse{Data: remaining}
	})
	if resp.Err != nil {
		return 0, resp.Err
	}
	return uint64(resp.Data.(int64)), nil
}

// / ListPiles liste les piles pour un deck
func (w *WorkerPool) ListPiles(deckId string) (map[string]int, error) {
	resp := w.Execute(func() DBResponse {
		w.handler.RLock()
		defer w.handler.RUnLock()

		rows, err := w.handler.db.Query(`
			SELECT name, COUNT(PileCard.id) 
			FROM Pile
			LEFT JOIN PileCard ON Pile.id = PileCard.pileId
			WHERE deckId = ?
			GROUP BY name
		`, deckId)
		if err != nil {
			return DBResponse{Err: err}
		}
		defer rows.Close()

		piles := make(map[string]int)
		for rows.Next() {
			var name string
			var count int
			if err := rows.Scan(&name, &count); err != nil {
				return DBResponse{Err: err}
			}
			piles[name] = count
		}
		return DBResponse{Data: piles}
	})

	if resp.Err != nil {
		return nil, resp.Err
	}
	return resp.Data.(map[string]int), nil
}
func (w *WorkerPool) ShuffleAllPiles(deckId string) (map[string]int, error) {
	results := make(map[string]int)

	resp := w.Execute(func() DBResponse {
		db := w.handler.db
		w.handler.Lock()
		defer w.handler.UnLock()

		// Start transaction for atomic operations
		tx, err := db.Begin()
		if err != nil {
			return DBResponse{Err: err}
		}
		defer func() {
			if err != nil {
				_ = tx.Rollback()
			}
		}()

		rows, err := tx.Query(`SELECT id, name FROM Pile WHERE deckId = ?`, deckId)
		if err != nil {
			return DBResponse{Err: err}
		}
		defer rows.Close()

		type pileInfo struct {
			id   int64
			name string
		}
		var piles []pileInfo

		for rows.Next() {
			var pi pileInfo
			if err := rows.Scan(&pi.id, &pi.name); err != nil {
				return DBResponse{Err: err}
			}
			piles = append(piles, pi)
		}
		rows.Close()

		// Process each pile
		for _, pile := range piles {
			// Fetch all card IDs for this pile
			cardRows, err := tx.Query(`SELECT id FROM PileCard WHERE pileId = ? ORDER BY id ASC`, pile.id)
			if err != nil {
				return DBResponse{Err: err}
			}

			var cards []int64
			for cardRows.Next() {
				var cardId int64
				if err := cardRows.Scan(&cardId); err != nil {
					cardRows.Close()
					return DBResponse{Err: err}
				}
				cards = append(cards, cardId)
			}
			cardRows.Close()

			if len(cards) == 0 {
				results[pile.name] = 0
				continue
			}

			// Shuffle in memory
			mathrand.Seed(time.Now().UnixNano())
			mathrand.Shuffle(len(cards), func(i, j int) {
				cards[i], cards[j] = cards[j], cards[i]
			})

			// Clear current links
			if _, err := tx.Exec(`UPDATE PileCard SET nextCardId = NULL WHERE pileId = ?`, pile.id); err != nil {
				return DBResponse{Err: err}
			}

			// Rebuild linked list with shuffled order
			for i := 0; i < len(cards)-1; i++ {
				if _, err := tx.Exec(`UPDATE PileCard SET nextCardId = ? WHERE id = ?`, cards[i+1], cards[i]); err != nil {
					return DBResponse{Err: err}
				}
			}

			// Update pile's topCardId to point to the first card in shuffled order
			// Note: The Pile table doesn't have topCardId in your schema, so we skip this
			// The top card is identified as the one NOT referenced by any nextCardId

			results[pile.name] = len(cards)
		}

		if err := tx.Commit(); err != nil {
			return DBResponse{Err: err}
		}

		return DBResponse{Data: results}
	})

	if resp.Err != nil {
		return nil, resp.Err
	}

	if resp.Data != nil {
		if res, ok := resp.Data.(map[string]int); ok {
			return res, nil
		}
	}

	return results, nil
}

// / UpdatePileOrder shuffle une pile
func (w *WorkerPool) UpdatePileOrder(deckId, pileName string, codes []string) error {
	resp := w.Execute(func() DBResponse {
		db := w.handler.db
		w.handler.Lock()
		defer w.handler.UnLock()

		var pileId int64
		row := db.QueryRow(`SELECT id FROM Pile WHERE deckId = ? AND name = ?`, deckId, pileName)
		if err := row.Scan(&pileId); err != nil {
			return DBResponse{Err: err}
		}

		if _, err := db.Exec(`UPDATE PileCard SET nextCardId = NULL WHERE pileId = ?`, pileId); err != nil {
			return DBResponse{Err: err}
		}

		var prevId *int64
		for _, code := range codes {
			var cardId int64
			row := db.QueryRow(`SELECT id FROM PileCard WHERE pileId = ? AND code = ?`, pileId, code)
			if err := row.Scan(&cardId); err != nil {
				return DBResponse{Err: err}
			}
			if prevId != nil {
				if _, err := db.Exec(`UPDATE PileCard SET nextCardId = ? WHERE id = ?`, cardId, *prevId); err != nil {
					return DBResponse{Err: err}
				}
			}
			prevId = &cardId
		}

		return DBResponse{}
	})

	return resp.Err
}

// ShuffleDeck Shuffle un deck
func (w *WorkerPool) ShuffleDeck(value string) (*models.Deck, error) {
	resp := w.Execute(func() DBResponse {
		db := w.handler.db
		w.handler.Lock()
		defer w.handler.UnLock()
		var count int
		row := db.QueryRow(`SELECT COUNT(deckId) FROM Deck WHERE deckId = ?`, value)
		if err := row.Scan(&count); err != nil {
			return DBResponse{Err: err}
		}
		if count == 0 {
			return DBResponse{Err: fmt.Errorf("deck inexistant")}
		}

		rows, err := db.Query(`SELECT id, code FROM DeckCard WHERE deckId = ?`, value)
		if err != nil {
			return DBResponse{Err: err}
		}
		defer rows.Close()

		type CardInfo struct {
			ID   int64
			Code string
		}
		var cards []CardInfo
		for rows.Next() {
			var c CardInfo
			if err := rows.Scan(&c.ID, &c.Code); err != nil {
				return DBResponse{Err: fmt.Errorf("echec de lecture de resultat de requete: %w", err)}
			}
			cards = append(cards, c)
		}
		if err := rows.Err(); err != nil {
			return DBResponse{Err: err}
		}

		if len(cards) == 0 {
			return DBResponse{Data: &models.Deck{Cards: []string{}}}
		}

		mathrand.Seed(time.Now().UnixNano())
		mathrand.Shuffle(len(cards), func(i, j int) {
			cards[i], cards[j] = cards[j], cards[i]
		})

		tx, err := db.Begin()
		if err != nil {
			return DBResponse{Err: fmt.Errorf("echec de demarrage de transaction: %w", err)}
		}
		defer func() {
			if err != nil {
				_ = tx.Rollback()
			}
		}()

		for i := 0; i < len(cards)-1; i++ {
			if _, err := tx.Exec(`UPDATE DeckCard SET nextId = ? WHERE id = ?`, cards[i+1].ID, cards[i].ID); err != nil {
				return DBResponse{Err: fmt.Errorf("echec de mise a jour de nextId: %w", err)}
			}
		}
		if _, err := tx.Exec(`UPDATE DeckCard SET nextId = NULL WHERE id = ?`, cards[len(cards)-1].ID); err != nil {
			return DBResponse{Err: fmt.Errorf("echec de mise a jour du dernier nextId: %w", err)}
		}
		if _, err := tx.Exec(`UPDATE Deck SET topCardId = ?, shuffled = 1 WHERE deckId = ?`, cards[0].ID, value); err != nil {
			return DBResponse{Err: fmt.Errorf("echec de mise a jour du deck: %w", err)}
		}

		if err := tx.Commit(); err != nil {
			return DBResponse{Err: fmt.Errorf("echec de commit: %w", err)}
		}

		codes := make([]string, len(cards))
		for i, card := range cards {
			codes[i] = card.Code
		}
		return DBResponse{Data: &models.Deck{Cards: codes}}
	})

	if resp.Err != nil {
		return nil, resp.Err
	}
	deck, ok := resp.Data.(*models.Deck)
	if !ok {
		return nil, fmt.Errorf("internal: unexpected shuffle result")
	}
	return deck, nil
}

// / Pige une carte d'une pile
func (w *WorkerPool) DrawFromPile(deckId, pileName, method string) (string, error) {
	resp := w.Execute(func() DBResponse {
		db := w.handler.db
		w.handler.Lock()
		defer w.handler.UnLock()

		var pileId int64
		if err := db.QueryRow(`SELECT id FROM Pile WHERE deckId=? AND name=?`, deckId, pileName).Scan(&pileId); err != nil {
			return DBResponse{Err: fmt.Errorf("pile not found: %w", err)}
		}
		var cardId int64
		var cardCode string
		var nextId sql.NullInt64

		switch method {
		case "top":
			row := db.QueryRow(`
				SELECT id, code, nextCardId FROM PileCard
				WHERE pileId=? AND id NOT IN (SELECT nextCardId FROM PileCard WHERE nextCardId IS NOT NULL)
			`, pileId)
			if err := row.Scan(&cardId, &cardCode, &nextId); err != nil {
				if err == sql.ErrNoRows {
					return DBResponse{Err: fmt.Errorf("pile empty")}
				}
				return DBResponse{Err: err}
			}
		case "bottom":
			row := db.QueryRow(`SELECT id, code FROM PileCard WHERE pileId=? AND nextCardId IS NULL`, pileId)
			if err := row.Scan(&cardId, &cardCode); err != nil {
				if err == sql.ErrNoRows {
					return DBResponse{Err: fmt.Errorf("pile empty")}
				}
				return DBResponse{Err: err}
			}
		case "random":
			rows, err := db.Query(`SELECT id, code, nextCardId FROM PileCard WHERE pileId=?`, pileId)
			if err != nil {
				return DBResponse{Err: err}
			}
			defer rows.Close()

			var cards []struct {
				id   int64
				code string
				next sql.NullInt64
			}
			for rows.Next() {
				var c struct {
					id   int64
					code string
					next sql.NullInt64
				}
				if err := rows.Scan(&c.id, &c.code, &c.next); err != nil {
					return DBResponse{Err: err}
				}
				cards = append(cards, c)
			}
			if len(cards) == 0 {
				return DBResponse{Err: fmt.Errorf("pile empty")}
			}
			mathrand.Seed(time.Now().UnixNano())
			pick := cards[mathrand.Intn(len(cards))]
			cardId, cardCode, nextId = pick.id, pick.code, pick.next
		default:
			return DBResponse{Err: fmt.Errorf("invalid draw method")}
		}

		var prevId sql.NullInt64
		_ = db.QueryRow(`SELECT id FROM PileCard WHERE pileId=? AND nextCardId=?`, pileId, cardId).Scan(&prevId)

		tx, err := db.Begin()
		if err != nil {
			return DBResponse{Err: err}
		}
		defer func() {
			if err != nil {
				_ = tx.Rollback()
			}
		}()

		if prevId.Valid {
			if _, err := tx.Exec(`UPDATE PileCard SET nextCardId=? WHERE id=?`, nextId, prevId.Int64); err != nil {
				return DBResponse{Err: err}
			}
		}

		if _, err := tx.Exec(`DELETE FROM PileCard WHERE id=?`, cardId); err != nil {
			return DBResponse{Err: err}
		}

		if _, err := tx.Exec(`UPDATE DeckEntry SET inPile = inPile - 1 WHERE deckId=? AND code=? AND inPile>0`, deckId, cardCode); err != nil {
			return DBResponse{Err: err}
		}

		if err := tx.Commit(); err != nil {
			return DBResponse{Err: err}
		}

		return DBResponse{Data: cardCode}
	})

	if resp.Err != nil {
		return "", resp.Err
	}
	code, ok := resp.Data.(string)
	if !ok {
		return "", fmt.Errorf("internal: unexpected draw result")
	}
	return code, nil
}

// / Pige une carte specifique d'une pile
func (w *WorkerPool) DrawSpecificFromPile(deckId, pileName, code string) (string, error) {
	db := w.handler.db
	w.handler.Lock()
	defer w.handler.UnLock()

	var pileId int64
	if err := db.QueryRow(`SELECT id FROM Pile WHERE deckId=? AND name=?`, deckId, pileName).Scan(&pileId); err != nil {
		return "", err
	}

	var cardId int64
	var nextId sql.NullInt64
	if err := db.QueryRow(`SELECT id, nextCardId FROM PileCard WHERE pileId=? AND code=?`, pileId, code).Scan(&cardId, &nextId); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("card %s not in pile", code)
		}
		return "", err
	}

	var prevId sql.NullInt64
	_ = db.QueryRow(`SELECT id FROM PileCard WHERE pileId=? AND nextCardId=?`, pileId, cardId).Scan(&prevId)

	tx, err := db.Begin()
	if err != nil {
		return "", err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if prevId.Valid {
		if _, err := tx.Exec(`UPDATE PileCard SET nextCardId=? WHERE id=?`, nextId, prevId.Int64); err != nil {
			return "", err
		}
	}

	if _, err := tx.Exec(`DELETE FROM PileCard WHERE id=?`, cardId); err != nil {
		return "", err
	}

	if _, err := tx.Exec(`UPDATE DeckEntry SET inPile = inPile - 1 WHERE deckId=? AND code=? AND inPile>0`, deckId, code); err != nil {
		return "", err
	}

	if err := tx.Commit(); err != nil {
		return "", err
	}

	return code, nil
}

func (w *WorkerPool) ReturnSpecificDrawn(deckId, code string) (string, error) {
	resp := w.Execute(func() DBResponse {
		db := w.handler.db
		w.handler.Lock()
		defer w.handler.UnLock()

		tx, err := db.Begin()
		if err != nil {
			return DBResponse{Err: err}
		}
		defer func() {
			if err != nil {
				_ = tx.Rollback()
			}
		}()

		// Check if card exists and how many are drawn
		var total, inDeck, inPile int
		if err := tx.QueryRow(
			`SELECT total, inDeck, inPile FROM DeckEntry WHERE deckId=? AND code=?`,
			deckId, code,
		).Scan(&total, &inDeck, &inPile); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return DBResponse{Err: fmt.Errorf("card %s not found in deck %s", code, deckId)}
			}
			return DBResponse{Err: fmt.Errorf("query failed: %w", err)}
		}

		// Calculate how many are drawn (not in deck, not in pile)
		drawn := total - inDeck - inPile
		if drawn <= 0 {
			return DBResponse{Err: fmt.Errorf(
				"card %s is not drawn (total=%d, inDeck=%d, inPile=%d)",
				code, total, inDeck, inPile,
			)}
		}

		// Get current top card of the deck
		var topCardId sql.NullInt64
		_ = tx.QueryRow(`SELECT topCardId FROM Deck WHERE deckId = ?`, deckId).Scan(&topCardId)

		// Insert a new DeckCard for the returned card on top
		res, err := tx.Exec(
			`INSERT INTO DeckCard (deckId, code, nextId) VALUES (?, ?, ?)`,
			deckId, code, topCardId,
		)
		if err != nil {
			return DBResponse{Err: fmt.Errorf("insert DeckCard: %w", err)}
		}
		newCardId, _ := res.LastInsertId()

		// Update Deck.topCardId
		if _, err := tx.Exec(
			`UPDATE Deck SET topCardId = ? WHERE deckId = ?`,
			newCardId, deckId,
		); err != nil {
			return DBResponse{Err: fmt.Errorf("update topCardId: %w", err)}
		}

		// Increment inDeck in DeckEntry
		if _, err := tx.Exec(
			`UPDATE DeckEntry SET inDeck = inDeck + 1 WHERE deckId = ? AND code = ?`,
			deckId, code,
		); err != nil {
			return DBResponse{Err: fmt.Errorf("update DeckEntry: %w", err)}
		}

		if err := tx.Commit(); err != nil {
			return DBResponse{Err: err}
		}

		return DBResponse{Data: code}
	})

	if resp.Err != nil {
		return "", resp.Err
	}
	out, ok := resp.Data.(string)
	if !ok {
		return "", fmt.Errorf("internal: unexpected return value")
	}
	return out, nil
}

// ReturnAllDrawn Retourne toutes les cartes pigees dans le deck
func (w *WorkerPool) ReturnAllDrawn(deckId string) error {
	resp := w.Execute(func() DBResponse {
		db := w.handler.db
		w.handler.Lock()
		defer w.handler.UnLock()

		tx, err := db.Begin()
		if err != nil {
			return DBResponse{Err: err}
		}
		defer func() {
			if err != nil {
				_ = tx.Rollback()
			}
		}()

		var topCardId sql.NullInt64
		_ = tx.QueryRow(`SELECT topCardId FROM Deck WHERE deckId = ?`, deckId).Scan(&topCardId)

		rows, err := tx.Query(`SELECT code, total, inDeck, inPile FROM DeckEntry WHERE deckId = ?`, deckId)
		if err != nil {
			return DBResponse{Err: fmt.Errorf("query DeckEntry: %w", err)}
		}
		defer rows.Close()

		type entry struct {
			code  string
			count int
		}
		var toReturn []entry
		for rows.Next() {
			var code string
			var total, inDeck, inPile int
			if err := rows.Scan(&code, &total, &inDeck, &inPile); err != nil {
				return DBResponse{Err: err}
			}
			drawn := total - inDeck - inPile
			if drawn > 0 {
				toReturn = append(toReturn, entry{code, drawn})
			}
		}

		for i := len(toReturn) - 1; i >= 0; i-- {
			for j := 0; j < toReturn[i].count; j++ {
				res, err := tx.Exec(`INSERT INTO DeckCard (deckId, code, nextId) VALUES (?, ?, ?)`, deckId, toReturn[i].code, topCardId)
				if err != nil {
					return DBResponse{Err: fmt.Errorf("insert DeckCard: %w", err)}
				}
				newCardId, _ := res.LastInsertId()
				topCardId.Int64 = newCardId
				topCardId.Valid = true
			}
		}

		if topCardId.Valid {
			if _, err := tx.Exec(`UPDATE Deck SET topCardId = ? WHERE deckId = ?`, topCardId, deckId); err != nil {
				return DBResponse{Err: fmt.Errorf("update topCardId: %w", err)}
			}
		}

		if _, err := tx.Exec(`UPDATE DeckEntry SET inDeck = total - inPile WHERE deckId = ?`, deckId); err != nil {
			return DBResponse{Err: fmt.Errorf("update DeckEntry: %w", err)}
		}

		if err := tx.Commit(); err != nil {
			return DBResponse{Err: err}
		}
		return DBResponse{}
	})
	return resp.Err
}

// ReturnSpecificFromPile Retourne des cartes specifiques d'une pile dans le deck
func (w *WorkerPool) ReturnSpecificFromPile(deckId, pileName, code string) (string, error) {
	resp := w.Execute(func() DBResponse {
		db := w.handler.db
		w.handler.Lock()
		defer w.handler.UnLock()

		var pileId int64
		if err := db.QueryRow(`SELECT id FROM Pile WHERE deckId=? AND name=?`, deckId, pileName).Scan(&pileId); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return DBResponse{Err: fmt.Errorf("pile %s not found", pileName)}
			}
			return DBResponse{Err: fmt.Errorf("pile query: %w", err)}
		}

		var cardId int64
		var next sql.NullInt64
		if err := db.QueryRow(`SELECT id, nextCardId FROM PileCard WHERE pileId=? AND code=?`, pileId, code).Scan(&cardId, &next); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return DBResponse{Err: fmt.Errorf("card %s not in pile %s", code, pileName)}
			}
			return DBResponse{Err: fmt.Errorf("pilecard query: %w", err)}
		}

		var prevId sql.NullInt64
		_ = db.QueryRow(`SELECT id FROM PileCard WHERE pileId=? AND nextCardId=?`, pileId, cardId).Scan(&prevId)

		tx, err := db.Begin()
		if err != nil {
			return DBResponse{Err: fmt.Errorf("begin tx: %w", err)}
		}
		defer func() {
			if err != nil {
				_ = tx.Rollback()
			}
		}()

		if prevId.Valid {
			if _, err := tx.Exec(`UPDATE PileCard SET nextCardId=? WHERE id=?`, next, prevId.Int64); err != nil {
				return DBResponse{Err: fmt.Errorf("update prev: %w", err)}
			}
		}

		if _, err := tx.Exec(`DELETE FROM PileCard WHERE id=?`, cardId); err != nil {
			return DBResponse{Err: fmt.Errorf("delete pilecard: %w", err)}
		}

		res, err := tx.Exec(`
            UPDATE DeckEntry
            SET inPile = inPile - 1, inDeck = inDeck + 1
            WHERE deckId = ? AND code = ? AND inPile > 0
        `, deckId, code)
		if err != nil {
			return DBResponse{Err: fmt.Errorf("update deckentry: %w", err)}
		}
		rows, _ := res.RowsAffected()
		if rows == 0 {
			return DBResponse{Err: fmt.Errorf("inconsistent DeckEntry for code %s (not in pile)", code)}
		}

		var topCardId sql.NullInt64
		_ = tx.QueryRow(`SELECT topCardId FROM Deck WHERE deckId = ?`, deckId).Scan(&topCardId)

		res2, err := tx.Exec(`INSERT INTO DeckCard (deckId, code, nextId) VALUES (?, ?, ?)`, deckId, code, topCardId)
		if err != nil {
			return DBResponse{Err: fmt.Errorf("insert DeckCard: %w", err)}
		}
		newCardId, _ := res2.LastInsertId()

		if _, err := tx.Exec(`UPDATE Deck SET topCardId = ? WHERE deckId = ?`, newCardId, deckId); err != nil {
			return DBResponse{Err: fmt.Errorf("update topCardId: %w", err)}
		}

		if err := tx.Commit(); err != nil {
			return DBResponse{Err: fmt.Errorf("commit: %w", err)}
		}
		return DBResponse{Data: code}
	})
	if resp.Err != nil {
		return "", resp.Err
	}
	out, ok := resp.Data.(string)
	if !ok {
		return "", fmt.Errorf("internal: unexpected return value")
	}
	return out, nil
}

// ReturnAllFromPile Retourne toutes les cartes d'une pile dans le deck
func (w *WorkerPool) ReturnAllFromPile(deckId, pileName string) error {
	resp := w.Execute(func() DBResponse {
		db := w.handler.db
		w.handler.Lock()
		defer w.handler.UnLock()

		// get pile id
		var pileId int64
		if err := db.QueryRow(`SELECT id FROM Pile WHERE deckId=? AND name=?`, deckId, pileName).Scan(&pileId); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return DBResponse{Err: fmt.Errorf("pile %s not found", pileName)}
			}
			return DBResponse{Err: fmt.Errorf("pile query: %w", err)}
		}

		// gather all img in the pile (in order)
		rows, err := db.Query(`
			SELECT code FROM PileCard 
			WHERE pileId=? 
			ORDER BY id ASC
		`, pileId)
		if err != nil {
			return DBResponse{Err: fmt.Errorf("query pilecards: %w", err)}
		}
		defer rows.Close()

		var codes []string
		for rows.Next() {
			var code string
			if err := rows.Scan(&code); err != nil {
				return DBResponse{Err: fmt.Errorf("scan: %w", err)}
			}
			codes = append(codes, code)
		}

		// count occurrences per code for DeckEntry update
		codeCounts := make(map[string]int64)
		for _, code := range codes {
			codeCounts[code]++
		}

		tx, err := db.Begin()
		if err != nil {
			return DBResponse{Err: fmt.Errorf("begin tx: %w", err)}
		}
		defer func() {
			if err != nil {
				_ = tx.Rollback()
			}
		}()

		for code, cnt := range codeCounts {
			res, err := tx.Exec(`
                UPDATE DeckEntry
                SET inPile = inPile - ?, inDeck = inDeck + ?
                WHERE deckId = ? AND code = ? AND inPile >= ?
            `, cnt, cnt, deckId, code, cnt)
			if err != nil {
				return DBResponse{Err: fmt.Errorf("update deckentry: %w", err)}
			}
			n, _ := res.RowsAffected()
			if n == 0 {
				return DBResponse{Err: fmt.Errorf("inconsistent DeckEntry for code %s", code)}
			}
		}

		if _, err := tx.Exec(`DELETE FROM PileCard WHERE pileId = ?`, pileId); err != nil {
			return DBResponse{Err: fmt.Errorf("delete pilecards: %w", err)}
		}

		var topCardId sql.NullInt64
		_ = tx.QueryRow(`SELECT topCardId FROM Deck WHERE deckId = ?`, deckId).Scan(&topCardId)

		for i := len(codes) - 1; i >= 0; i-- {
			res, err := tx.Exec(`INSERT INTO DeckCard (deckId, code, nextId) VALUES (?, ?, ?)`, deckId, codes[i], topCardId)
			if err != nil {
				return DBResponse{Err: fmt.Errorf("insert DeckCard: %w", err)}
			}
			newCardId, _ := res.LastInsertId()
			topCardId.Int64 = newCardId
			topCardId.Valid = true
		}

		if topCardId.Valid {
			if _, err := tx.Exec(`UPDATE Deck SET topCardId = ? WHERE deckId = ?`, topCardId, deckId); err != nil {
				return DBResponse{Err: fmt.Errorf("update topCardId: %w", err)}
			}
		}

		if err := tx.Commit(); err != nil {
			return DBResponse{Err: fmt.Errorf("commit: %w", err)}
		}
		return DBResponse{}
	})
	return resp.Err
}

// DrawCards Pige jusqu'a amount cartes et retourne les codes et le nombre de cartes restantes
func (w *WorkerPool) DrawCards(deckId string, amount int) ([]string, int, error) {
	resp := w.Execute(func() DBResponse {
		db := w.handler.db
		w.handler.Lock()
		defer w.handler.UnLock()

		var remaining int
		row := db.QueryRow(`SELECT COUNT(*) FROM DeckCard WHERE deckId = ?`, deckId)
		if err := row.Scan(&remaining); err != nil {
			return DBResponse{Err: fmt.Errorf("echec de lecture du deck: %w", err)}
		}

		if remaining < amount {
			amount = remaining
		}
		if amount == 0 {
			return DBResponse{Data: struct {
				Codes     []string
				Remaining int
			}{Codes: []string{}, Remaining: remaining}}
		}

		tx, err := db.Begin()
		if err != nil {
			return DBResponse{Err: fmt.Errorf("echec de demarrage de transaction: %w", err)}
		}
		defer func() {
			if err != nil {
				_ = tx.Rollback()
			}
		}()

		var codes []string
		for i := 0; i < amount; i++ {
			var topCardId int64
			var topCardCode string
			var nextId sql.NullInt64

			row := tx.QueryRow(`SELECT DeckCard.id, DeckCard.code, DeckCard.nextId FROM DeckCard INNER JOIN Deck ON DeckCard.id = Deck.topCardId WHERE Deck.deckId = ?`, deckId)
			if err := row.Scan(&topCardId, &topCardCode, &nextId); err != nil {
				if err == sql.ErrNoRows {
					// no top card
					break
				}
				return DBResponse{Err: fmt.Errorf("echec de lecture de la carte du top: %w", err)}
			}

			// UPDATE inDeck
			if _, err := tx.Exec(`UPDATE DeckEntry SET inDeck = inDeck - 1 WHERE deckId = ? AND code = ?`, deckId, topCardCode); err != nil {
				return DBResponse{Err: fmt.Errorf("echec de mise a jour de DeckEntry: %w", err)}
			}

			codes = append(codes, topCardCode)

			// UPDATE topCardId
			var newTopId interface{}
			if nextId.Valid {
				newTopId = nextId.Int64
			} else {
				newTopId = nil
			}
			if _, err := tx.Exec(`UPDATE Deck SET topCardId = ? WHERE deckId = ?`, newTopId, deckId); err != nil {
				return DBResponse{Err: fmt.Errorf("echec de mise a jour du topCardId: %w", err)}
			}

			// DELETE card
			if _, err := tx.Exec(`DELETE FROM DeckCard WHERE id = ?`, topCardId); err != nil {
				return DBResponse{Err: fmt.Errorf("echec de suppression de carte: %w", err)}
			}

			remaining--
		}

		if err := tx.Commit(); err != nil {
			return DBResponse{Err: fmt.Errorf("echec de commit: %w", err)}
		}

		return DBResponse{Data: struct {
			Codes     []string
			Remaining int
		}{Codes: codes, Remaining: remaining}}
	})

	if resp.Err != nil {
		return nil, 0, resp.Err
	}
	data, ok := resp.Data.(struct {
		Codes     []string
		Remaining int
	})
	if !ok {
		type drawResult struct {
			Codes     []string
			Remaining int
		}
		dr, ok2 := resp.Data.(drawResult)
		if ok2 {
			return dr.Codes, dr.Remaining, nil
		}
		// fallback: attempt to interpret as map[string]interface{}
		if m, ok := resp.Data.(map[string]interface{}); ok {
			codes, _ := m["Codes"].([]string)
			rem, _ := m["Remaining"].(int)
			return codes, rem, nil
		}

		return nil, 0, fmt.Errorf("internal: unexpected draw result")
	}
	return data.Codes, data.Remaining, nil
}
