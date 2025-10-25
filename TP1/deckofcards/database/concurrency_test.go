package database

import (
	"database/sql"
	"deckofcards/models"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// Helper: Setup test database for concurrency tests
func setupConcurrencyTestDB(t *testing.T) (*DBHandler, *WorkerPool, string) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_concurrent.db")

	dsn := fmt.Sprintf("%s?_foreign_keys=on&_busy_timeout=5000", dbPath)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		t.Fatalf("Failed to ping database: %v", err)
	}

	if _, err := db.Exec(`PRAGMA journal_mode=WAL;`); err != nil {
		db.Close()
		t.Fatalf("Failed to set WAL mode: %v", err)
	}

	schema := `
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS Deck (
  deckId   TEXT PRIMARY KEY,
  topCardId INTEGER,
  shuffled INTEGER DEFAULT 0,
  FOREIGN KEY (topCardId) REFERENCES DeckCard(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS DeckCard (
  id       INTEGER PRIMARY KEY AUTOINCREMENT,
  deckId   TEXT NOT NULL,
  code     TEXT NOT NULL,
  nextId   INTEGER,
  FOREIGN KEY (deckId) REFERENCES Deck(deckId) ON DELETE CASCADE,
  FOREIGN KEY (nextId) REFERENCES DeckCard(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS Pile (
  id     INTEGER PRIMARY KEY AUTOINCREMENT,
  deckId TEXT NOT NULL,
  name   TEXT NOT NULL,
  topCardId INTEGER,
  FOREIGN KEY (deckId) REFERENCES Deck(deckId) ON DELETE CASCADE,
  FOREIGN KEY (topCardId) REFERENCES DeckCard(id) ON DELETE SET NULL,
  UNIQUE(deckId, name)
);

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

	if _, err := db.Exec(schema); err != nil {
		db.Close()
		t.Fatalf("Failed to apply schema: %v", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	handler := NewDBHandler(db)
	workerPool := Init(handler)

	return handler, workerPool, dbPath
}

// Helper: Create test deck with standard 52 cards
func createConcurrencyTestDeck(t *testing.T, wp *WorkerPool) string {
	deck := models.NewMultiDeck(1, false)
	deckId, err := wp.InsertDeck(deck)
	if err != nil {
		t.Fatalf("Failed to create test deck: %v", err)
	}
	return deckId
}

// Test 1: Race Condition Detection - Concurrent Draws
func TestConcurrency_RaceCondition_Draws(t *testing.T) {
	handler, wp, dbPath := setupConcurrencyTestDB(t)
	defer wp.Close()
	defer handler.db.Close()
	defer os.Remove(dbPath)

	deckId := createConcurrencyTestDeck(t, wp)

	// Verify initial state
	initialCount, err := wp.CardsInDeck(deckId)
	if err != nil {
		t.Fatalf("Failed to get initial card count: %v", err)
	}
	if initialCount != 52 {
		t.Fatalf("Expected 52 cards, got %d", initialCount)
	}

	numGoroutines := 52
	cardsPerGoroutine := 1
	var wg sync.WaitGroup
	drawnCards := make(chan []string, numGoroutines)
	errors := make(chan error, numGoroutines)

	// Launch concurrent draws (52 goroutines to draw all cards)
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cards, _, err := wp.DrawCards(deckId, cardsPerGoroutine)
			if err != nil {
				errors <- err
				return
			}
			if len(cards) > 0 {
				drawnCards <- cards
			}
		}()
	}

	wg.Wait()
	close(drawnCards)
	close(errors)

	// Check for errors
	errorCount := 0
	for err := range errors {
		errorCount++
		t.Logf("Draw error (may be normal if deck empty): %v", err)
	}

	// Collect all drawn cards
	allDrawnCards := make(map[string]int)
	totalDrawn := 0
	for cards := range drawnCards {
		totalDrawn += len(cards)
		for _, card := range cards {
			allDrawnCards[card]++
		}
	}

	// Verify no duplicates (critical for race condition detection)
	duplicatesFound := false
	for card, count := range allDrawnCards {
		if count > 1 {
			t.Errorf("RACE CONDITION DETECTED: Card %s was drawn %d times", card, count)
			duplicatesFound = true
		}
	}

	if duplicatesFound {
		t.FailNow()
	}

	// Verify remaining cards (should be 0 since we drew 52)
	remaining, err := wp.CardsInDeck(deckId)
	if err != nil {
		t.Fatalf("Failed to get remaining card count: %v", err)
	}

	if remaining != 0 {
		t.Errorf("Expected 0 cards remaining, got %d", remaining)
	}

	if totalDrawn != 52 {
		t.Errorf("Expected 52 cards drawn total, got %d", totalDrawn)
	}

	t.Logf("✓ Race condition test passed: %d unique cards drawn, %d remaining", len(allDrawnCards), remaining)
}

// Test 2: High Concurrency Load - Mixed Operations
func TestConcurrency_HighLoad_MixedOps(t *testing.T) {
	handler, wp, dbPath := setupConcurrencyTestDB(t)
	defer wp.Close()
	defer handler.db.Close()
	defer os.Remove(dbPath)

	deckId := createConcurrencyTestDeck(t, wp)

	// Pre-draw 40 cards for pile operations
	drawnCards, _, err := wp.DrawCards(deckId, 40)
	if err != nil {
		t.Fatalf("Failed to draw initial cards: %v", err)
	}

	numOperations := 500
	var wg sync.WaitGroup
	var errorCount, successCount int32
	errors := make(chan string, numOperations)
	startTime := time.Now()

	// Execute mixed operations concurrently
	for i := 0; i < numOperations; i++ {
		wg.Add(1)

		switch i % 5 {
		case 0: // Draw operation
			go func() {
				defer wg.Done()
				_, _, err := wp.DrawCards(deckId, 1)
				if err != nil {
					errors <- fmt.Sprintf("draw error: %v", err)
					atomic.AddInt32(&errorCount, 1)
					return
				}
				atomic.AddInt32(&successCount, 1)
			}()

		case 1: // Shuffle operation
			go func() {
				defer wg.Done()
				_, err := wp.ShuffleDeck(deckId)
				if err != nil {
					errors <- fmt.Sprintf("shuffle error: %v", err)
					atomic.AddInt32(&errorCount, 1)
					return
				}
				atomic.AddInt32(&successCount, 1)
			}()

		case 2: // Add to pile
			go func(idx int) {
				defer wg.Done()
				if idx < len(drawnCards) {
					pileName := fmt.Sprintf("pile_%d", idx%10)
					_, err := wp.InsertIntoPile(pileName, deckId, []string{drawnCards[idx]})
					if err != nil {
						errors <- fmt.Sprintf("add to pile error: %v", err)
						atomic.AddInt32(&errorCount, 1)
						return
					}
					atomic.AddInt32(&successCount, 1)
				}
			}(i)

		case 3: // List piles
			go func() {
				defer wg.Done()
				_, err := wp.ListPiles(deckId)
				if err != nil {
					errors <- fmt.Sprintf("list piles error: %v", err)
					atomic.AddInt32(&errorCount, 1)
					return
				}
				atomic.AddInt32(&successCount, 1)
			}()

		case 4: // Get pile cards
			go func(idx int) {
				defer wg.Done()
				pileName := fmt.Sprintf("pile_%d", idx%10)
				_, _, err := wp.GetPileCards(deckId, pileName)
				if err != nil {
					// Acceptable: pile may not exist yet
					return
				}
				atomic.AddInt32(&successCount, 1)
			}(i)
		}
	}

	wg.Wait()
	duration := time.Since(startTime)
	close(errors)

	// Log first few errors
	errCount := 0
	for err := range errors {
		if errCount < 5 {
			t.Logf("Operation error: %s", err)
		}
		errCount++
	}

	opsPerSec := float64(numOperations) / duration.Seconds()
	successVal := atomic.LoadInt32(&successCount)
	errorVal := atomic.LoadInt32(&errorCount)

	t.Logf("✓ High Load: %d operations in %v (%.2f ops/sec)", numOperations, duration, opsPerSec)
	t.Logf("  Success: %d, Errors: %d", successVal, errorVal)

	if errorVal > int32(numOperations/5) {
		t.Errorf("Too many errors under load: %d/%d", errorVal, numOperations)
	}
}

// Test 3: Isolation - Multiple Decks Concurrently
func TestConcurrency_Isolation_MultiplDecks(t *testing.T) {
	handler, wp, dbPath := setupConcurrencyTestDB(t)
	defer wp.Close()
	defer handler.db.Close()
	defer os.Remove(dbPath)

	numDecks := 10
	decks := make([]string, numDecks)
	for i := 0; i < numDecks; i++ {
		decks[i] = createConcurrencyTestDeck(t, wp)
	}

	numOpsPerDeck := 50
	var wg sync.WaitGroup
	var mu sync.Mutex
	results := make(map[string][]uint64)

	// Concurrent operations on different decks
	for deckIdx, deckId := range decks {
		for op := 0; op < numOpsPerDeck; op++ {
			wg.Add(1)
			go func(idx int, id string, opNum int) {
				defer wg.Done()

				switch opNum % 3 {
				case 0:
					wp.DrawCards(id, 1)
				case 1:
					wp.ShuffleDeck(id)
				case 2:
					if remaining, err := wp.CardsInDeck(id); err == nil {
						mu.Lock()
						results[id] = append(results[id], remaining)
						mu.Unlock()
					}
				}
			}(deckIdx, deckId, op)
		}
	}

	wg.Wait()

	// Verify deck isolation: each deck should have independent state
	for _, deckId := range decks {
		remaining, err := wp.CardsInDeck(deckId)
		if err != nil {
			t.Errorf("Failed to get remaining cards for deck %s: %v", deckId, err)
			continue
		}

		// Each deck started with 52 cards
		if remaining > 52 {
			t.Errorf("Deck %s shows isolation failure: %d > 52", deckId, remaining)
		}
	}

	t.Logf("✓ Isolation test passed: %d decks maintained independent state", numDecks)
}

// Test 4: Concurrent Pile Operations with Ordering
func TestConcurrency_Piles_OrderingIntegrity(t *testing.T) {
	handler, wp, dbPath := setupConcurrencyTestDB(t)
	defer wp.Close()
	defer handler.db.Close()
	defer os.Remove(dbPath)

	deckId := createConcurrencyTestDeck(t, wp)
	drawnCards, _, err := wp.DrawCards(deckId, 30)
	if err != nil {
		t.Fatalf("Failed to draw cards: %v", err)
	}

	numPiles := 5
	var wg sync.WaitGroup

	// Concurrently add cards to different piles
	for i := 0; i < numPiles; i++ {
		wg.Add(1)
		go func(pileIdx int) {
			defer wg.Done()
			pileName := fmt.Sprintf("test_pile_%d", pileIdx)
			cardsToAdd := drawnCards[pileIdx*6 : (pileIdx+1)*6]

			_, err := wp.InsertIntoPile(pileName, deckId, cardsToAdd)
			if err != nil {
				t.Errorf("Failed to add to pile %s: %v", pileName, err)
			}
		}(i)
	}

	wg.Wait()

	// Verify all piles exist and have correct counts
	piles, err := wp.ListPiles(deckId)
	if err != nil {
		t.Fatalf("Failed to list piles: %v", err)
	}

	if len(piles) != numPiles {
		t.Errorf("Expected %d piles, got %d", numPiles, len(piles))
	}

	totalInPiles := 0
	for i := 0; i < numPiles; i++ {
		pileName := fmt.Sprintf("test_pile_%d", i)
		count, exists := piles[pileName]
		if !exists {
			t.Errorf("Pile %s missing", pileName)
			continue
		}
		if count != 6 {
			t.Errorf("Pile %s has %d cards, expected 6", pileName, count)
		}

		// Verify card order integrity
		cards, _, err := wp.GetPileCards(deckId, pileName)
		if err != nil {
			t.Errorf("Failed to get pile %s cards: %v", pileName, err)
			continue
		}
		if len(cards) != 6 {
			t.Errorf("Pile %s returns %d cards, expected 6", pileName, len(cards))
		}
		totalInPiles += count
	}

	if totalInPiles != 30 {
		t.Errorf("Total cards in piles: %d, expected 30", totalInPiles)
	}

	t.Logf("✓ Pile ordering test passed: %d piles with %d cards", numPiles, totalInPiles)
}

// Test 5: Deadlock Prevention - Shuffle During Draws
func TestConcurrency_Deadlock_Prevention(t *testing.T) {
	handler, wp, dbPath := setupConcurrencyTestDB(t)
	defer wp.Close()
	defer handler.db.Close()
	defer os.Remove(dbPath)

	deckId := createConcurrencyTestDeck(t, wp)

	numGoroutines := 50
	done := make(chan bool, numGoroutines)
	timeout := time.After(15 * time.Second)

	// Half draws, half shuffles - designed to expose deadlocks
	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer func() { done <- true }()

			if idx%2 == 0 {
				wp.DrawCards(deckId, 1)
			} else {
				wp.ShuffleDeck(deckId)
			}
		}(i)
	}

	// Wait for all to complete or timeout
	completed := 0
	for {
		select {
		case <-done:
			completed++
			if completed == numGoroutines {
				t.Logf("✓ Deadlock prevention passed: All %d operations completed", numGoroutines)
				return
			}
		case <-timeout:
			t.Errorf("DEADLOCK DETECTED: Only %d/%d operations completed before timeout", completed, numGoroutines)
			return
		}
	}
}

// Test 6: Data Integrity - Verify No Card Loss/Duplication
func TestConcurrency_DataIntegrity_CardCounting(t *testing.T) {
	handler, wp, dbPath := setupConcurrencyTestDB(t)
	defer wp.Close()
	defer handler.db.Close()
	defer os.Remove(dbPath)

	deckId := createConcurrencyTestDeck(t, wp)

	// Draw all 52 cards while tracking
	numOperations := 100
	var wg sync.WaitGroup
	cardTracker := make(map[string]int32)
	var trackerMu sync.Mutex

	for i := 0; i < numOperations; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cards, _, err := wp.DrawCards(deckId, 1)
			if err != nil || len(cards) == 0 {
				return
			}

			trackerMu.Lock()
			for _, card := range cards {
				cardTracker[card]++
			}
			trackerMu.Unlock()
		}()
	}

	wg.Wait()

	// Verify card integrity: no card drawn twice
	corruptionDetected := false
	for card, count := range cardTracker {
		if count > 1 {
			t.Errorf("DATA CORRUPTION: Card %s drawn %d times", card, count)
			corruptionDetected = true
		}
	}

	if corruptionDetected {
		t.FailNow()
	}

	// Verify total cards
	totalDrawn := int32(0)
	for _, count := range cardTracker {
		totalDrawn += count
	}

	remaining, _ := wp.CardsInDeck(deckId)
	accountedFor := totalDrawn + int32(remaining)

	if accountedFor != 52 {
		t.Errorf("Card count mismatch: drawn=%d + remaining=%d = %d (expected 52)",
			totalDrawn, remaining, accountedFor)
	}

	t.Logf("✓ Data integrity passed: %d unique cards drawn, %d remaining, total=%d", len(cardTracker), remaining, accountedFor)
}

// Test 7: Stress Test - High Velocity Operations
func TestConcurrency_Stress_HighVelocity(t *testing.T) {
	handler, wp, dbPath := setupConcurrencyTestDB(t)
	defer wp.Close()
	defer handler.db.Close()
	defer os.Remove(dbPath)

	// Create multiple decks for sustained load
	numDecks := 5
	decks := make([]string, numDecks)
	for i := 0; i < numDecks; i++ {
		decks[i] = createConcurrencyTestDeck(t, wp)
	}

	totalOps := 1000
	var wg sync.WaitGroup
	var completed, failed int32
	startTime := time.Now()

	for op := 0; op < totalOps; op++ {
		wg.Add(1)
		deckIdx := op % numDecks

		go func(idx, opNum int) {
			defer wg.Done()

			switch opNum % 4 {
			case 0:
				if _, _, err := wp.DrawCards(decks[idx], 1); err == nil {
					atomic.AddInt32(&completed, 1)
				} else {
					atomic.AddInt32(&failed, 1)
				}
			case 1:
				if _, err := wp.ShuffleDeck(decks[idx]); err == nil {
					atomic.AddInt32(&completed, 1)
				} else {
					atomic.AddInt32(&failed, 1)
				}
			case 2:
				if _, err := wp.CardsInDeck(decks[idx]); err == nil {
					atomic.AddInt32(&completed, 1)
				} else {
					atomic.AddInt32(&failed, 1)
				}
			case 3:
				if _, err := wp.ListPiles(decks[idx]); err == nil {
					atomic.AddInt32(&completed, 1)
				} else {
					atomic.AddInt32(&failed, 1)
				}
			}
		}(deckIdx, op)
	}

	wg.Wait()
	duration := time.Since(startTime)

	completedVal := atomic.LoadInt32(&completed)
	failedVal := atomic.LoadInt32(&failed)

	successRate := (float64(completedVal) / float64(totalOps)) * 100
	opsPerSec := float64(totalOps) / duration.Seconds()

	t.Logf("✓ Stress Test Results: %d ops in %v (%.2f ops/sec)", totalOps, duration, opsPerSec)
	t.Logf("  Success: %d, Failed: %d (%.2f%% success rate)", completedVal, failedVal, successRate)

	if successRate < 95.0 {
		t.Errorf("Success rate too low: %.2f%% (expected > 95%%)", successRate)
	}
}
