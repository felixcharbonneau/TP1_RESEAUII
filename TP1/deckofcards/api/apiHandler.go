package api

import (
	"deckofcards/database"
	"deckofcards/models"
	"deckofcards/utils"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// /RegisterHandlers Enregistre les endpoints de l'api
func RegisterHandlers(workerPool *database.WorkerPool) {
	http.HandleFunc("GET /api/deck/new/{$}", newDeck(workerPool))
	http.HandleFunc("GET /api/deck/new/draw/{$}", newDeckDraw(workerPool))
	http.HandleFunc("GET /api/deck/new/shuffle/{$}", newDeckShuffled(workerPool))
	http.HandleFunc("GET /api/deck/{deck_id}/shuffle/{$}", shuffleDeck(workerPool))
	http.HandleFunc("GET /api/deck/{deck_id}/draw/{$}", drawCards(workerPool))
	http.HandleFunc("GET /api/deck/{deck_id}/pile/{pile_name}/add/{$}", addToPile(workerPool))
	http.HandleFunc("GET /api/deck/{deck_id}/pile/{pile_name}/list/{$}", listPiles(workerPool))
	http.HandleFunc("GET /api/deck/{deck_id}/pile/{pile_name}/shuffle/{$}", shufflePile(workerPool))

	http.HandleFunc("/api/deck/{deck_id}/pile/{pile_name}/draw/{$}", drawPile(workerPool, "top"))
	http.HandleFunc("/api/deck/{deck_id}/pile/{pile_name}/draw/bottom/{$}", drawPile(workerPool, "bottom"))
	http.HandleFunc("/api/deck/{deck_id}/pile/{pile_name}/draw/random/{$}", drawPile(workerPool, "random"))
	http.HandleFunc("/api/deck/{deck_id}/return/{$}", returnCardsHandler(workerPool))
	http.HandleFunc("/api/deck/{deck_id}/pile/{pile_name}/return/{$}", returnCardsHandler(workerPool))

	http.HandleFunc("GET /static/img/{filename}", serveCardImage)
	http.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		http.ServeFile(w, r, "index.html")
	})
}

// serveCardImage Retourne les images svg des cartes
func serveCardImage(w http.ResponseWriter, r *http.Request) {
	filename := r.PathValue("filename")

	if strings.Contains(filename, "..") || strings.Contains(filename, "/") || strings.Contains(filename, "\\") {
		http.Error(w, "Invalid filename", http.StatusBadRequest)
		return
	}

	if !strings.HasSuffix(filename, ".svg") {
		http.Error(w, "Only SVG files are allowed", http.StatusBadRequest)
		return
	}

	filePath := filepath.Join("static", "img", filename)

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		http.Error(w, "Card image not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "image/svg+xml")
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	http.ServeFile(w, r, filePath)
}

// / Retourne les cartes dans le deck
func returnCardsHandler(workerPool *database.WorkerPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		deckId := r.PathValue("deck_id")
		pileName := r.PathValue("pile_name")
		cardsParam := r.URL.Query().Get("img")

		var requested []string
		if cardsParam != "" {
			for _, c := range strings.Split(cardsParam, ",") {
				c = strings.TrimSpace(c)
				if c != "" {
					requested = append(requested, c)
				}
			}
			if len(requested) == 0 {
				_ = json.NewEncoder(w).Encode(Response{
					Success: false, DeckId: deckId, Shuffled: nil, Remaining: 0,
					Piles: map[string]PileResponse{},
				})
				return
			}
		}

		var err error
		if pileName != "" {
			if len(requested) > 0 {
				for _, code := range requested {
					_, err = workerPool.ReturnSpecificFromPile(deckId, pileName, code)
					if err != nil {
						_ = json.NewEncoder(w).Encode(Response{
							Success: false, DeckId: deckId, Error: err.Error(),
							Piles: map[string]PileResponse{},
						})
						return
					}
				}
			} else {
				if err = workerPool.ReturnAllFromPile(deckId, pileName); err != nil {
					_ = json.NewEncoder(w).Encode(Response{
						Success: false, DeckId: deckId, Error: err.Error(),
						Piles: map[string]PileResponse{},
					})
					return
				}
			}
		} else {
			if len(requested) > 0 {
				for _, code := range requested {
					if _, err = workerPool.ReturnSpecificDrawn(deckId, code); err != nil {
						_ = json.NewEncoder(w).Encode(Response{
							Success: false, DeckId: deckId, Error: err.Error(),
							Piles: map[string]PileResponse{},
						})
						return
					}
				}
			} else {
				// Return all drawn img
				if err = workerPool.ReturnAllDrawn(deckId); err != nil {
					_ = json.NewEncoder(w).Encode(Response{
						Success: false, DeckId: deckId, Error: err.Error(),
						Piles: map[string]PileResponse{},
					})
					return
				}
			}
		}

		// success - build response
		deckRemaining, _ := workerPool.CardsInDeck(deckId)

		pilesResp := make(map[string]PileResponse)
		if pileName != "" {
			pcount, _ := workerPool.CardsInPile(deckId, pileName)
			pilesResp[pileName] = PileResponse{Remaining: int(pcount)}
		}

		resp := Response{
			Success:   true,
			DeckId:    deckId,
			Remaining: int(deckRemaining),
			Piles:     pilesResp,
		}
		_ = json.NewEncoder(w).Encode(resp)
	}
}

func shufflePile(workerPool *database.WorkerPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		deckId := r.PathValue("deck_id")
		pileName := r.PathValue("pile_name")

		// 1. Get img in the requested pile
		codes, _, err := workerPool.GetPileCards(deckId, pileName)
		if err != nil {
			_ = json.NewEncoder(w).Encode(Response{
				Success: false,
				DeckId:  deckId,
				Error:   err.Error(),
			})
			return
		}

		// 2. Shuffle them
		rand.Seed(time.Now().UnixNano())
		for i := range codes {
			j := rand.Intn(i + 1)
			codes[i], codes[j] = codes[j], codes[i]
		}

		// 3. Persist new order
		if err := workerPool.UpdatePileOrder(deckId, pileName, codes); err != nil {
			_ = json.NewEncoder(w).Encode(Response{
				Success: false,
				DeckId:  deckId,
				Error:   err.Error(),
			})
			return
		}

		// 4. Get remaining img in **this pile only**
		pileRemaining, err := workerPool.CardsInPile(deckId, pileName)
		if err != nil {
			_ = json.NewEncoder(w).Encode(Response{
				Success: false,
				DeckId:  deckId,
				Error:   err.Error(),
			})
			return
		}

		// 5. Get deck remaining (img not in any pile)
		deckRemaining, err := workerPool.CardsInDeck(deckId)
		if err != nil {
			_ = json.NewEncoder(w).Encode(Response{
				Success: false,
				DeckId:  deckId,
				Error:   err.Error(),
			})
			return
		}

		// 6. Return response with only the shuffled pile
		resp := Response{
			Success:   true,
			DeckId:    deckId,
			Remaining: int(deckRemaining),
			Piles: map[string]PileResponse{
				pileName: {Remaining: int(pileRemaining)},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}
}

func listPiles(workerPool *database.WorkerPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		deckId := r.PathValue("deck_id")
		requestedPile := r.PathValue("pile_name")

		// 1. Get all pile names and their counts
		allPiles, err := workerPool.ListPiles(deckId)
		if err != nil {
			_ = json.NewEncoder(w).Encode(Response{
				Success: false,
				DeckId:  deckId,
				Error:   err.Error(),
			})
			return
		}

		// 2. Get img for the requested pile only
		var cards []CardResponse
		if requestedPile != "" {
			codes, _, err := workerPool.GetPileCards(deckId, requestedPile)
			if err != nil {
				_ = json.NewEncoder(w).Encode(Response{
					Success: false,
					DeckId:  deckId,
					Error:   err.Error(),
				})
				return
			}
			for _, code := range codes {
				value, _ := models.GetValue(code)
				suit, _ := models.GetSuit(code)
				cards = append(cards, CardResponse{
					Code:  code,
					Image: fmt.Sprintf(utils.SERVER_PATH+"/static/img/%s.svg", code),
					Value: value,
					Suit:  suit,
				})
			}
		}

		// 3. Build pile map
		piles := make(map[string]PileResponse)
		for name, count := range allPiles {
			pileResp := PileResponse{
				Remaining: count,
			}
			if name == requestedPile {
				pileResp.Cards = cards
			}
			piles[name] = pileResp
		}

		// 4. Compute remaining img in deck not in piles
		deckRemaining, err := workerPool.CardsInDeck(deckId)
		if err != nil {
			_ = json.NewEncoder(w).Encode(Response{
				Success: false,
				DeckId:  deckId,
				Error:   err.Error(),
			})
			return
		}

		// 5. Send response
		resp := Response{
			Success:   true,
			DeckId:    deckId,
			Remaining: int(deckRemaining),
			Piles:     piles,
		}
		_ = json.NewEncoder(w).Encode(resp)
	}
}

func addToPile(workerPool *database.WorkerPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		deckId := r.PathValue("deck_id")
		pileName := r.PathValue("pile_name")
		cardsParam := r.URL.Query().Get("cards")

		if cardsParam == "" {
			writeError(w, ErrInvalidParameter, deckId)
			return
		}

		cardsArray := strings.Split(cardsParam, ",")
		if len(cardsArray) == 0 {
			writeError(w, ErrInvalidParameter, deckId)
			return
		}

		// Check for duplicates
		seen := make(map[string]bool)
		for _, card := range cardsArray {
			card = strings.TrimSpace(card)
			if card == "" {
				continue
			}
			if seen[card] {
				writeError(w, ErrDuplicateCards, deckId)
				return
			}
			seen[card] = true
		}

		inserted, err := workerPool.InsertIntoPile(pileName, deckId, cardsArray)
		if err != nil {
			if strings.Contains(err.Error(), "non trouvee") || strings.Contains(err.Error(), "not found") {
				writeError(w, ErrCardNotInDeck, deckId)
				return
			}
			if strings.Contains(err.Error(), "non pigée") || strings.Contains(err.Error(), "not drawn") {
				writeError(w, ErrCardNotInDeck, deckId)
				return
			}
			writeError(w, ErrDatabase, deckId)
			return
		}

		piles := make(map[string]PileResponse)
		piles[pileName] = PileResponse{
			Remaining: inserted.Piles[pileName].Remaining,
		}

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(Response{
			Success:   true,
			Remaining: inserted.Remaining,
			DeckId:    inserted.Id,
			Piles:     piles,
		})
	}
}
func drawCards(workerPool *database.WorkerPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		deckId := r.PathValue("deck_id")

		count := 1
		if r.URL.Query().Has("count") {
			c, err := strconv.Atoi(r.URL.Query().Get("count"))
			if err != nil || c <= 0 {
				writeError(w, ErrInvalidParameter, deckId)
				return
			}
			count = c
		}

		cards, remaining, err := workerPool.DrawCards(deckId, count)
		if err != nil {
			if strings.Contains(err.Error(), "deck inexistant") {
				writeError(w, ErrDeckNotFound, deckId)
				return
			}
			writeError(w, ErrDatabase, deckId)
			return
		}

		if len(cards) == 0 {
			writeError(w, ErrDeckEmpty, deckId)
			return
		}

		responses := make([]CardResponse, len(cards))
		for idx, card := range cards {
			value, _ := models.GetValue(card)
			suit, _ := models.GetSuit(card)
			responses[idx] = CardResponse{
				Code:  card,
				Image: fmt.Sprintf(utils.SERVER_PATH+"/static/img/%s.svg", card),
				Value: value,
				Suit:  suit,
			}
		}

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(Response{
			Success:   true,
			DeckId:    deckId,
			Cards:     responses,
			Remaining: remaining,
		})
	}
}

// Updated drawPile handler
func drawPile(workerPool *database.WorkerPool, method string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		deckId := r.PathValue("deck_id")
		pileName := r.PathValue("pile_name")

		var drawn []string
		cardsParam := r.URL.Query().Get("cards")

		if cardsParam != "" {
			// Check for duplicates
			codes := strings.Split(cardsParam, ",")
			seen := make(map[string]bool)
			for _, code := range codes {
				code = strings.TrimSpace(code)
				if code == "" {
					continue
				}
				if seen[code] {
					writeError(w, ErrDuplicateCards, deckId)
					return
				}
				seen[code] = true

				cardCode, err := workerPool.DrawSpecificFromPile(deckId, pileName, code)
				if err != nil {
					if strings.Contains(err.Error(), "not in pile") {
						writeError(w, ErrCardNotInPile, deckId)
						return
					}
					if strings.Contains(err.Error(), "pile") && strings.Contains(err.Error(), "not found") {
						writeError(w, ErrPileNotFound, deckId)
						return
					}
					writeError(w, ErrDatabase, deckId)
					return
				}
				drawn = append(drawn, cardCode)
			}
		} else {
			count := 1
			if r.URL.Query().Has("count") {
				c, err := strconv.Atoi(r.URL.Query().Get("count"))
				if err != nil || c <= 0 {
					writeError(w, ErrInvalidParameter, deckId)
					return
				}
				count = c
			}

			for i := 0; i < count; i++ {
				card, err := workerPool.DrawFromPile(deckId, pileName, method)
				if err != nil {
					if strings.Contains(err.Error(), "empty") {
						writeError(w, ErrPileEmpty, deckId)
						return
					}
					if strings.Contains(err.Error(), "not found") {
						writeError(w, ErrPileNotFound, deckId)
						return
					}
					writeError(w, ErrDatabase, deckId)
					return
				}
				if card == "" {
					break
				}
				drawn = append(drawn, card)
			}
		}

		if len(drawn) == 0 {
			writeError(w, ErrPileEmpty, deckId)
			return
		}

		pileRemaining, _ := workerPool.CardsInPile(deckId, pileName)
		deckRemaining, _ := workerPool.CardsInDeck(deckId)

		cardResponses := make([]CardResponse, len(drawn))
		for i, code := range drawn {
			value, _ := models.GetValue(code)
			suit, _ := models.GetSuit(code)
			cardResponses[i] = CardResponse{
				Code:  code,
				Value: value,
				Suit:  suit,
				Image: fmt.Sprintf(utils.SERVER_PATH+"/static/img/%s.svg", code),
			}
		}

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(Response{
			Success:   true,
			DeckId:    deckId,
			Remaining: int(deckRemaining),
			Piles: map[string]PileResponse{
				pileName: {Remaining: int(pileRemaining)},
			},
			Cards: cardResponses,
		})
	}
}
func newDeckDraw(workerPool *database.WorkerPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		shuffled := true
		deck := models.NewMultiDeck(1, false)
		deck.Shuffle()
		id, err := workerPool.InsertDeck(deck)
		resp := Response{}
		if err != nil {
			resp.Success = false
			resp.Error = err.Error()
		} else {
			resp.Success = true
			resp.Shuffled = &shuffled
			resp.DeckId = id
			count := 1
			if r.URL.Query().Has("count") {
				count, _ = strconv.Atoi(r.URL.Query().Get("count"))
			}
			cards, remaining, err := workerPool.DrawCards(id, count)
			if err != nil {
				resp.Success = false
				resp.Error = err.Error()
			} else {
				responses := make([]CardResponse, len(cards))
				for idx, card := range cards {
					value, _ := models.GetValue(card)
					suit, _ := models.GetSuit(card)
					responses[idx] = CardResponse{
						Code:  card,
						Image: fmt.Sprintf(utils.SERVER_PATH+"/static/img/%s.svg", card),
						Value: value,
						Suit:  suit,
					}
				}
				var errMessage string
				if len(responses) <= 0 {
					errMessage = "Plus de cartes dans le deck"
				}
				resp.Cards = responses
				resp.Remaining = remaining
				resp.Error = errMessage
				resp.Success = len(cards) > 0
			}
		}
		_ = json.NewEncoder(w).Encode(resp)
	}
}

func shuffleDeck(workerPool *database.WorkerPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		deckID := r.PathValue("deck_id")
		wantRemainingOnly := r.URL.Query().Get("remaining") == "true"

		deck, err := workerPool.ShuffleDeck(deckID)
		if err != nil {
			if strings.Contains(err.Error(), "inexistant") || strings.Contains(err.Error(), "not found") {
				writeError(w, ErrDeckNotFound, deckID)
				return
			}
			writeError(w, ErrDatabase, deckID)
			return
		}

		shuffled := true
		resp := Response{
			Success:   true,
			DeckId:    deckID,
			Shuffled:  &shuffled,
			Remaining: len(deck.Cards),
		}

		if !wantRemainingOnly {
			piles, err := workerPool.ShuffleAllPiles(deckID)
			if err != nil {
				writeError(w, ErrDatabase, deckID)
				return
			}

			if len(piles) > 0 {
				resp.Piles = make(map[string]PileResponse, len(piles))
				for name, count := range piles {
					resp.Piles[name] = PileResponse{Remaining: count}
				}
			}
		}

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
	}
}

func newDeck(workerPool *database.WorkerPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		shuffled := false
		var deck *models.Deck

		if r.URL.Query().Has("cards") {
			cards := r.URL.Query().Get("cards")
			cardsArray := strings.Split(cards, ",")

			if len(cards) > utils.CUSTOM_DECK_CARDS_LIMIT {
				writeError(w, ErrParameterOutOfRange, "")
				return
			}

			// Check for duplicates
			seen := make(map[string]bool)
			for _, card := range cardsArray {
				card = strings.TrimSpace(card)
				if card == "" {
					continue
				}
				if seen[card] {
					writeError(w, ErrDuplicateCards, "")
					return
				}
				seen[card] = true
			}

			var err error
			deck, err = models.NewCustomDeck(cardsArray)
			if err != nil {
				writeError(w, ErrInvalidCardCode, "")
				return
			}
		} else {
			q := r.URL.Query()
			jokers, _ := strconv.ParseBool(q.Get("jokers_enabled"))

			nbDecks := 1
			if v := q.Get("deck_count"); v != "" {
				i, err := strconv.Atoi(v)
				if err != nil || i <= 0 {
					writeError(w, ErrInvalidParameter, "")
					return
				}
				if i > utils.MAX_DECKS {
					writeError(w, ErrParameterOutOfRange, "")
					return
				}
				nbDecks = i
			}
			deck = models.NewMultiDeck(nbDecks, jokers)
		}

		remaining := len(deck.Cards)
		deckId, err := workerPool.InsertDeck(deck)

		if err != nil {
			writeError(w, ErrDatabase, deckId)
			return
		}

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(Response{
			Success:   true,
			DeckId:    deckId,
			Shuffled:  &shuffled,
			Remaining: remaining,
		})
	}
}

func newDeckShuffled(workerPool *database.WorkerPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		shuffled := true
		var deck *models.Deck
		if r.URL.Query().Has("cards") {
			cards := r.URL.Query().Get("cards")
			cardsArray := strings.Split(cards, ",")
			if len(cards) > utils.CUSTOM_DECK_CARDS_LIMIT {
				_ = json.NewEncoder(w).Encode(Response{
					Success:   false,
					DeckId:    "",
					Piles:     nil,
					Cards:     nil,
					Shuffled:  &shuffled,
					Remaining: 0,
					Error:     "Depassement de la limite de cartes",
				})
				return
			}
			var err error
			deck, err = models.NewCustomDeck(cardsArray)
			if err != nil {
				_ = json.NewEncoder(w).Encode(Response{
					Success:   false,
					DeckId:    "",
					Piles:     nil,
					Cards:     nil,
					Shuffled:  &shuffled,
					Remaining: 0,
					Error:     "Code de carte invalide",
				})
				return
			}
		} else {
			q := r.URL.Query()
			jokers, _ := strconv.ParseBool(q.Get("jokers_enabled"))

			nbDecks := 1
			if v := q.Get("deck_count"); v != "" {
				if i, err := strconv.Atoi(v); err == nil && i > 0 {
					nbDecks = min(i, utils.MAX_DECKS)
				}
			}
			deck = models.NewMultiDeck(nbDecks, jokers)
		}

		deck.Shuffle()
		remaining := len(deck.Cards)

		deckId, err := workerPool.InsertDeck(deck)
		resp := Response{
			Success:   err == nil,
			DeckId:    deckId,
			Piles:     nil,
			Cards:     nil,
			Shuffled:  &shuffled,
			Remaining: remaining,
		}
		if err != nil {
			resp.Error = "Echec d'insertion de deck: " + err.Error()
		}
		_ = json.NewEncoder(w).Encode(resp)
	}
}
