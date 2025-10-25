package api

import (
	"encoding/json"
	"errors"
	"net/http"
)

// Error types for consistent error handling
var (
	ErrDeckNotFound   = errors.New("deck not found")
	ErrNotEnoughCards = errors.New("not enough cards remaining")
	ErrDeckEmpty      = errors.New("deck is empty")

	ErrInvalidCardCode = errors.New("invalid card code")
	ErrCardNotInPile   = errors.New("card not found in pile")
	ErrDuplicateCards  = errors.New("duplicate cards in request")
	ErrCardNotInDeck   = errors.New("card not found in deck")

	ErrPileNotFound = errors.New("pile not found")
	ErrPileEmpty    = errors.New("pile is empty")

	ErrDatabase       = errors.New("database error")
	ErrRequestTimeout = errors.New("request timeout")
	ErrConcurrentMod  = errors.New("concurrent modification detected")

	ErrInvalidParameter    = errors.New("invalid parameter")
	ErrParameterOutOfRange = errors.New("parameter out of range")
)

// ErrorResponse represents structured error information
type ErrorResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error"`
	DeckId  string `json:"deck_id,omitempty"`
}

// getHTTPStatus returns the appropriate HTTP status code for an error
func getHTTPStatus(err error) int {
	switch {
	case errors.Is(err, ErrDeckNotFound):
		return http.StatusNotFound
	case errors.Is(err, ErrPileNotFound):
		return http.StatusNotFound
	case errors.Is(err, ErrCardNotInDeck):
		return http.StatusNotFound
	case errors.Is(err, ErrCardNotInPile):
		return http.StatusNotFound

	case errors.Is(err, ErrInvalidCardCode):
		return http.StatusBadRequest
	case errors.Is(err, ErrDuplicateCards):
		return http.StatusBadRequest
	case errors.Is(err, ErrInvalidParameter):
		return http.StatusBadRequest
	case errors.Is(err, ErrParameterOutOfRange):
		return http.StatusBadRequest
	case errors.Is(err, ErrNotEnoughCards):
		return http.StatusBadRequest
	case errors.Is(err, ErrDeckEmpty):
		return http.StatusBadRequest
	case errors.Is(err, ErrPileEmpty):
		return http.StatusBadRequest

	case errors.Is(err, ErrRequestTimeout):
		return http.StatusServiceUnavailable
	case errors.Is(err, ErrConcurrentMod):
		return http.StatusConflict

	case errors.Is(err, ErrDatabase):
		return http.StatusInternalServerError

	default:
		return http.StatusInternalServerError
	}
}

// writeError writes a standardized error response
func writeError(w http.ResponseWriter, err error, deckId string) {
	status := getHTTPStatus(err)
	w.WriteHeader(status)

	if errors.Is(err, ErrDeckNotFound) {
		deckId = "invalid_id"
	}

	response := ErrorResponse{
		Success: false,
		DeckId:  deckId,
		Error:   err.Error(),
	}

	_ = json.NewEncoder(w).Encode(response)
}
