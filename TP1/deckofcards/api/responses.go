package api

type CardResponse struct {
	Code  string `json:"code"`
	Image string `json:"image"`
	Value string `json:"value"`
	Suit  string `json:"suit"`
}

type PileResponse struct {
	Cards     []CardResponse `json:"img,omitempty"`
	Remaining int            `json:"remaining,omitempty"`
}

type Response struct {
	Success   bool                    `json:"success"`
	DeckId    string                  `json:"deck_id"`
	Remaining int                     `json:"remaining,omitempty"`
	Piles     map[string]PileResponse `json:"piles,omitempty"`
	Cards     []CardResponse          `json:"img,omitempty"`
	Shuffled  *bool                   `json:"shuffled,omitempty"`
	Error     string                  `json:"error,omitempty"`
}
