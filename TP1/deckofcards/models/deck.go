package models

import (
	"errors"
	"math/rand"
	"strings"
	"time"
)

type Pile struct {
	Cards     map[string]string
	Remaining int
}

type Deck struct {
	Cards     []string //< liste de cartes(identifies par leurs codes)
	Piles     map[string]*Pile
	Remaining int
	NPackets  int //< nombre de packets
	Id        string
}

// NewMultiDeck /** Permet de generer un nouveau deck comportant un a plusieurs decks
// @param number nombre de decks a generer
// @param jokers si on doit generer les jokers
func NewMultiDeck(number int, jokers bool) *Deck {
	deck := new(Deck)
	deck.Cards = []string{}
	values := []string{"A", "2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K"}
	suits := []string{"S", "H", "D", "C"}
	for i := 0; i < number; i++ {
		for _, value := range values {
			for _, suit := range suits {
				deck.Cards = append(deck.Cards, value+suit)
			}
		}
		if jokers {
			deck.Cards = append(deck.Cards, "ZB", "ZR")
		}
	}
	deck.NPackets = number

	return deck
}

// NewCustomDeck /** Permet de generer un nouveau deck avec des cartes specifiques
// @param codes codes de cartes a inserer dans le nouveau deck
func NewCustomDeck(codes []string) (*Deck, error) {
	deck := new(Deck)
	deck.Cards = []string{}
	for _, code := range codes {
		if !CodeValid(code) {
			return nil, errors.New("code de deck invalide")
		}
		deck.Cards = append(deck.Cards, strings.ToUpper(code))
	}
	return deck, nil
}

// Shuffle /** Melange le deck
func (d *Deck) Shuffle() {
	rand.Seed(time.Now().UnixNano())

	for i := range d.Cards {
		j := rand.Intn(i + 1)
		d.Cards[i], d.Cards[j] = d.Cards[j], d.Cards[i]
	}
}
