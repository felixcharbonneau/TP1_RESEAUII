package models

import (
	"errors"
	"strings"
)

var values = map[string]string{
	"A":  "1",
	"2":  "2",
	"3":  "3",
	"4":  "4",
	"5":  "5",
	"6":  "6",
	"7":  "7",
	"8":  "8",
	"9":  "9",
	"10": "10",
	"J":  "VALET",
	"Q":  "REINE",
	"K":  "ROI",
	"Z":  "JOKER",
}
var suits = map[string]string{
	"S": "PIQUE",
	"H": "COEUR",
	"D": "CARREAU",
	"C": "TREFLE",
}

func CodeValid(code string) bool {
	switch len(code) {
	case 2:
		if _, ok := values[string(code[0])]; !ok {
			return false
		}
		if code[0] == 'Z' {
			return code[1] == 'R' || code[1] == 'B'
		}
		_, ok := suits[string(code[1])]
		return ok
	case 3:
		if code[0:2] == "10" {
			if val, ok := suits[string(code[2])]; ok {
				return val != "R" && val != "B"
			}
			return false
		}
	}
	return false
}

// GetValue /** Permet d'obtenir la valeur textuelle d'une carte depuis son code
func GetValue(code string) (string, error) {
	code = strings.ToUpper(strings.TrimSpace(code))
	if !CodeValid(code) {
		return "", errors.New("code invalide")
	}
	key := ""
	if len(code) == 3 {
		key = code[0:2]
	} else {
		key = string(code[0])
	}
	value, ok := values[key]
	if !ok {
		return "", errors.New("code invalide")
	}
	return value, nil
}

// GetSuit /** Permet d'obtenir la couleur d'une carte depuis son code
func GetSuit(code string) (string, error) {
	code = strings.ToUpper(strings.TrimSpace(code))
	if !CodeValid(code) {
		return "", errors.New("code invalide")
	}
	var key string
	if len(code) == 3 {
		key = string(code[2])
	} else {
		key = string(code[1])
	}
	suit, ok := suits[key]
	if !ok {
		return "", errors.New("Couleur invalide: " + key)
	}
	return suit, nil
}
