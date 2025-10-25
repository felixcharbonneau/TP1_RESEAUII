package models

import "testing"

func TestGetValue(t *testing.T) {
	tests := []struct {
		code    string
		want    string
		wantErr bool
	}{
		{"AS", "1", false},
		{"10H", "10", false},
		{"QD", "REINE", false},
		{"Z", "", true},
		{"ZB", "JOKER", false},
		{"ZC", "", true},
		{"", "", true},
		{"XS", "", true},
		{"AHH", "", true},
	}
	for _, tt := range tests {
		got, err := GetValue(tt.code)
		if (err != nil) != tt.wantErr {
			t.Fatalf("GetValue(%q) error = %v, wantErr=%v", tt.code, err, tt.wantErr)
		}
		if err == nil && got != tt.want {
			t.Fatalf("GetValue(%q) = %q, want %q", tt.code, got, tt.want)
		}
	}
}
func TestGetSuit(t *testing.T) {
	tests := []struct {
		code    string
		want    string
		wantErr bool
	}{
		{"AS", "PIQUE", false},
		{"10H", "COEUR", false},
		{"QD", "CARREAU", false},
		{"KC", "TREFLE", false},
		{"Z", "", true},   // Joker => erreur
		{"A", "", true},   // taille invalide
		{"10X", "", true}, // couleur invalide
		{"", "", true},    // vide
		{"AHH", "", true}, // 3 chars mais pas "10?"
	}
	for _, tt := range tests {
		got, err := GetSuit(tt.code)
		if (err != nil) != tt.wantErr {
			t.Fatalf("GetSuit(%q) error = %v, wantErr=%v", tt.code, err, tt.wantErr)
		}
		if err == nil && got != tt.want {
			t.Fatalf("GetSuit(%q) = %q, want %q", tt.code, got, tt.want)
		}
	}
}
func TestCodeValid(t *testing.T) {
	tests := []struct {
		code string
		want bool
	}{
		{"AS", true},
		{"10H", true},
		{"QD", true},
		{"KC", true},
		{"Z", false},
		{"A", false},
		{"10X", false},
		{"", false},
		{"AHH", false},
		{"AB", false},
		{"ZC", false},
		{"ZB", true},
	}
	for _, tt := range tests {
		got := CodeValid(tt.code)
		if got != tt.want {
			t.Fatalf("CodeValid(%q) = %t, want %t", tt.code, got, tt.want)
		}
	}
}
