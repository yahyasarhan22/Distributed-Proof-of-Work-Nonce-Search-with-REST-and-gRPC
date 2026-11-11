package pow

import (
	"crypto/sha256"
	"fmt"
	"testing"
)

func TestHashAndDifficulty(t *testing.T) {
	if got := Hash("hello", 0); got != fmt.Sprintf("%x", sha256.Sum256([]byte("hello0"))) {
		t.Fatal("SHA256 mismatch")
	}
	if Hash("", 0) != "5feceb66ffc86f38d952786c6d696c79c2dbc239dd4e91b46729d73a27fb57e9" {
		t.Fatal("decimal zero hash")
	}
	if Hash("1", 23) != Hash("12", 3) {
		t.Fatal("decimal concatenation")
	}
	if !Valid("hello", 28, 1) || Hash("hello", 28) != "02a13c40ba00dc0fb199d3cbe5b01be59d937775890243fd411bdf001935ffc8" {
		t.Fatal("known nonce hello28")
	}
	for _, d := range []int{0, 9, -1} {
		if Valid("hello", 28, d) {
			t.Fatal("invalid difficulty", d)
		}
	}
	if !LeadingZeros([32]byte{}, 8) || LeadingZeros([32]byte{}, 9) {
		t.Fatal("boundaries")
	}
	var h [32]byte
	h[0] = 0x0f
	if !LeadingZeros(h, 1) || LeadingZeros(h, 2) {
		t.Fatal("hexadecimal nibbles, not bits")
	}
}
