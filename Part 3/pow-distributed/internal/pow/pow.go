// Package pow implements SHA-256(data + base-10 nonce), with hex-zero difficulty.
package pow

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
)

func Sum(data string, nonce uint64) [32]byte {
	return sha256.Sum256(strconv.AppendUint([]byte(data), nonce, 10))
}
func Hash(data string, nonce uint64) string {
	h := Sum(data, nonce)
	return hex.EncodeToString(h[:])
}
func LeadingZeros(h [32]byte, difficulty int) bool {
	if difficulty < 1 || difficulty > 8 {
		return false
	}
	for i := 0; i < difficulty/2; i++ {
		if h[i] != 0 {
			return false
		}
	}
	return difficulty%2 == 0 || h[difficulty/2]>>4 == 0
}
func Valid(data string, nonce uint64, difficulty int) bool {
	return LeadingZeros(Sum(data, nonce), difficulty)
}
