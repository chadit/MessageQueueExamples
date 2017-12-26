// Package crypto provides AES encyption and decryption with safe defaults,
// as well as an implementation to the FNV non-cryptographic hash function.
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"hash/fnv"
	"io"
)

// Config is a set of configuration variables.
type Config struct {
	Key string `json:",omitempty"`
}

// T holds all methods related to cryptography.
type T struct {
	config Config
}

// ErrInvalidInput indicates invalid input.
var ErrInvalidInput = errors.New("no input string provided")

// New returns a pointer to a new T value.
func New(c Config) (*T, error) {
	t := T{config: c}
	return &t, nil
}

// Encrypt accepts a byte slice and returns an AES-encoded byte slice.
func (t *T) Encrypt(in []byte) ([]byte, error) {
	b := make([]byte, aes.BlockSize+len(in))
	k := []byte(t.config.Key)
	a, err := aes.NewCipher(k)
	if err != nil {
		return b, err
	}

	v := b[:aes.BlockSize]
	_, err = io.ReadFull(rand.Reader, v)
	if err != nil {
		return b, err
	}

	n := cipher.NewCFBEncrypter(a, v)
	n.XORKeyStream(b[aes.BlockSize:], in)
	return b, nil
}

// Decrypt accepts an AES-encoded byte slice and returns a decrypted byte slice.
func (t *T) Decrypt(in []byte) ([]byte, error) {
	k := []byte(t.config.Key)
	n, err := aes.NewCipher(k)
	if err != nil {
		return nil, err
	}

	b := in[aes.BlockSize:]
	d := cipher.NewCFBDecrypter(n, in[:aes.BlockSize])
	d.XORKeyStream(b, b)
	return b, nil
}

// HashFNV1a32 accepts a string and returns a 32 bit hash based off FNV-1a, non-cryptographic hash function.
func HashFNV1a32(s string) uint32 {
	h := fnv.New32a()
	if _, err := h.Write([]byte(s)); err != nil {
		return 0 // This will never occur under current Write implementation,  https://github.com/golang/go/blob/master/src/hash/fnv/fnv.go#L84
	}
	return h.Sum32()
}

// HashFNV1a64 accepts a string and returns a 64 bit hash based off FNV-1a, non-cryptographic hash function.
func HashFNV1a64(s string) uint64 {
	h := fnv.New64a()
	if _, err := h.Write([]byte(s)); err != nil {
		return 0 // This will never occur under current Write implementation,  https://github.com/golang/go/blob/master/src/hash/fnv/fnv.go#L104
	}
	return h.Sum64()
}
