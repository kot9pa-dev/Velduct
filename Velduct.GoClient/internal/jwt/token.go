// Package jwt generates single-use HS256 JWT tokens for WebSocket authentication.
// Client generates token (iss, aud, jti, exp) → passes via ?access_token=<jwt> →
// Server validates signature, audience, expiry, and jti (one-time use).
package jwt

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// TokenOptions for token generation.
type TokenOptions struct {
	Issuer   string        // Client name (iss claim)
	Audience string        // Must match server's JwtSettings.Audience
	Key      string        // HMAC key, must match server's JwtSettings.ServerKeys
	TTL      time.Duration // Token lifetime, recommend 5 minutes
}

// Generate creates a signed HS256 JWT with unique JTI. Each call produces a new token.
func Generate(opts TokenOptions) (string, error) {
	if opts.Key == "" {
		return "", fmt.Errorf("jwt: key must not be empty")
	}
	if opts.Issuer == "" {
		return "", fmt.Errorf("jwt: issuer must not be empty")
	}
	if opts.Audience == "" {
		return "", fmt.Errorf("jwt: audience must not be empty")
	}
	if opts.TTL <= 0 {
		opts.TTL = 5 * time.Minute
	}

	jti, err := newJTI()
	if err != nil {
		return "", fmt.Errorf("jwt: failed to generate jti: %w", err)
	}

	now := time.Now()
	claims := jwt.RegisteredClaims{
		Issuer:    opts.Issuer,
		Audience:  jwt.ClaimStrings{opts.Audience},
		ID:        jti,
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(opts.TTL)),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(opts.Key))
	if err != nil {
		return "", fmt.Errorf("jwt: signing failed: %w", err)
	}

	return signed, nil
}

// newJTI generates a cryptographically random token identifier.
func newJTI() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
