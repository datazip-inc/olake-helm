package utils

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/spf13/viper"
)

// getSecretKey returns the AES key bytes and optionally a KMS client.
//
//   - If OLAKE_SECRET_KEY is empty          → encryption disabled (nil key, nil client)
//   - If OLAKE_SECRET_KEY starts with "arn:aws:kms:" → AWS KMS mode
//   - Otherwise                             → local AES-256-GCM (key is SHA-256 of the env value)
func getSecretKey() ([]byte, *kms.Client, error) {
	envKey := viper.GetString(constants.EnvSecretKey)
	if strings.TrimSpace(envKey) == "" {
		return []byte{}, nil, nil // Encryption is disabled
	}

	if strings.HasPrefix(envKey, "arn:aws:kms:") {
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			return nil, nil, fmt.Errorf("failed to load AWS config: %s", err)
		}
		return []byte(envKey), kms.NewFromConfig(cfg), nil
	}

	// Local AES-GCM mode — derive 256-bit key via SHA-256
	hash := sha256.Sum256([]byte(envKey))
	return hash[:], nil, nil
}

// Decrypt decrypts a value that was encrypted by the server's Encrypt function.
// The stored format is a JSON-quoted, base64-encoded ciphertext (nonce prepended for AES-GCM).
// If OLAKE_SECRET_KEY is not set, the raw encryptedText is returned unchanged.
func Decrypt(encryptedText string) (string, error) {
	if strings.TrimSpace(encryptedText) == "" {
		return "", fmt.Errorf("cannot decrypt empty or whitespace-only input")
	}

	key, kmsClient, err := getSecretKey()
	if err != nil || key == nil || len(key) == 0 {
		return encryptedText, err
	}

	// The server stores the ciphertext as a JSON-quoted string (via fmt.Sprintf("%q", ...))
	var decoded string
	if err := json.Unmarshal([]byte(encryptedText), &decoded); err != nil {
		return "", fmt.Errorf("failed to unmarshal JSON string: %s", err)
	}

	encryptedData, err := base64.StdEncoding.DecodeString(decoded)
	if err != nil {
		return "", fmt.Errorf("failed to decode base64 data: %s", err)
	}

	// AWS KMS path
	if kmsClient != nil {
		result, err := kmsClient.Decrypt(context.Background(), &kms.DecryptInput{
			CiphertextBlob: encryptedData,
		})
		if err != nil {
			return "", fmt.Errorf("failed to decrypt with KMS: %s", err)
		}
		return string(result.Plaintext), nil
	}

	// Local AES-256-GCM path
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("failed to create cipher: %s", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("failed to create GCM: %s", err)
	}

	if len(encryptedData) < gcm.NonceSize() {
		return "", errors.New("ciphertext too short")
	}

	plaintext, err := gcm.Open(nil, encryptedData[:gcm.NonceSize()], encryptedData[gcm.NonceSize():], nil)
	if err != nil {
		return "", fmt.Errorf("failed to decrypt: %s", err)
	}
	return string(plaintext), nil
}
