// Package aesbnc 对齐公司封面加密: AES-128-ECB + PKCS7, 密钥与 AesUtil::decryptRaw 相同。
package aesbnc

import (
	"crypto/aes"
	"errors"
)

const Key = "525202f9149e061d"

func Encrypt(plain []byte) ([]byte, error) {
	if plain == nil {
		plain = []byte{}
	}
	block, err := aes.NewCipher([]byte(Key))
	if err != nil {
		return nil, err
	}
	padded := pkcs7Pad(plain, aes.BlockSize)
	out := make([]byte, len(padded))
	for i := 0; i < len(padded); i += aes.BlockSize {
		block.Encrypt(out[i:i+aes.BlockSize], padded[i:i+aes.BlockSize])
	}
	return out, nil
}

func pkcs7Pad(b []byte, n int) []byte {
	pad := n - (len(b) % n)
	out := make([]byte, len(b)+pad)
	copy(out, b)
	for i := len(b); i < len(out); i++ {
		out[i] = byte(pad)
	}
	return out
}

func Decrypt(data []byte) ([]byte, error) {
	if len(data) == 0 || len(data)%aes.BlockSize != 0 {
		return nil, errors.New("密文长度无效")
	}
	block, err := aes.NewCipher([]byte(Key))
	if err != nil {
		return nil, err
	}
	out := make([]byte, len(data))
	for i := 0; i < len(data); i += aes.BlockSize {
		block.Decrypt(out[i:i+aes.BlockSize], data[i:i+aes.BlockSize])
	}
	return pkcs7Unpad(out, aes.BlockSize)
}

func pkcs7Unpad(b []byte, n int) ([]byte, error) {
	if len(b) == 0 || len(b)%n != 0 {
		return nil, errors.New("填充无效")
	}
	pad := int(b[len(b)-1])
	if pad == 0 || pad > n || pad > len(b) {
		return nil, errors.New("填充无效")
	}
	for i := len(b) - pad; i < len(b); i++ {
		if b[i] != byte(pad) {
			return nil, errors.New("填充无效")
		}
	}
	return b[:len(b)-pad], nil
}
