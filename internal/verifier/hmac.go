package verifier

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

var (
	ErrInvalidSignature = errors.New("invalid signature: payload tampered or secret mismatch")
	ErrEmptySignature   = errors.New("signature cannot be empty")
)

// VerifyHMACSHA256 يتحقق من صحة التوقيع الرقمي بمقارنة زمنية ثابتة
func VerifyHMACSHA256(payload []byte, signatureHex string, secretKey string) error {
	if signatureHex == "" {
		return ErrEmptySignature
	}

	// 1. حساب بصمة التشفير المتوقعة للبيانات الخام عبر المفتاح المشترك
	mac := hmac.New(sha256.New, []byte(secretKey))
	mac.Write(payload)
	expectedMAC := mac.Sum(nil)

	// 2. تحويل التوقيع القادم من ترويسة الطلب من صيغة Hex إلى بايتات
	actualMAC, err := hex.DecodeString(signatureHex)
	if err != nil {
		return ErrInvalidSignature
	}

	// 3. المقارنة بزمن ثابت لمنع هجمات التوقيت
	if !hmac.Equal(expectedMAC, actualMAC) {
		return ErrInvalidSignature
	}

	return nil
}
