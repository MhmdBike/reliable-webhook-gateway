package verifier

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestVerifyHMACSHA256(t *testing.T) {
	// البيانات المرجعية للاختبار
	secret := "dev_shared_secret_key_12345"
	validPayload := []byte(`{"event":"order.completed","amount":999}`)

	// توليد توقيع سليم مسبقاً لمقارنته
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(validPayload)
	validSignature := hex.EncodeToString(mac.Sum(nil))

	// جدول السيناريوهات (Table-Driven Cases)
	testCases := []struct {
		name          string // اسم السيناريو
		payload       []byte // البيانات المرسلة
		signatureHex  string // التوقيع المرسل
		secretKey     string // المفتاح المستخدم
		expectedError error  // النتيجة المتوقعة (nil في حال النجاح)
	}{
		{
			name:          "حالة ناجحة: بيانات وتوقيع ومفتاح متطابقين تماماً",
			payload:       validPayload,
			signatureHex:  validSignature,
			secretKey:     secret,
			expectedError: nil,
		},
		{
			name:          "فشل: حمولة تم التلاعب بمحتواها (Tampered Payload)",
			payload:       []byte(`{"event":"order.completed","amount":1000}`),
			signatureHex:  validSignature,
			secretKey:     secret,
			expectedError: ErrInvalidSignature,
		},
		{
			name:          "فشل: مفتاح سري غير متطابق (Wrong Secret)",
			payload:       validPayload,
			signatureHex:  validSignature,
			secretKey:     "wrong_secret_key",
			expectedError: ErrInvalidSignature,
		},
		{
			name:          "فشل: توقيع فارغ (Empty Signature)",
			payload:       validPayload,
			signatureHex:  "",
			secretKey:     secret,
			expectedError: ErrEmptySignature,
		},
		{
			name:          "فشل: توقيع بصيغة غير صالحة (Corrupted Hex)",
			payload:       validPayload,
			signatureHex:  "invalid_hex_string_xyz",
			secretKey:     secret,
			expectedError: ErrInvalidSignature,
		},
	}

	// تنفيذ الفحص عبر تكرار السيناريوهات
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := VerifyHMACSHA256(tc.payload, tc.signatureHex, tc.secretKey)

			if tc.expectedError == nil && err != nil {
				t.Fatalf("كان من المتوقع نجاح العملية، لكن ظهر خطأ: %v", err)
			}

			if tc.expectedError != nil && err != tc.expectedError {
				t.Fatalf("الخطأ المستلم: [%v] لا يطابق الخطأ المتوقع: [%v]", err, tc.expectedError)
			}
		})
	}
}
