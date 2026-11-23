package main

import (
	"fmt"
	"io"
	"net/http"
	"sync"
)

var (
	mu          sync.RWMutex
	shouldError = true // الافتراضي: محاكاة خادم معطل
)

func main() {
	// مسار استقبال الـ Webhook
	http.HandleFunc("/webhook-receiver", func(w http.ResponseWriter, r *http.Request) {
		mu.RLock()
		isErr := shouldError
		mu.RUnlock()

		body, _ := io.ReadAll(r.Body)

		if isErr {
			fmt.Printf(" [Target Server]  فشل متعمد (500 Internal Server Error)! البيانات: %s\n", string(body))
			http.Error(w, "Target database down", http.StatusInternalServerError)
			return
		}

		fmt.Printf(" [Target Server]  استلام ناجح (200 OK)! البيانات: %s\n", string(body))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"received":true}`))
	})

	// مسار تبديل حالة السيرفر الوهمي للنجاح
	http.HandleFunc("/toggle-fix", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		shouldError = false
		mu.Unlock()
		fmt.Println(" [Target Server]  تم إصلاح السيرفر الوهمي! الآن يستجيب بـ 200 OK.")
		w.Write([]byte(`{"status":"mock server fixed"}`))
	})

	fmt.Println(" الخادم الوهمي يستمع على المنفذ 9000 (وضع الفشل الافتراضي مفعل)...")
	http.ListenAndServe(":9000", nil)
}
