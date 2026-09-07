package syncz_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/http"
	"strconv"
	"testing"
	"time"

	syncz "github.com/quanturisai-ai/syncz-go"
)

func gerarAssinatura(secret []byte, webhookID string, timestamp int64, corpo []byte) string {
	msg := webhookID + "." + strconv.FormatInt(timestamp, 10) + "."
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(msg))
	mac.Write(corpo)
	return "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func TestJanelaDeTimestamp(t *testing.T) {
	if err := syncz.VerificarJanela(time.Now().UTC().Add(-2*time.Minute), syncz.JanelaWebhookPadrao); err != nil {
		t.Fatalf("2 min atras dentro de ±5 min deveria passar: %v", err)
	}
	if err := syncz.VerificarJanela(time.Now().UTC().Add(2*time.Minute), 0); err != nil {
		t.Fatalf("2 min no futuro com default 5 min deveria passar: %v", err)
	}
	err := syncz.VerificarJanela(time.Now().UTC().Add(-10*time.Minute), 5*time.Minute)
	if !errors.Is(err, syncz.ErrTimestampInvalido) {
		t.Fatalf("10 min atras deveria recusar, obteve %v", err)
	}
	err = syncz.VerificarJanela(time.Now().UTC().Add(10*time.Minute), syncz.JanelaWebhookPadrao)
	if !errors.Is(err, syncz.ErrTimestampInvalido) {
		t.Fatalf("10 min no futuro deveria recusar, obteve %v", err)
	}
}

func TestVerificarWebhook(t *testing.T) {
	secretAtual := []byte("segredo-secreto-atual-123")
	secretAnterior := []byte("segredo-velho-rotacionado-456")
	corpo := []byte(`{"event":"message.received","data":{"text":"ola"}}`)
	webhookID := "msg_event_001"
	agoraUnix := time.Now().UTC().Unix()

	// 1. Assinatura válida com segredo atual
	sigAtual := gerarAssinatura(secretAtual, webhookID, agoraUnix, corpo)
	headers := http.Header{}
	headers.Set("Webhook-Id", webhookID)
	headers.Set("Webhook-Timestamp", strconv.FormatInt(agoraUnix, 10))
	headers.Set("Webhook-Signature", sigAtual)

	if err := syncz.VerificarWebhook(secretAtual, nil, headers, corpo, 5*time.Minute); err != nil {
		t.Fatalf("esperava assinatura valida, erro: %v", err)
	}

	// 2. Assinatura válida com segredo anterior durante rotação (múltiplas assinaturas no header)
	sigVelha := gerarAssinatura(secretAnterior, webhookID, agoraUnix, corpo)
	headersDuplas := http.Header{}
	headersDuplas.Set("webhook-id", webhookID)
	headersDuplas.Set("webhook-timestamp", strconv.FormatInt(agoraUnix, 10))
	headersDuplas.Set("webhook-signature", "v1,invalida "+sigVelha)

	if err := syncz.VerificarWebhook(secretAtual, secretAnterior, headersDuplas, corpo, 5*time.Minute); err != nil {
		t.Fatalf("esperava aceitar segredo anterior em rotacao, erro: %v", err)
	}

	// 3. Assinatura inválida
	headersInvalida := http.Header{}
	headersInvalida.Set("Webhook-Id", webhookID)
	headersInvalida.Set("Webhook-Timestamp", strconv.FormatInt(agoraUnix, 10))
	headersInvalida.Set("Webhook-Signature", "v1,bm9uc2Vuc2U=")

	err := syncz.VerificarWebhook(secretAtual, nil, headersInvalida, corpo, 5*time.Minute)
	if !errors.Is(err, syncz.ErrAssinaturaInvalida) {
		t.Fatalf("esperava ErrAssinaturaInvalida, obteve: %v", err)
	}

	// 4. Timestamp expirado (10 minutos atrás com janela de 5min)
	dezMinAtras := time.Now().UTC().Add(-10 * time.Minute).Unix()
	sigExpirada := gerarAssinatura(secretAtual, webhookID, dezMinAtras, corpo)
	headersExpirada := http.Header{}
	headersExpirada.Set("Webhook-Id", webhookID)
	headersExpirada.Set("Webhook-Timestamp", strconv.FormatInt(dezMinAtras, 10))
	headersExpirada.Set("Webhook-Signature", sigExpirada)

	err = syncz.VerificarWebhook(secretAtual, nil, headersExpirada, corpo, 5*time.Minute)
	if !errors.Is(err, syncz.ErrTimestampInvalido) {
		t.Fatalf("esperava ErrTimestampInvalido para 10 min atras, obteve: %v", err)
	}

	// 5. Timestamp aceito dentro da janela (2 minutos atrás com janela de 5min)
	doisMinAtras := time.Now().UTC().Add(-2 * time.Minute).Unix()
	sigDoisMin := gerarAssinatura(secretAtual, webhookID, doisMinAtras, corpo)
	headersDoisMin := http.Header{}
	headersDoisMin.Set("Webhook-Id", webhookID)
	headersDoisMin.Set("Webhook-Timestamp", strconv.FormatInt(doisMinAtras, 10))
	headersDoisMin.Set("Webhook-Signature", sigDoisMin)

	if err := syncz.VerificarWebhook(secretAtual, nil, headersDoisMin, corpo, 5*time.Minute); err != nil {
		t.Fatalf("esperava aceitar timestamp dentro da janela de 5min, erro: %v", err)
	}

	// 6. Cabeçalhos obrigatórios ausentes
	headersFaltando := http.Header{}
	err = syncz.VerificarWebhook(secretAtual, nil, headersFaltando, corpo, 5*time.Minute)
	if !errors.Is(err, syncz.ErrInvalido) {
		t.Fatalf("esperava ErrInvalido por falta de headers, obteve: %v", err)
	}
}
