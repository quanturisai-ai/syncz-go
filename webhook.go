package syncz

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var (
	// ErrAssinaturaInvalida indica que a assinatura do webhook não confere com nenhum dos segredos.
	ErrAssinaturaInvalida = errors.New("syncz: assinatura de webhook invalida")
	// ErrTimestampInvalido indica que o timestamp do webhook está fora da janela de tolerância.
	ErrTimestampInvalido = errors.New("syncz: timestamp de webhook fora da janela permitida")
)

// JanelaWebhookPadrao e a tolerancia simetrica do Webhook-Timestamp (D-4.9).
// O mesmo valor default vive no gateway (internal/webhook.JanelaPadrao).
const JanelaWebhookPadrao = 5 * time.Minute

// VerificarJanela recusa envelope cujo timestamp dista mais que janela do
// relogio local. janela <= 0 usa JanelaWebhookPadrao (5 minutos).
func VerificarJanela(ts time.Time, janela time.Duration) error {
	if janela <= 0 {
		janela = JanelaWebhookPadrao
	}
	diff := time.Now().UTC().Sub(ts.UTC())
	if diff < 0 {
		diff = -diff
	}
	if diff > janela {
		return novoErroAPI(ErrTimestampInvalido, fmt.Sprintf("timestamp dista %v do relogio local (janela: %v)", diff.Round(time.Second), janela))
	}
	return nil
}

// ObterCabecalho busca valor de cabeçalho tolerando diferentes formatos de casing.
func obterCabecalho(h http.Header, chaves ...string) string {
	for _, c := range chaves {
		if v := h.Get(c); v != "" {
			return v
		}
	}
	// Fallback para iteração caso o Header não esteja normalizado
	for k, vals := range h {
		for _, c := range chaves {
			if strings.EqualFold(k, c) && len(vals) > 0 {
				return vals[0]
			}
		}
	}
	return ""
}

// VerificarWebhook confere a assinatura Standard Webhooks e a janela de timestamp.
// Suporta rotação de segredos (segredoAtual e segredoAnterior).
func VerificarWebhook(segredoAtual, segredoAnterior []byte, cabecalhos http.Header, corpo []byte, janela time.Duration) error {
	if len(segredoAtual) == 0 && len(segredoAnterior) == 0 {
		return novoErroAPI(ErrConfiguracao, "ao menos um segredo deve ser fornecido para verificacao")
	}

	webhookID := obterCabecalho(cabecalhos, "Webhook-Id", "webhook-id")
	if webhookID == "" {
		return novoErroAPI(ErrInvalido, "cabecalho Webhook-Id ausente")
	}

	tsStr := obterCabecalho(cabecalhos, "Webhook-Timestamp", "webhook-timestamp")
	if tsStr == "" {
		return novoErroAPI(ErrInvalido, "cabecalho Webhook-Timestamp ausente")
	}

	unixTs, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return novoErroAPI(ErrInvalido, "formato de Webhook-Timestamp invalido")
	}

	ts := time.Unix(unixTs, 0)
	if err := VerificarJanela(ts, janela); err != nil {
		return err
	}

	sigHeader := obterCabecalho(cabecalhos, "Webhook-Signature", "webhook-signature")
	if sigHeader == "" {
		return novoErroAPI(ErrCredencial, "cabecalho Webhook-Signature ausente")
	}

	prefixoMensagem := webhookID + "." + strconv.FormatInt(unixTs, 10) + "."

	// Assinaturas enviadas podem conter múltiplas versões separadas por espaço (ex.: "v1,<sig1> v1,<sig2>")
	partes := strings.Fields(sigHeader)
	var assinaturasRecebidas [][]byte

	for _, parte := range partes {
		if strings.HasPrefix(parte, "v1,") {
			rawB64 := strings.TrimPrefix(parte, "v1,")
			decodificado, err := base64.StdEncoding.DecodeString(rawB64)
			if err == nil {
				assinaturasRecebidas = append(assinaturasRecebidas, decodificado)
			}
		}
	}

	if len(assinaturasRecebidas) == 0 {
		return novoErroAPI(ErrAssinaturaInvalida, "nenhuma assinatura v1 valida encontrada no cabecalho")
	}

	// Testa contra o segredo atual
	if len(segredoAtual) > 0 {
		mac := hmac.New(sha256.New, segredoAtual)
		mac.Write([]byte(prefixoMensagem))
		mac.Write(corpo)
		esperado := mac.Sum(nil)

		for _, recebido := range assinaturasRecebidas {
			if hmac.Equal(recebido, esperado) {
				return nil
			}
		}
	}

	// Testa contra o segredo anterior durante janela de rotação
	if len(segredoAnterior) > 0 {
		mac := hmac.New(sha256.New, segredoAnterior)
		mac.Write([]byte(prefixoMensagem))
		mac.Write(corpo)
		esperado := mac.Sum(nil)

		for _, recebido := range assinaturasRecebidas {
			if hmac.Equal(recebido, esperado) {
				return nil
			}
		}
	}

	return novoErroAPI(ErrAssinaturaInvalida, "assinatura nao corresponde a nenhum dos segredos fornecidos")
}
