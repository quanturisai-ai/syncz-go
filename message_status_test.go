package syncz_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	syncz "github.com/quanturisai-ai/syncz-go"
)

func statusClienteREST(t *testing.T, ts *httptest.Server) syncz.Cliente {
	t.Helper()
	cli, err := syncz.Novo(syncz.Opcoes{
		BaseURL:    ts.URL,
		ChaveAPI:   "secret-token",
		HTTPClient: ts.Client(),
	})
	if err != nil {
		t.Fatalf("syncz.Novo: %v", err)
	}
	t.Cleanup(func() { _ = cli.Fechar() })
	return cli
}

// TestStatusMensagem_Enviada cobre o caso feliz: mensagem que já chegou ao
// destinatario, espelhando o JSON real de handleGetMessageStatus
// (internal/transport/rest/messages.go).
func TestStatusMensagem_Enviada(t *testing.T) {
	sentAt := time.Now().UTC().Truncate(time.Second)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/instances/{id}/messages/{message_id}", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.PathValue("id") != "inst-1" || r.PathValue("message_id") != "msg-1" {
			t.Errorf("path inesperado: id=%s message_id=%s", r.PathValue("id"), r.PathValue("message_id"))
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"message_id":    "msg-1",
			"status":        "sent",
			"wa_message_id": "3EB0C767D",
			"send_attempts": 1,
			"queued_at":     sentAt.Format(time.RFC3339),
			"sent_at":       sentAt.Format(time.RFC3339),
		})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cli := statusClienteREST(t, ts)

	st, err := cli.StatusMensagem(context.Background(), "inst-1", "msg-1")
	if err != nil {
		t.Fatalf("StatusMensagem falhou: %v", err)
	}
	if st.MensagemID != "msg-1" || st.Status != "sent" {
		t.Errorf("estado inesperado: %+v", st)
	}
	if st.WAMensagemID != "3EB0C767D" {
		t.Errorf("wa_message_id inesperado: %q", st.WAMensagemID)
	}
	if st.Tentativas != 1 {
		t.Errorf("send_attempts inesperado: %d", st.Tentativas)
	}
	if !st.EnviadaEm.Equal(sentAt) {
		t.Errorf("sent_at inesperado: %v (esperava %v)", st.EnviadaEm, sentAt)
	}
	if !st.EntregueEm.IsZero() {
		t.Errorf("delivered_at deveria vir zero (mensagem so' sent), obteve %v", st.EntregueEm)
	}
	if st.ErrorCode != "" {
		t.Errorf("error_code deveria vir vazio, obteve %q", st.ErrorCode)
	}
}

// TestStatusMensagem_Falhou cobre mensagem que falhou no envio, com
// error_code/error_message preenchidos.
func TestStatusMensagem_Falhou(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/instances/{id}/messages/{message_id}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"message_id":    "msg-2",
			"status":        "failed",
			"error_code":    "recipient_not_on_whatsapp",
			"error_message": "numero nao possui WhatsApp",
			"send_attempts": 3,
			"queued_at":     time.Now().UTC().Format(time.RFC3339),
		})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cli := statusClienteREST(t, ts)

	st, err := cli.StatusMensagem(context.Background(), "inst-1", "msg-2")
	if err != nil {
		t.Fatalf("StatusMensagem falhou: %v", err)
	}
	if st.Status != "failed" {
		t.Errorf("status esperado failed, obteve %q", st.Status)
	}
	if st.ErrorCode != "recipient_not_on_whatsapp" {
		t.Errorf("error_code inesperado: %q", st.ErrorCode)
	}
	if st.ErrorMessage != "numero nao possui WhatsApp" {
		t.Errorf("error_message inesperado: %q", st.ErrorMessage)
	}
	if st.Tentativas != 3 {
		t.Errorf("send_attempts inesperado: %d", st.Tentativas)
	}
	if !st.EnviadaEm.IsZero() {
		t.Errorf("sent_at deveria vir zero numa falha, obteve %v", st.EnviadaEm)
	}
}

// TestStatusMensagem_NaoEncontrada cobre 404 de mensagem inexistente --
// deve virar ErrNaoEncontrado, comparavel via errors.Is (mesmo padrao de
// erroPorStatusHTTP usado por todo o resto do cliente REST).
func TestStatusMensagem_NaoEncontrada(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/instances/{id}/messages/{message_id}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{"detail": "mensagem nao encontrada"})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cli := statusClienteREST(t, ts)

	_, err := cli.StatusMensagem(context.Background(), "inst-1", "msg-inexistente")
	if !errors.Is(err, syncz.ErrNaoEncontrado) {
		t.Fatalf("esperava ErrNaoEncontrado, obteve: %v", err)
	}
}

// TestStatusMensagem_Sandbox cobre o Dublê: mensagem enviada via Enviar fica
// consultavel em seguida, e mensagem inexistente devolve ErrNaoEncontrado --
// mesmo contrato do transporte REST, sem tocar rede.
func TestStatusMensagem_Sandbox(t *testing.T) {
	sbx := syncz.NovoSandbox()
	defer sbx.Fechar()

	ctx := context.Background()
	rec, err := sbx.Enviar(ctx, syncz.EntradaEnvio{
		InstanciaID:       "inst-sbx",
		Para:              "5511999999999",
		Texto:             "ola",
		ChaveIdempotencia: "idem-1",
	})
	if err != nil {
		t.Fatalf("Enviar falhou: %v", err)
	}

	st, err := sbx.StatusMensagem(ctx, "inst-sbx", rec.MensagemID)
	if err != nil {
		t.Fatalf("StatusMensagem falhou: %v", err)
	}
	if st.MensagemID != rec.MensagemID {
		t.Errorf("mensagem_id inesperado: %+v", st)
	}
	if st.Status == "" {
		t.Errorf("status vazio: %+v", st)
	}

	_, err = sbx.StatusMensagem(ctx, "inst-sbx", "msg-nao-existe")
	if !errors.Is(err, syncz.ErrNaoEncontrado) {
		t.Fatalf("esperava ErrNaoEncontrado, obteve: %v", err)
	}
}

// TestStatusMensagem_GRPCNaoSuportado documenta a decisao da task 565: o
// proto gRPC nao tem RPC equivalente a GET .../messages/{message_id}, entao
// clienteGRPC devolve ErrTransporteNaoSuporta em vez de inventar um valor.
func TestStatusMensagem_GRPCNaoSuportado(t *testing.T) {
	cli := syncz.NovoClienteGRPCComConn(nil, func() error { return nil })

	_, err := cli.StatusMensagem(context.Background(), "inst-1", "msg-1")
	if !errors.Is(err, syncz.ErrTransporteNaoSuporta) {
		t.Fatalf("esperava ErrTransporteNaoSuporta, obteve: %v", err)
	}
}
