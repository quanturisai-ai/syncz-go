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

func operationClienteREST(t *testing.T, ts *httptest.Server) syncz.Cliente {
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

// TestStatusOperacao_Done cobre o caso feliz: provider-op assincrona que ja
// terminou, com result preenchido -- espelha o JSON real de
// handleGetOperation/toOperationResp (internal/transport/rest/operations.go),
// no formato que create_group devolve.
func TestStatusOperacao_Done(t *testing.T) {
	createdAt := time.Now().UTC().Add(-2 * time.Second).Truncate(time.Second)
	finishedAt := time.Now().UTC().Truncate(time.Second)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/instances/{id}/operations/{op_id}", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.PathValue("id") != "inst-1" || r.PathValue("op_id") != "op-1" {
			t.Errorf("path inesperado: id=%s op_id=%s", r.PathValue("id"), r.PathValue("op_id"))
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"op_id":       "op-1",
			"operation":   "create_group",
			"status":      "done",
			"result":      map[string]any{"jid": "123456789-987654321@g.us", "subject": "Fidelidade", "participants": []string{"5511999998888"}},
			"created_at":  createdAt.Format(time.RFC3339),
			"finished_at": finishedAt.Format(time.RFC3339),
		})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cli := operationClienteREST(t, ts)

	st, err := cli.StatusOperacao(context.Background(), "inst-1", "op-1")
	if err != nil {
		t.Fatalf("StatusOperacao falhou: %v", err)
	}
	if st.OpID != "op-1" || st.Operacao != "create_group" || st.Status != "done" {
		t.Errorf("estado inesperado: %+v", st)
	}
	if !st.CriadaEm.Equal(createdAt) {
		t.Errorf("created_at inesperado: %v (esperava %v)", st.CriadaEm, createdAt)
	}
	if !st.FinalizadaEm.Equal(finishedAt) {
		t.Errorf("finished_at inesperado: %v (esperava %v)", st.FinalizadaEm, finishedAt)
	}
	var result struct {
		JID string `json:"jid"`
	}
	if err := json.Unmarshal(st.Result, &result); err != nil || result.JID != "123456789-987654321@g.us" {
		t.Errorf("result.jid = %q err=%v", result.JID, err)
	}
	if st.Erro != "" {
		t.Errorf("erro deveria vir vazio, obteve %q", st.Erro)
	}
}

// TestStatusOperacao_Pending cobre uma provider-op que ainda nao terminou --
// result e finished_at vem ausentes.
func TestStatusOperacao_Pending(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/instances/{id}/operations/{op_id}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"op_id":      "op-2",
			"operation":  "create_group",
			"status":     "pending",
			"created_at": time.Now().UTC().Format(time.RFC3339),
		})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cli := operationClienteREST(t, ts)

	st, err := cli.StatusOperacao(context.Background(), "inst-1", "op-2")
	if err != nil {
		t.Fatalf("StatusOperacao falhou: %v", err)
	}
	if st.Status != "pending" {
		t.Errorf("status esperado pending, obteve %q", st.Status)
	}
	if st.Result != nil {
		t.Errorf("result deveria vir nil para op pending, obteve %s", st.Result)
	}
	if !st.FinalizadaEm.IsZero() {
		t.Errorf("finished_at deveria vir zero para op pending, obteve %v", st.FinalizadaEm)
	}
}

// TestStatusOperacao_NaoEncontrada cobre 404 de op_id inexistente -- deve
// virar ErrNaoEncontrado, comparavel via errors.Is.
func TestStatusOperacao_NaoEncontrada(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/instances/{id}/operations/{op_id}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{"detail": "operacao nao encontrada"})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cli := operationClienteREST(t, ts)

	_, err := cli.StatusOperacao(context.Background(), "inst-1", "op-inexistente")
	if !errors.Is(err, syncz.ErrNaoEncontrado) {
		t.Fatalf("esperava ErrNaoEncontrado, obteve: %v", err)
	}
}

// TestStatusOperacao_Sandbox cobre o Dublê: CriarGrupo com
// EsperaSincrona{Assincrono:true} devolve um OpID enderecavel via
// StatusOperacao (ja "done", pois o Dublê nao tem provider pra atrasar), e
// op_id desconhecido devolve ErrNaoEncontrado -- mesmo contrato do REST.
func TestStatusOperacao_Sandbox(t *testing.T) {
	sbx := syncz.NovoSandbox()
	defer sbx.Fechar()

	ctx := context.Background()
	g, err := sbx.CriarGrupo(ctx, syncz.EntradaGrupo{
		InstanciaID:   "inst-sbx",
		Assunto:       "Fidelidade",
		Participantes: []string{"5511999998888"},
		EsperaSincrona: syncz.EsperaSincrona{
			Assincrono: true,
		},
	})
	if err != nil {
		t.Fatalf("CriarGrupo falhou: %v", err)
	}
	if g.OpID == "" || g.Status != "accepted" {
		t.Fatalf("esperava grupo accepted com OpID, obteve: %+v", g)
	}

	st, err := sbx.StatusOperacao(ctx, "inst-sbx", g.OpID)
	if err != nil {
		t.Fatalf("StatusOperacao falhou: %v", err)
	}
	if st.Status != "done" || st.Operacao != "create_group" {
		t.Errorf("estado inesperado: %+v", st)
	}
	var result struct {
		JID string `json:"jid"`
	}
	if err := json.Unmarshal(st.Result, &result); err != nil || result.JID == "" {
		t.Errorf("result.jid vazio ou invalido: %q err=%v", result.JID, err)
	}

	_, err = sbx.StatusOperacao(ctx, "inst-sbx", "op-nao-existe")
	if !errors.Is(err, syncz.ErrNaoEncontrado) {
		t.Fatalf("esperava ErrNaoEncontrado, obteve: %v", err)
	}
}

// TestStatusOperacao_GRPCNaoSuportado documenta a decisao da task 566: o
// proto gRPC nao tem RPC equivalente a GET .../operations/{op_id} (os RPCs de
// grupo sempre devolvem o tipo final direto, sem op_id assincrono), entao
// clienteGRPC devolve ErrTransporteNaoSuporta.
func TestStatusOperacao_GRPCNaoSuportado(t *testing.T) {
	cli := syncz.NovoClienteGRPCComConn(nil, func() error { return nil })

	_, err := cli.StatusOperacao(context.Background(), "inst-1", "op-1")
	if !errors.Is(err, syncz.ErrTransporteNaoSuporta) {
		t.Fatalf("esperava ErrTransporteNaoSuporta, obteve: %v", err)
	}
}
