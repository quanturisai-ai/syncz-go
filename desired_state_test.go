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

func desiredStateClienteREST(t *testing.T, ts *httptest.Server) syncz.Cliente {
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

// TestAtualizarEstadoDesejado_Connected cobre o caso feliz de reconectar uma
// instância, espelhando o JSON real de handleSetDesiredState/toInstanceResp
// (internal/transport/rest/instances.go, dto.go).
func TestAtualizarEstadoDesejado_Connected(t *testing.T) {
	updatedAt := time.Now().UTC().Truncate(time.Second)

	mux := http.NewServeMux()
	mux.HandleFunc("PATCH /api/v1/instances/{id}", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.PathValue("id") != "inst-1" {
			t.Errorf("path inesperado: id=%s", r.PathValue("id"))
		}
		var body struct {
			DesiredState string `json:"desired_state"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decodificar corpo: %v", err)
		}
		if body.DesiredState != syncz.EstadoDesejadoConectado {
			t.Errorf("desired_state inesperado: %q", body.DesiredState)
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":            "inst-1",
			"tenant_id":     "tenant-1",
			"name":          "principal",
			"phone":         "5511999999999",
			"status":        "connected",
			"desired_state": "connected",
			"created_at":    updatedAt.Add(-time.Hour).Format(time.RFC3339),
			"updated_at":    updatedAt.Format(time.RFC3339),
		})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cli := desiredStateClienteREST(t, ts)

	inst, err := cli.AtualizarEstadoDesejado(context.Background(), "inst-1", syncz.EstadoDesejadoConectado)
	if err != nil {
		t.Fatalf("AtualizarEstadoDesejado falhou: %v", err)
	}
	if inst.ID != "inst-1" || inst.EstadoDesejado != "connected" || inst.Status != "connected" {
		t.Errorf("instancia inesperada: %+v", inst)
	}
	if !inst.AtualizadaEm.Equal(updatedAt) {
		t.Errorf("updated_at inesperado: %v (esperava %v)", inst.AtualizadaEm, updatedAt)
	}
}

// TestAtualizarEstadoDesejado_Disconnected cobre desconexão explícita.
func TestAtualizarEstadoDesejado_Disconnected(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("PATCH /api/v1/instances/{id}", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			DesiredState string `json:"desired_state"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.DesiredState != syncz.EstadoDesejadoDesconectado {
			t.Errorf("desired_state inesperado: %q", body.DesiredState)
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":            "inst-2",
			"tenant_id":     "tenant-1",
			"name":          "secundaria",
			"phone":         "",
			"status":        "disconnected",
			"desired_state": "disconnected",
			"created_at":    time.Now().UTC().Format(time.RFC3339),
			"updated_at":    time.Now().UTC().Format(time.RFC3339),
		})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cli := desiredStateClienteREST(t, ts)

	inst, err := cli.AtualizarEstadoDesejado(context.Background(), "inst-2", syncz.EstadoDesejadoDesconectado)
	if err != nil {
		t.Fatalf("AtualizarEstadoDesejado falhou: %v", err)
	}
	if inst.Status != "disconnected" || inst.EstadoDesejado != "disconnected" {
		t.Errorf("instancia inesperada: %+v", inst)
	}
}

// TestAtualizarEstadoDesejado_NaoEncontrada cobre 404 de instância
// inexistente -- deve virar ErrNaoEncontrado, comparável via errors.Is.
func TestAtualizarEstadoDesejado_NaoEncontrada(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("PATCH /api/v1/instances/{id}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{"detail": "instancia nao encontrada"})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cli := desiredStateClienteREST(t, ts)

	_, err := cli.AtualizarEstadoDesejado(context.Background(), "inst-inexistente", syncz.EstadoDesejadoConectado)
	if !errors.Is(err, syncz.ErrNaoEncontrado) {
		t.Fatalf("esperava ErrNaoEncontrado, obteve: %v", err)
	}
}

// TestAtualizarEstadoDesejado_Sandbox cobre o Dublê: instância criada via
// CriarInstancia aceita a atualização, instância inexistente devolve
// ErrNaoEncontrado e estado inválido devolve ErrInvalido -- mesmo contrato
// do transporte REST, sem tocar rede.
func TestAtualizarEstadoDesejado_Sandbox(t *testing.T) {
	sbx := syncz.NovoSandbox()
	defer sbx.Fechar()

	ctx := context.Background()
	criada, err := sbx.CriarInstancia(ctx, syncz.EntradaCriarInstancia{Nome: "sbx", Telefone: "5511999999999"})
	if err != nil {
		t.Fatalf("CriarInstancia falhou: %v", err)
	}

	inst, err := sbx.AtualizarEstadoDesejado(ctx, criada.ID, syncz.EstadoDesejadoDesconectado)
	if err != nil {
		t.Fatalf("AtualizarEstadoDesejado falhou: %v", err)
	}
	if inst.EstadoDesejado != "disconnected" {
		t.Errorf("estado_desejado inesperado: %+v", inst)
	}

	_, err = sbx.AtualizarEstadoDesejado(ctx, "inst-nao-existe", syncz.EstadoDesejadoConectado)
	if !errors.Is(err, syncz.ErrNaoEncontrado) {
		t.Fatalf("esperava ErrNaoEncontrado, obteve: %v", err)
	}

	_, err = sbx.AtualizarEstadoDesejado(ctx, criada.ID, "estado-invalido")
	if !errors.Is(err, syncz.ErrInvalido) {
		t.Fatalf("esperava ErrInvalido, obteve: %v", err)
	}
}

// TestAtualizarEstadoDesejado_GRPCNaoSuportado documenta a decisao da task
// 570: o proto gRPC nao tem RPC equivalente a PATCH .../instances/{id}, entao
// clienteGRPC devolve ErrTransporteNaoSuporta em vez de inventar um valor.
func TestAtualizarEstadoDesejado_GRPCNaoSuportado(t *testing.T) {
	cli := syncz.NovoClienteGRPCComConn(nil, func() error { return nil })

	_, err := cli.AtualizarEstadoDesejado(context.Background(), "inst-1", syncz.EstadoDesejadoConectado)
	if !errors.Is(err, syncz.ErrTransporteNaoSuporta) {
		t.Fatalf("esperava ErrTransporteNaoSuporta, obteve: %v", err)
	}
}
