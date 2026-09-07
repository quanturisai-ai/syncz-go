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

func TestClienteREST(t *testing.T) {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /api/v1/instances", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret-token" {
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]any{"detail": "token invalido"})
			return
		}
		var req map[string]string
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req["name"] == "erro" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"detail": "nome invalido"})
			return
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":         "inst-rest-1",
			"tenant_id":  "ten-1",
			"name":       req["name"],
			"phone":      req["phone"],
			"status":     "connected",
			"created_at": time.Now().UTC().Format(time.RFC3339),
			"updated_at": time.Now().UTC().Format(time.RFC3339),
		})
	})

	mux.HandleFunc("GET /api/v1/instances/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if id == "nao-existe" {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"detail": "instancia inexistente"})
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":     id,
			"name":   "teste-get",
			"status": "connected",
		})
	})

	mux.HandleFunc("POST /api/v1/instances/{id}/messages", func(w http.ResponseWriter, r *http.Request) {
		idem := r.Header.Get("Idempotency-Key")
		if idem == "dup-key" {
			w.WriteHeader(http.StatusOK) // 200 = duplicate
			_ = json.NewEncoder(w).Encode(map[string]any{
				"message_id": "msg-dup-rest",
				"status":     "delivered",
			})
			return
		}
		if idem == "erro-key" {
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]any{"detail": "conflito de chave"})
			return
		}
		w.WriteHeader(http.StatusAccepted) // 202 = accepted
		_ = json.NewEncoder(w).Encode(map[string]any{
			"message_id": "msg-new-rest",
			"status":     "accepted",
		})
	})

	var ultimoWaitQuery string
	var houveWaitQuery bool
	mux.HandleFunc("POST /api/v1/instances/{id}/groups", func(w http.ResponseWriter, r *http.Request) {
		ultimoWaitQuery, houveWaitQuery = r.URL.Query().Get("wait"), r.URL.Query().Has("wait")
		if !houveWaitQuery {
			// Sem ?wait=, o servidor de verdade cai no caminho assincrono e
			// devolve 202 com jid vazio -- o mesmo bug do relato ao vivo
			// (task 349). O fake replica esse comportamento para o teste
			// pegar uma regressao onde o SDK pare de mandar wait por padrao.
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "accepted", "op_id": "op-async-rest-1"})
			return
		}
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jid":          "120363888@g.us",
			"subject":      req["subject"],
			"participants": req["participants"],
			"owner_jid":    "5511999999999@s.whatsapp.net",
		})
	})

	mux.HandleFunc("GET /api/v1/instances/{id}/pairing", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"qr_code": "2@qr-code-data",
			"seq":     1,
			"paired":  false,
		})
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	cli, err := syncz.Novo(syncz.Opcoes{
		BaseURL:    ts.URL,
		ChaveAPI:   "secret-token",
		HTTPClient: ts.Client(),
	})
	if err != nil {
		t.Fatalf("Novo falhou: %v", err)
	}
	defer cli.Fechar()

	ctx := context.Background()

	// 1. Criar Instancia
	inst, err := cli.CriarInstancia(ctx, syncz.EntradaCriarInstancia{Nome: "inst-zap", Telefone: "5511988888888"})
	if err != nil {
		t.Fatalf("CriarInstancia falhou: %v", err)
	}
	if inst.ID != "inst-rest-1" || inst.Nome != "inst-zap" {
		t.Errorf("instancia inesperada: %+v", inst)
	}

	// 2. Erro 400 -> ErrInvalido
	_, err = cli.CriarInstancia(ctx, syncz.EntradaCriarInstancia{Nome: "erro"})
	if !errors.Is(err, syncz.ErrInvalido) {
		t.Errorf("esperava ErrInvalido, obteve: %v", err)
	}

	// 3. Obter Instancia
	got, err := cli.ObterInstancia(ctx, "inst-123")
	if err != nil {
		t.Fatalf("ObterInstancia falhou: %v", err)
	}
	if got.ID != "inst-123" {
		t.Errorf("esperava id inst-123, obteve %s", got.ID)
	}

	// 4. Erro 404 -> ErrNaoEncontrado
	_, err = cli.ObterInstancia(ctx, "nao-existe")
	if !errors.Is(err, syncz.ErrNaoEncontrado) {
		t.Errorf("esperava ErrNaoEncontrado, obteve: %v", err)
	}

	// 5. Envio de mensagem nova (202 -> Duplicada = false)
	rec, err := cli.Enviar(ctx, syncz.EntradaEnvio{
		InstanciaID:       "inst-rest-1",
		Para:              "5511999999999",
		Texto:             "ola rest",
		ChaveIdempotencia: "nova-chave",
	})
	if err != nil {
		t.Fatalf("Enviar falhou: %v", err)
	}
	if rec.MensagemID != "msg-new-rest" || rec.Duplicada {
		t.Errorf("recibo inesperado: %+v", rec)
	}

	// 6. Envio duplicado (200 -> Duplicada = true)
	recDup, err := cli.Enviar(ctx, syncz.EntradaEnvio{
		InstanciaID:       "inst-rest-1",
		Para:              "5511999999999",
		Texto:             "ola rest",
		ChaveIdempotencia: "dup-key",
	})
	if err != nil {
		t.Fatalf("Enviar dup falhou: %v", err)
	}
	if !recDup.Duplicada {
		t.Errorf("esperava Duplicada=true, obteve: %+v", recDup)
	}

	// 7. Envio com erro de conflito (409 -> ErrConflito)
	_, err = cli.Enviar(ctx, syncz.EntradaEnvio{
		InstanciaID:       "inst-rest-1",
		ChaveIdempotencia: "erro-key",
	})
	if !errors.Is(err, syncz.ErrConflito) {
		t.Errorf("esperava ErrConflito, obteve: %v", err)
	}

	// 8. Estado pareamento
	par, err := cli.EstadoPareamento(ctx, "inst-rest-1")
	if err != nil {
		t.Fatalf("EstadoPareamento falhou: %v", err)
	}
	if par.QRCode != "2@qr-code-data" {
		t.Errorf("QR code inesperado: %s", par.QRCode)
	}

	// 9. CriarGrupo por padrao (Assincrono=false, zero-value) PEDE ?wait= --
	// task 349: antes desta correcao rest.go nunca mandava ?wait= nenhum, e o
	// servidor real (internal/transport/rest/groups.go) cai sempre no caminho
	// assincrono sem ele, devolvendo JID vazio. O fake acima replica esse
	// comportamento: so' devolve JID de verdade quando ?wait= esta presente.
	grupo, err := cli.CriarGrupo(ctx, syncz.EntradaGrupo{
		InstanciaID:   "inst-rest-1",
		Assunto:       "Grupo Teste",
		Participantes: []string{"5511988887777"},
	})
	if err != nil {
		t.Fatalf("CriarGrupo falhou: %v", err)
	}
	if !houveWaitQuery {
		t.Fatal("REGRESSAO (task 349): CriarGrupo por padrao nao mandou ?wait= ao servidor")
	}
	if ultimoWaitQuery == "" {
		t.Error("esperava ?wait= com uma duracao, veio vazio")
	}
	if grupo.JID == "" {
		t.Errorf("esperava JID preenchido com wait, veio vazio (status=%q op_id=%q)", grupo.Status, grupo.OpID)
	}

	// 10. CriarGrupo com Assincrono=true explicito: sem ?wait=, e o chamador
	// enxerga o recibo de aceite (Status/OpID) em vez de um JID vazio silencioso.
	grupoAsync, err := cli.CriarGrupo(ctx, syncz.EntradaGrupo{
		InstanciaID:    "inst-rest-1",
		Assunto:        "Grupo Assincrono",
		Participantes:  []string{"5511988887777"},
		EsperaSincrona: syncz.EsperaSincrona{Assincrono: true},
	})
	if err != nil {
		t.Fatalf("CriarGrupo assincrono falhou: %v", err)
	}
	if houveWaitQuery {
		t.Fatal("Assincrono=true deveria omitir ?wait=")
	}
	if grupoAsync.Status != "accepted" || grupoAsync.OpID == "" {
		t.Errorf("esperava recibo de aceite (status=accepted, op_id preenchido), obteve: %+v", grupoAsync)
	}
}
