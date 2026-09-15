package syncz_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// sseEventoData escreve um bloco SSE sem "event:" (o formato real de GET
// /events/stream -- ver internal/transport/sse/stream.go, Stream.Send com
// name="") e da flush imediato.
func sseEventoData(w http.ResponseWriter, data string) {
	fmt.Fprintf(w, "data: %s\n\n", data)
	w.(http.Flusher).Flush()
}

func eventosStreamCliente(t *testing.T, ts *httptest.Server) syncz.ClienteEventosStream {
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

	streamer, ok := cli.(syncz.ClienteEventosStream)
	if !ok {
		t.Fatalf("cliente REST nao implementa ClienteEventosStream")
	}
	return streamer
}

// TestStreamEventos_InstanceIDObrigatorio cobre a validacao client-side:
// instance_id ausente falha sem sequer abrir a conexao HTTP (o handler falha
// o teste se for chamado).
func TestStreamEventos_InstanceIDObrigatorio(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/events/stream", func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("nao deveria chamar o servidor sem instance_id")
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	streamer := eventosStreamCliente(t, ts)

	eventos, err := streamer.AcompanharEventos(context.Background(), "", 0)
	if err == nil {
		t.Fatalf("esperava erro, recebi eventos=%v", eventos)
	}
	if !errors.Is(err, syncz.ErrInvalido) {
		t.Fatalf("esperava ErrInvalido, recebi: %v", err)
	}
	if eventos != nil {
		t.Fatalf("esperava canal nil no erro, recebi %v", eventos)
	}
}

// TestStreamEventos_ReplayOrdemSemGap cobre o caso central: uma sequencia de
// eventos (seq 1,2,3) chega no canal na mesma ordem e sem gap/repeticao (CA-23
// -- docs/homologacao/staging-2026-09.md), com from_seq aplicado corretamente
// na query string.
func TestStreamEventos_ReplayOrdemSemGap(t *testing.T) {
	var instanceIDRecebido, fromSeqRecebido string
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/events/stream", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		instanceIDRecebido = r.URL.Query().Get("instance_id")
		fromSeqRecebido = r.URL.Query().Get("from_seq")
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		sseEventoData(w, `{"id":"ev-2","type":"message.received","instance_id":"inst-1","chat_jid":"5511@s.whatsapp.net","seq":2,"occurred_at":"2026-01-01T00:00:02Z","payload":{"body":"oi"},"trace_id":"tr-2"}`)
		sseEventoData(w, `{"id":"ev-3","type":"message.received","instance_id":"inst-1","chat_jid":"5511@s.whatsapp.net","seq":3,"occurred_at":"2026-01-01T00:00:03Z","payload":{"body":"tudo bem"},"trace_id":"tr-3"}`)
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	streamer := eventosStreamCliente(t, ts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	eventos, err := streamer.AcompanharEventos(ctx, "inst-1", 2)
	if err != nil {
		t.Fatalf("AcompanharEventos: %v", err)
	}

	var recebidos []syncz.EventoStream
	for ev := range eventos {
		recebidos = append(recebidos, ev)
	}

	if instanceIDRecebido != "inst-1" {
		t.Fatalf("instance_id na query = %q, esperado inst-1", instanceIDRecebido)
	}
	if fromSeqRecebido != "2" {
		t.Fatalf("from_seq na query = %q, esperado 2", fromSeqRecebido)
	}

	if len(recebidos) != 2 {
		t.Fatalf("esperava 2 eventos, recebi %d: %+v", len(recebidos), recebidos)
	}
	if recebidos[0].Err != nil || recebidos[0].Evento.Seq != 2 || recebidos[0].Evento.ID != "ev-2" {
		t.Fatalf("evento 0 inesperado: %+v", recebidos[0])
	}
	if recebidos[1].Err != nil || recebidos[1].Evento.Seq != 3 || recebidos[1].Evento.ID != "ev-3" {
		t.Fatalf("evento 1 inesperado: %+v", recebidos[1])
	}
	if recebidos[0].Evento.Tipo != "message.received" || recebidos[0].Evento.ChatJID != "5511@s.whatsapp.net" || recebidos[0].Evento.TraceID != "tr-2" {
		t.Fatalf("campos do evento 0 nao desserializaram como esperado: %+v", recebidos[0].Evento)
	}
	if recebidos[0].Evento.OcorridoEm.IsZero() {
		t.Fatalf("esperava OcorridoEm preenchido, veio zero: %+v", recebidos[0].Evento)
	}
	if string(recebidos[0].Evento.Payload) != `{"body":"oi"}` {
		t.Fatalf("payload cru inesperado: %s", recebidos[0].Evento.Payload)
	}
}

// TestStreamEventos_FromSeqOmitidoQuandoZero cobre o outro lado da mesma
// regra: fromSeq<=0 (nao informado) nao manda from_seq nenhum na query --
// mesma convencao que o servidor ja trata como equivalente
// (internal/transport/sse/stream.go, parseFromSeq).
func TestStreamEventos_FromSeqOmitidoQuandoZero(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/events/stream", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("from_seq") {
			t.Errorf("esperava from_seq ausente da query, veio %q", r.URL.Query().Get("from_seq"))
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		sseEventoData(w, `{"id":"ev-1","type":"message.received","instance_id":"inst-1","seq":1,"occurred_at":"2026-01-01T00:00:01Z","payload":{}}`)
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	streamer := eventosStreamCliente(t, ts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	eventos, err := streamer.AcompanharEventos(ctx, "inst-1", 0)
	if err != nil {
		t.Fatalf("AcompanharEventos: %v", err)
	}
	var count int
	for range eventos {
		count++
	}
	if count != 1 {
		t.Fatalf("esperava 1 evento, recebi %d", count)
	}
}

// TestStreamEventos_ErroConexaoInicial cobre erro HTTP na abertura (ex.:
// instancia inexistente) -- deve vir como erro de retorno da chamada, sem
// canal.
func TestStreamEventos_ErroConexaoInicial(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/events/stream", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"detail":"instancia inexistente"}`)
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	streamer := eventosStreamCliente(t, ts)

	eventos, err := streamer.AcompanharEventos(context.Background(), "nao-existe", 0)
	if err == nil {
		t.Fatalf("esperava erro, recebi eventos=%v", eventos)
	}
	if !errors.Is(err, syncz.ErrNaoEncontrado) {
		t.Fatalf("esperava ErrNaoEncontrado, recebi: %v", err)
	}
	if eventos != nil {
		t.Fatalf("esperava canal nil no erro, recebi %v", eventos)
	}
}

// TestStreamEventos_QuedaDeConexao cobre erro de rede no meio do stream
// (servidor mata a conexao sem fechar direito) -- deve chegar como
// EventoStream.Err antes do canal fechar, sem travar o consumidor.
func TestStreamEventos_QuedaDeConexao(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/events/stream", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		sseEventoData(w, `{"id":"ev-1","type":"message.received","instance_id":"inst-1","seq":1,"occurred_at":"2026-01-01T00:00:01Z","payload":{}}`)
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Fatalf("ResponseWriter nao suporta Hijack neste teste")
		}
		conn, _, err := hj.Hijack()
		if err != nil {
			t.Fatalf("hijack: %v", err)
		}
		_ = conn.Close()
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	streamer := eventosStreamCliente(t, ts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	eventos, err := streamer.AcompanharEventos(ctx, "inst-1", 0)
	if err != nil {
		t.Fatalf("AcompanharEventos: %v", err)
	}

	var recebidos []syncz.EventoStream
	for ev := range eventos {
		recebidos = append(recebidos, ev)
	}

	if len(recebidos) == 0 {
		t.Fatalf("esperava ao menos o evento seq=1 antes da queda")
	}
	ultimo := recebidos[len(recebidos)-1]
	if ultimo.Err == nil {
		t.Fatalf("esperava erro de rede no ultimo evento, recebi: %+v", recebidos)
	}
}

// TestStreamEventos_FechaPorCancelamentoDeContexto cobre o cancelamento do
// lado do chamador: o canal deve fechar (sem EventoStream.Err, ja que
// cancelamento nao e falha) mesmo com o servidor mandando eventos
// indefinidamente.
func TestStreamEventos_FechaPorCancelamentoDeContexto(t *testing.T) {
	liberar := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/events/stream", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		seq := 0
		for {
			select {
			case <-r.Context().Done():
				return
			case <-liberar:
				return
			default:
			}
			seq++
			sseEventoData(w, fmt.Sprintf(`{"id":"ev-%d","type":"message.received","instance_id":"inst-1","seq":%d,"occurred_at":"2026-01-01T00:00:01Z","payload":{}}`, seq, seq))
			time.Sleep(5 * time.Millisecond)
		}
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()
	defer close(liberar)

	streamer := eventosStreamCliente(t, ts)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eventos, err := streamer.AcompanharEventos(ctx, "inst-1", 0)
	if err != nil {
		t.Fatalf("AcompanharEventos: %v", err)
	}

	count := 0
	for range eventos {
		count++
		if count == 2 {
			cancel()
		}
	}

	if count < 2 {
		t.Fatalf("esperava ao menos 2 eventos antes do cancelamento, recebi %d", count)
	}
}
