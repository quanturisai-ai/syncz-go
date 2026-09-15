package syncz_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	syncz "github.com/quanturisai-ai/syncz-go"
)

// sseFlushWriter escreve um bloco SSE e da flush imediato -- exatamente o que
// internal/transport/sse.Stream.Send faz no servidor real.
func sseFlushWriter(w http.ResponseWriter, event, data string) {
	fmt.Fprintf(w, "event: %s\n", event)
	fmt.Fprintf(w, "data: %s\n\n", data)
	w.(http.Flusher).Flush()
}

func streamCliente(t *testing.T, ts *httptest.Server) syncz.ClientePareamentoStream {
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

	streamer, ok := cli.(syncz.ClientePareamentoStream)
	if !ok {
		t.Fatalf("cliente REST nao implementa ClientePareamentoStream")
	}
	return streamer
}

// TestAcompanharPareamento_SequenciaQREPaired cobre a sequencia real do
// servidor: varios QR renovando (seq crescente) e depois "paired" -- apos o
// que o servidor fecha o stream (ver handlePairingStream, que retorna false
// apos emitir "paired").
func TestAcompanharPareamento_SequenciaQREPaired(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/instances/{id}/pairing/stream", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		sseFlushWriter(w, "qr", `{"qr_code":"2@qr-1","seq":1,"expires_at":"2026-01-01T00:00:00Z"}`)
		sseFlushWriter(w, "qr", `{"qr_code":"2@qr-2","seq":2,"expires_at":"2026-01-01T00:01:00Z"}`)
		sseFlushWriter(w, "paired", `{"phone":"5511999999999"}`)
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	streamer := streamCliente(t, ts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	eventos, err := streamer.AcompanharPareamento(ctx, "inst-1")
	if err != nil {
		t.Fatalf("AcompanharPareamento: %v", err)
	}

	var recebidos []syncz.EventoPareamento
	for ev := range eventos {
		recebidos = append(recebidos, ev)
	}

	if len(recebidos) != 3 {
		t.Fatalf("esperava 3 eventos, recebi %d: %+v", len(recebidos), recebidos)
	}
	if recebidos[0].Err != nil || recebidos[0].Estado.QRCode != "2@qr-1" || recebidos[0].Estado.Seq != 1 {
		t.Fatalf("evento 0 inesperado: %+v", recebidos[0])
	}
	if recebidos[1].Err != nil || recebidos[1].Estado.QRCode != "2@qr-2" || recebidos[1].Estado.Seq != 2 {
		t.Fatalf("evento 1 inesperado: %+v", recebidos[1])
	}
	if recebidos[2].Err != nil || !recebidos[2].Estado.Pareado || recebidos[2].Estado.Telefone != "5511999999999" {
		t.Fatalf("evento 2 (paired) inesperado: %+v", recebidos[2])
	}
}

// TestAcompanharPareamento_Expirado cobre o evento "expired": o SDK deve
// entregar ErrPareamentoExpirado (comparavel via errors.Is) como o ultimo
// evento, e fechar o canal em seguida.
func TestAcompanharPareamento_Expirado(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/instances/{id}/pairing/stream", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		sseFlushWriter(w, "qr", `{"qr_code":"2@qr-1","seq":1,"expires_at":"2026-01-01T00:00:00Z"}`)
		sseFlushWriter(w, "expired", `{}`)
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	streamer := streamCliente(t, ts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	eventos, err := streamer.AcompanharPareamento(ctx, "inst-1")
	if err != nil {
		t.Fatalf("AcompanharPareamento: %v", err)
	}

	var recebidos []syncz.EventoPareamento
	for ev := range eventos {
		recebidos = append(recebidos, ev)
	}

	if len(recebidos) != 2 {
		t.Fatalf("esperava 2 eventos, recebi %d: %+v", len(recebidos), recebidos)
	}
	ultimo := recebidos[len(recebidos)-1]
	if !errors.Is(ultimo.Err, syncz.ErrPareamentoExpirado) {
		t.Fatalf("esperava ErrPareamentoExpirado, recebi: %+v", ultimo)
	}
}

// TestAcompanharPareamento_ConexaoInicialRecusada cobre erro de rede/HTTP na
// abertura do stream (ex.: instancia inexistente) -- deve vir como erro de
// retorno da chamada, sem canal.
func TestAcompanharPareamento_ConexaoInicialRecusada(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/instances/{id}/pairing/stream", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"detail":"instancia inexistente"}`)
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	streamer := streamCliente(t, ts)

	eventos, err := streamer.AcompanharPareamento(context.Background(), "nao-existe")
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

// TestAcompanharPareamento_QuedaDeConexao cobre erro de rede no meio do
// stream (servidor mata a conexao sem fechar direito) -- deve chegar como
// EventoPareamento.Err antes do canal fechar, sem travar o consumidor.
func TestAcompanharPareamento_QuedaDeConexao(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/instances/{id}/pairing/stream", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		sseFlushWriter(w, "qr", `{"qr_code":"2@qr-1","seq":1,"expires_at":"2026-01-01T00:00:00Z"}`)
		// Derruba a conexao no meio do stream, sem terminar o evento e sem
		// fechar via EOF normal -- simula queda de rede.
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

	streamer := streamCliente(t, ts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	eventos, err := streamer.AcompanharPareamento(ctx, "inst-1")
	if err != nil {
		t.Fatalf("AcompanharPareamento: %v", err)
	}

	var recebidos []syncz.EventoPareamento
	for ev := range eventos {
		recebidos = append(recebidos, ev)
	}

	if len(recebidos) == 0 {
		t.Fatalf("esperava ao menos o evento qr antes da queda")
	}
	ultimo := recebidos[len(recebidos)-1]
	if ultimo.Err == nil {
		t.Fatalf("esperava erro de rede no ultimo evento, recebi: %+v", recebidos)
	}
}

// TestAcompanharPareamento_FechaPorCancelamentoDeContexto cobre o
// cancelamento do lado do chamador: o canal deve fechar (sem
// EventoPareamento.Err, ja que cancelamento nao e falha) mesmo com o servidor
// mandando eventos indefinidamente.
func TestAcompanharPareamento_FechaPorCancelamentoDeContexto(t *testing.T) {
	liberar := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/instances/{id}/pairing/stream", func(w http.ResponseWriter, r *http.Request) {
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
			sseFlushWriter(w, "qr", fmt.Sprintf(`{"qr_code":"2@qr-%d","seq":%d,"expires_at":"2026-01-01T00:00:00Z"}`, seq, seq))
			time.Sleep(5 * time.Millisecond)
		}
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()
	defer close(liberar)

	streamer := streamCliente(t, ts)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eventos, err := streamer.AcompanharPareamento(ctx, "inst-1")
	if err != nil {
		t.Fatalf("AcompanharPareamento: %v", err)
	}

	// Consome alguns eventos, depois cancela -- o canal precisa fechar sem
	// travar o teste.
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
