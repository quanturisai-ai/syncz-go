package syncz_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	syncz "github.com/quanturisai-ai/syncz-go"
	synczv1 "github.com/quanturisai-ai/syncz-go/synczv1"
)

func groupEventsClienteREST(t *testing.T, ts *httptest.Server) syncz.Cliente {
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

// TestCriarEventoGrupo_OK cobre o caso feliz: POST .../group-events devolvendo
// o evento recem-criado, espelhando o JSON real de handleCreateGroupEvent/
// groupEventResp (internal/transport/rest/group_events.go e dto.go).
func TestCriarEventoGrupo_OK(t *testing.T) {
	inicio := time.Now().Add(24 * time.Hour).UTC().Truncate(time.Second)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/instances/{id}/group-events", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.PathValue("id") != "inst-1" {
			t.Errorf("path inesperado: id=%s", r.PathValue("id"))
		}
		var body struct {
			GroupJID          string `json:"group_jid"`
			Title             string `json:"title"`
			Description       string `json:"description"`
			StartTime         string `json:"start_time"`
			ReminderOffsetSec int64  `json:"reminder_offset_sec"`
			LocationName      string `json:"location_name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decodificar corpo: %v", err)
		}
		if body.GroupJID != "123456789@g.us" || body.Title != "Reuniao mensal" || body.LocationName != "Sala 2" {
			t.Errorf("corpo inesperado: %+v", body)
		}
		if body.ReminderOffsetSec != 900 {
			t.Errorf("reminder_offset_sec inesperado: %d", body.ReminderOffsetSec)
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"event_id":             "evt-1",
			"instance_id":          "inst-1",
			"group_jid":            body.GroupJID,
			"title":                body.Title,
			"description":          body.Description,
			"start_time":           inicio.Unix(),
			"end_time":             0,
			"reminder_offset_sec":  body.ReminderOffsetSec,
			"location_name":        body.LocationName,
			"join_link":            "",
			"is_canceled":          false,
			"extra_guests_allowed": false,
			"is_schedule_call":     false,
			"created_at":           inicio.Unix(),
			"updated_at":           inicio.Unix(),
		})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cli := groupEventsClienteREST(t, ts)

	ev, err := cli.CriarEventoGrupo(context.Background(), syncz.EntradaCriarEventoGrupo{
		InstanciaID:          "inst-1",
		GrupoJID:             "123456789@g.us",
		Titulo:               "Reuniao mensal",
		Inicio:               inicio,
		LembreteAntecedencia: 15 * time.Minute,
		LocalNome:            "Sala 2",
	})
	if err != nil {
		t.Fatalf("CriarEventoGrupo falhou: %v", err)
	}
	if ev.EventoID != "evt-1" || ev.GrupoJID != "123456789@g.us" || ev.Titulo != "Reuniao mensal" {
		t.Errorf("evento inesperado: %+v", ev)
	}
	if !ev.Inicio.Equal(inicio) || !ev.Fim.IsZero() {
		t.Errorf("horarios inesperados: %+v", ev)
	}
	if ev.LembreteAntecedencia != 15*time.Minute {
		t.Errorf("lembrete inesperado: %v", ev.LembreteAntecedencia)
	}
}

// TestCriarEventoGrupo_TituloInvalido cobre o 400 sincrono que o servidor
// devolve quando title esta ausente/invalido (validarCreateGroupEvent,
// internal/service/group_events.go) -- deve virar ErrInvalido.
func TestCriarEventoGrupo_TituloInvalido(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/instances/{id}/group-events", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{"detail": "title: obrigatorio"})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cli := groupEventsClienteREST(t, ts)

	_, err := cli.CriarEventoGrupo(context.Background(), syncz.EntradaCriarEventoGrupo{
		InstanciaID: "inst-1",
		GrupoJID:    "123456789@g.us",
		Titulo:      "",
		Inicio:      time.Now().Add(time.Hour),
	})
	if !errors.Is(err, syncz.ErrInvalido) {
		t.Fatalf("esperava ErrInvalido, obteve: %v", err)
	}
}

// TestAtualizarEventoGrupo cobre PATCH .../group-events/{event_id} --
// devolve o evento com o titulo alterado.
func TestAtualizarEventoGrupo(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("PATCH /api/v1/instances/{id}/group-events/{event_id}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("id") != "inst-1" || r.PathValue("event_id") != "evt-1" {
			t.Errorf("path inesperado: id=%s event_id=%s", r.PathValue("id"), r.PathValue("event_id"))
		}
		var body struct {
			Title *string `json:"title"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decodificar corpo: %v", err)
		}
		if body.Title == nil || *body.Title != "Novo titulo" {
			t.Errorf("corpo inesperado: %+v", body)
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"event_id":    "evt-1",
			"instance_id": "inst-1",
			"group_jid":   "123456789@g.us",
			"title":       *body.Title,
			"start_time":  time.Now().Unix(),
			"created_at":  time.Now().Unix(),
			"updated_at":  time.Now().Unix(),
		})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cli := groupEventsClienteREST(t, ts)

	novoTitulo := "Novo titulo"
	ev, err := cli.AtualizarEventoGrupo(context.Background(), syncz.EntradaAtualizarEventoGrupo{
		InstanciaID: "inst-1",
		EventoID:    "evt-1",
		Titulo:      &novoTitulo,
	})
	if err != nil {
		t.Fatalf("AtualizarEventoGrupo falhou: %v", err)
	}
	if ev.Titulo != "Novo titulo" {
		t.Errorf("titulo inesperado: %+v", ev)
	}
}

// TestAtualizarEventoGrupo_NaoEncontrado cobre 404 de event_id inexistente.
func TestAtualizarEventoGrupo_NaoEncontrado(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("PATCH /api/v1/instances/{id}/group-events/{event_id}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{"detail": "evento nao encontrado"})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cli := groupEventsClienteREST(t, ts)

	novoTitulo := "Novo titulo"
	_, err := cli.AtualizarEventoGrupo(context.Background(), syncz.EntradaAtualizarEventoGrupo{
		InstanciaID: "inst-1",
		EventoID:    "evt-inexistente",
		Titulo:      &novoTitulo,
	})
	if !errors.Is(err, syncz.ErrNaoEncontrado) {
		t.Fatalf("esperava ErrNaoEncontrado, obteve: %v", err)
	}
}

// TestCancelarEventoGrupo cobre o caso feliz de POST .../group-events/{event_id}/cancel.
func TestCancelarEventoGrupo(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/instances/{id}/group-events/{event_id}/cancel", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("id") != "inst-1" || r.PathValue("event_id") != "evt-1" {
			t.Errorf("path inesperado: id=%s event_id=%s", r.PathValue("id"), r.PathValue("event_id"))
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"event_id":    "evt-1",
			"instance_id": "inst-1",
			"group_jid":   "123456789@g.us",
			"title":       "Reuniao mensal",
			"is_canceled": true,
			"start_time":  time.Now().Unix(),
			"created_at":  time.Now().Unix(),
			"updated_at":  time.Now().Unix(),
		})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cli := groupEventsClienteREST(t, ts)

	ev, err := cli.CancelarEventoGrupo(context.Background(), syncz.EntradaCancelarEventoGrupo{
		InstanciaID: "inst-1",
		EventoID:    "evt-1",
	})
	if err != nil {
		t.Fatalf("CancelarEventoGrupo falhou: %v", err)
	}
	if !ev.Cancelado {
		t.Errorf("esperava evento cancelado, obteve: %+v", ev)
	}
}

// TestCancelarEventoGrupo_NaoEncontrado cobre 404 de event_id inexistente.
func TestCancelarEventoGrupo_NaoEncontrado(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/instances/{id}/group-events/{event_id}/cancel", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{"detail": "evento nao encontrado"})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cli := groupEventsClienteREST(t, ts)

	_, err := cli.CancelarEventoGrupo(context.Background(), syncz.EntradaCancelarEventoGrupo{
		InstanciaID: "inst-1",
		EventoID:    "evt-inexistente",
	})
	if !errors.Is(err, syncz.ErrNaoEncontrado) {
		t.Fatalf("esperava ErrNaoEncontrado, obteve: %v", err)
	}
}

// TestResponderEventoGrupo cobre o caso feliz de POST .../group-events/{event_id}/response.
func TestResponderEventoGrupo(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/instances/{id}/group-events/{event_id}/response", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("id") != "inst-1" || r.PathValue("event_id") != "evt-1" {
			t.Errorf("path inesperado: id=%s event_id=%s", r.PathValue("id"), r.PathValue("event_id"))
		}
		var body struct {
			Response        string `json:"response"`
			ExtraGuestCount int32  `json:"extra_guest_count"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decodificar corpo: %v", err)
		}
		if body.Response != syncz.RespostaEventoGrupoIndo {
			t.Errorf("response inesperado: %+v", body)
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"event_id":   "evt-1",
			"message_id": "msg-rsvp-1",
			"response":   body.Response,
		})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cli := groupEventsClienteREST(t, ts)

	res, err := cli.ResponderEventoGrupo(context.Background(), syncz.EntradaResponderEventoGrupo{
		InstanciaID: "inst-1",
		EventoID:    "evt-1",
		Resposta:    syncz.RespostaEventoGrupoIndo,
	})
	if err != nil {
		t.Fatalf("ResponderEventoGrupo falhou: %v", err)
	}
	if res.MensagemID != "msg-rsvp-1" || res.Resposta != syncz.RespostaEventoGrupoIndo {
		t.Errorf("resposta inesperada: %+v", res)
	}
}

// TestResponderEventoGrupo_NaoEncontrado cobre 404 de event_id inexistente.
func TestResponderEventoGrupo_NaoEncontrado(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/instances/{id}/group-events/{event_id}/response", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{"detail": "evento nao encontrado"})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cli := groupEventsClienteREST(t, ts)

	_, err := cli.ResponderEventoGrupo(context.Background(), syncz.EntradaResponderEventoGrupo{
		InstanciaID: "inst-1",
		EventoID:    "evt-inexistente",
		Resposta:    syncz.RespostaEventoGrupoIndo,
	})
	if !errors.Is(err, syncz.ErrNaoEncontrado) {
		t.Fatalf("esperava ErrNaoEncontrado, obteve: %v", err)
	}
}

// TestEventoGrupo_Sandbox cobre o Dublê: criar, atualizar, responder e
// cancelar um evento de grupo fake sem tocar rede, mais os casos de erro
// (title ausente na criacao, event_id desconhecido, response invalido).
func TestEventoGrupo_Sandbox(t *testing.T) {
	sbx := syncz.NovoSandbox()
	defer sbx.Fechar()

	ctx := context.Background()
	inicio := time.Now().Add(24 * time.Hour).UTC()

	ev, err := sbx.CriarEventoGrupo(ctx, syncz.EntradaCriarEventoGrupo{
		InstanciaID:             "inst-sbx",
		GrupoJID:                "123456789@g.us",
		Titulo:                  "Reuniao mensal",
		Inicio:                  inicio,
		PermiteConvidadosExtras: true,
	})
	if err != nil {
		t.Fatalf("CriarEventoGrupo falhou: %v", err)
	}
	if ev.EventoID == "" || ev.Cancelado {
		t.Fatalf("evento inesperado: %+v", ev)
	}

	novoTitulo := "Reuniao mensal (remarcada)"
	atualizado, err := sbx.AtualizarEventoGrupo(ctx, syncz.EntradaAtualizarEventoGrupo{
		InstanciaID: "inst-sbx",
		EventoID:    ev.EventoID,
		Titulo:      &novoTitulo,
	})
	if err != nil {
		t.Fatalf("AtualizarEventoGrupo falhou: %v", err)
	}
	if atualizado.Titulo != novoTitulo {
		t.Errorf("titulo nao atualizado: %+v", atualizado)
	}

	res, err := sbx.ResponderEventoGrupo(ctx, syncz.EntradaResponderEventoGrupo{
		InstanciaID:      "inst-sbx",
		EventoID:         ev.EventoID,
		Resposta:         syncz.RespostaEventoGrupoIndo,
		ConvidadosExtras: 2,
	})
	if err != nil {
		t.Fatalf("ResponderEventoGrupo falhou: %v", err)
	}
	if res.Resposta != syncz.RespostaEventoGrupoIndo || res.MensagemID == "" {
		t.Errorf("resposta inesperada: %+v", res)
	}

	cancelado, err := sbx.CancelarEventoGrupo(ctx, syncz.EntradaCancelarEventoGrupo{
		InstanciaID: "inst-sbx",
		EventoID:    ev.EventoID,
	})
	if err != nil {
		t.Fatalf("CancelarEventoGrupo falhou: %v", err)
	}
	if !cancelado.Cancelado {
		t.Errorf("esperava evento cancelado, obteve: %+v", cancelado)
	}

	if _, err := sbx.CriarEventoGrupo(ctx, syncz.EntradaCriarEventoGrupo{
		InstanciaID: "inst-sbx",
		GrupoJID:    "123456789@g.us",
		Titulo:      "",
		Inicio:      inicio,
	}); !errors.Is(err, syncz.ErrInvalido) {
		t.Fatalf("esperava ErrInvalido para title vazio, obteve: %v", err)
	}

	if _, err := sbx.AtualizarEventoGrupo(ctx, syncz.EntradaAtualizarEventoGrupo{
		InstanciaID: "inst-sbx",
		EventoID:    "evt-nao-existe",
	}); !errors.Is(err, syncz.ErrNaoEncontrado) {
		t.Fatalf("esperava ErrNaoEncontrado, obteve: %v", err)
	}
	if _, err := sbx.CancelarEventoGrupo(ctx, syncz.EntradaCancelarEventoGrupo{
		InstanciaID: "inst-sbx",
		EventoID:    "evt-nao-existe",
	}); !errors.Is(err, syncz.ErrNaoEncontrado) {
		t.Fatalf("esperava ErrNaoEncontrado, obteve: %v", err)
	}
	if _, err := sbx.ResponderEventoGrupo(ctx, syncz.EntradaResponderEventoGrupo{
		InstanciaID: "inst-sbx",
		EventoID:    ev.EventoID,
		Resposta:    "invalida",
	}); !errors.Is(err, syncz.ErrInvalido) {
		t.Fatalf("esperava ErrInvalido para response invalido, obteve: %v", err)
	}
}

// mockGroupEventsServer implementa só os RPCs de evento de grupo do
// SyncZapServer, para o teste de gRPC desta task -- embeda
// UnimplementedSyncZapServer como o resto do SDK faz (ver mockSyncZapServer
// em grpc_test.go).
type mockGroupEventsServer struct {
	synczv1.UnimplementedSyncZapServer
	cancelado bool
}

func (m *mockGroupEventsServer) CreateGroupEvent(_ context.Context, req *synczv1.CreateGroupEventRequest) (*synczv1.GroupEvent, error) {
	if req.GetTitle() == "" {
		return nil, status.Error(codes.InvalidArgument, "title: obrigatorio")
	}
	return &synczv1.GroupEvent{
		EventId:     "evt-grpc-1",
		InstanceId:  req.GetInstanceId(),
		GroupJid:    req.GetGroupJid(),
		Title:       req.GetTitle(),
		Description: req.GetDescription(),
		StartTime:   req.GetStartTime(),
		CreatedAt:   1700000000,
		UpdatedAt:   1700000000,
	}, nil
}

func (m *mockGroupEventsServer) UpdateGroupEvent(_ context.Context, req *synczv1.UpdateGroupEventRequest) (*synczv1.GroupEvent, error) {
	if req.GetEventId() == "evt-inexistente" {
		return nil, status.Error(codes.NotFound, "evento nao encontrado")
	}
	title := req.GetTitle()
	return &synczv1.GroupEvent{
		EventId:    req.GetEventId(),
		InstanceId: req.GetInstanceId(),
		Title:      title,
		CreatedAt:  1700000000,
		UpdatedAt:  1700000100,
	}, nil
}

func (m *mockGroupEventsServer) CancelGroupEvent(_ context.Context, req *synczv1.CancelGroupEventRequest) (*synczv1.GroupEvent, error) {
	if req.GetEventId() == "evt-inexistente" {
		return nil, status.Error(codes.NotFound, "evento nao encontrado")
	}
	m.cancelado = true
	return &synczv1.GroupEvent{
		EventId:    req.GetEventId(),
		InstanceId: req.GetInstanceId(),
		IsCanceled: true,
		CreatedAt:  1700000000,
		UpdatedAt:  1700000200,
	}, nil
}

func (m *mockGroupEventsServer) SendEventResponse(_ context.Context, req *synczv1.SendEventResponseRequest) (*synczv1.SendEventResponseResponse, error) {
	if req.GetEventId() == "evt-inexistente" {
		return nil, status.Error(codes.NotFound, "evento nao encontrado")
	}
	return &synczv1.SendEventResponseResponse{
		EventId:   req.GetEventId(),
		MessageId: "msg-rsvp-grpc-1",
		Response:  req.GetResponse(),
	}, nil
}

func groupEventsClienteGRPC(t *testing.T) (syncz.Cliente, *mockGroupEventsServer) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	s := grpc.NewServer()
	mockSrv := &mockGroupEventsServer{}
	synczv1.RegisterSyncZapServer(s, mockSrv)

	go func() {
		_ = s.Serve(lis)
	}()
	t.Cleanup(s.Stop)

	dialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}
	conn, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithInsecure(),
	)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}

	cli := syncz.NovoClienteGRPCComConn(conn, conn.Close)
	t.Cleanup(func() { _ = cli.Fechar() })
	return cli, mockSrv
}

// TestEventoGrupo_GRPC documenta a decisao desta task (568): o proto gRPC TEM
// RPCs equivalentes para evento de grupo (CreateGroupEvent/UpdateGroupEvent/
// CancelGroupEvent/SendEventResponse, conferido em
// proto/syncz/v1/syncz.proto) -- clienteGRPC consome os quatro de verdade em
// vez de devolver ErrTransporteNaoSuporta.
func TestEventoGrupo_GRPC(t *testing.T) {
	cli, mockSrv := groupEventsClienteGRPC(t)
	ctx := context.Background()
	inicio := time.Now().Add(24 * time.Hour)

	ev, err := cli.CriarEventoGrupo(ctx, syncz.EntradaCriarEventoGrupo{
		InstanciaID: "inst-1",
		GrupoJID:    "123456789@g.us",
		Titulo:      "Reuniao mensal",
		Inicio:      inicio,
	})
	if err != nil {
		t.Fatalf("CriarEventoGrupo falhou: %v", err)
	}
	if ev.EventoID != "evt-grpc-1" || ev.GrupoJID != "123456789@g.us" {
		t.Errorf("evento inesperado: %+v", ev)
	}

	_, err = cli.CriarEventoGrupo(ctx, syncz.EntradaCriarEventoGrupo{
		InstanciaID: "inst-1",
		GrupoJID:    "123456789@g.us",
		Titulo:      "",
		Inicio:      inicio,
	})
	if !errors.Is(err, syncz.ErrInvalido) {
		t.Fatalf("esperava ErrInvalido, obteve: %v", err)
	}

	novoTitulo := "Reuniao mensal (remarcada)"
	atualizado, err := cli.AtualizarEventoGrupo(ctx, syncz.EntradaAtualizarEventoGrupo{
		InstanciaID: "inst-1",
		EventoID:    "evt-grpc-1",
		Titulo:      &novoTitulo,
	})
	if err != nil {
		t.Fatalf("AtualizarEventoGrupo falhou: %v", err)
	}
	if atualizado.Titulo != novoTitulo {
		t.Errorf("titulo nao atualizado: %+v", atualizado)
	}

	_, err = cli.AtualizarEventoGrupo(ctx, syncz.EntradaAtualizarEventoGrupo{
		InstanciaID: "inst-1",
		EventoID:    "evt-inexistente",
		Titulo:      &novoTitulo,
	})
	if !errors.Is(err, syncz.ErrNaoEncontrado) {
		t.Fatalf("esperava ErrNaoEncontrado, obteve: %v", err)
	}

	res, err := cli.ResponderEventoGrupo(ctx, syncz.EntradaResponderEventoGrupo{
		InstanciaID: "inst-1",
		EventoID:    "evt-grpc-1",
		Resposta:    syncz.RespostaEventoGrupoTalvez,
	})
	if err != nil {
		t.Fatalf("ResponderEventoGrupo falhou: %v", err)
	}
	if res.Resposta != syncz.RespostaEventoGrupoTalvez || res.MensagemID != "msg-rsvp-grpc-1" {
		t.Errorf("resposta inesperada: %+v", res)
	}

	cancelado, err := cli.CancelarEventoGrupo(ctx, syncz.EntradaCancelarEventoGrupo{
		InstanciaID: "inst-1",
		EventoID:    "evt-grpc-1",
	})
	if err != nil {
		t.Fatalf("CancelarEventoGrupo falhou: %v", err)
	}
	if !cancelado.Cancelado || !mockSrv.cancelado {
		t.Errorf("esperava evento cancelado: %+v", cancelado)
	}
}
