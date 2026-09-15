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

func pollsClienteREST(t *testing.T, ts *httptest.Server) syncz.Cliente {
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

// TestCriarEnquete_OK cobre o caso feliz: POST .../polls devolvendo a enquete
// recem-criada, espelhando o JSON real de handleCreatePoll/toPollResp
// (internal/transport/rest/polls.go).
func TestCriarEnquete_OK(t *testing.T) {
	createdAt := time.Now().UTC().Truncate(time.Second)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/instances/{id}/polls", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.PathValue("id") != "inst-1" {
			t.Errorf("path inesperado: id=%s", r.PathValue("id"))
		}
		var body struct {
			GroupJID        string   `json:"group_jid"`
			Question        string   `json:"question"`
			Options         []string `json:"options"`
			SelectableCount int      `json:"selectable_count"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decodificar corpo: %v", err)
		}
		if body.GroupJID != "123456789@g.us" || body.Question != "Qual sabor?" || len(body.Options) != 2 {
			t.Errorf("corpo inesperado: %+v", body)
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"poll_id":          "poll-1",
			"instance_id":      "inst-1",
			"group_jid":        body.GroupJID,
			"question":         body.Question,
			"options":          body.Options,
			"selectable_count": 1,
			"is_closed":        false,
			"created_at":       createdAt.Unix(),
		})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cli := pollsClienteREST(t, ts)

	enq, err := cli.CriarEnquete(context.Background(), syncz.EntradaCriarEnquete{
		InstanciaID: "inst-1",
		GrupoJID:    "123456789@g.us",
		Pergunta:    "Qual sabor?",
		Opcoes:      []string{"chocolate", "morango"},
	})
	if err != nil {
		t.Fatalf("CriarEnquete falhou: %v", err)
	}
	if enq.PollID != "poll-1" || enq.GrupoJID != "123456789@g.us" || enq.Pergunta != "Qual sabor?" {
		t.Errorf("enquete inesperada: %+v", enq)
	}
	if len(enq.Opcoes) != 2 || enq.Selecionaveis != 1 || enq.Encerrada {
		t.Errorf("campos inesperados: %+v", enq)
	}
	if !enq.CriadaEm.Equal(createdAt) {
		t.Errorf("created_at inesperado: %v (esperava %v)", enq.CriadaEm, createdAt)
	}
}

// TestCriarEnquete_GroupJIDInvalido cobre o 400 sincrono que o servidor
// devolve quando group_jid nao termina em @g.us (Polls.validar,
// internal/service/polls.go) -- deve virar ErrInvalido, comparavel via
// errors.Is.
func TestCriarEnquete_GroupJIDInvalido(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/instances/{id}/polls", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{"detail": "group_jid: precisa terminar em @g.us"})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cli := pollsClienteREST(t, ts)

	_, err := cli.CriarEnquete(context.Background(), syncz.EntradaCriarEnquete{
		InstanciaID: "inst-1",
		GrupoJID:    "nao-e-um-grupo",
		Pergunta:    "Qual sabor?",
		Opcoes:      []string{"chocolate", "morango"},
	})
	if !errors.Is(err, syncz.ErrInvalido) {
		t.Fatalf("esperava ErrInvalido, obteve: %v", err)
	}
}

// TestResultadoEnquete cobre o tally antes e depois de votos --
// GET .../polls/{poll_id}/results espelhando toPollResultsResp
// (internal/transport/rest/polls.go).
func TestResultadoEnquete(t *testing.T) {
	computedAt := time.Now().UTC().Truncate(time.Second)
	votado := false

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/instances/{id}/polls/{poll_id}/results", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("id") != "inst-1" || r.PathValue("poll_id") != "poll-1" {
			t.Errorf("path inesperado: id=%s poll_id=%s", r.PathValue("id"), r.PathValue("poll_id"))
		}
		w.WriteHeader(http.StatusOK)
		if !votado {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"poll_id": "poll-1",
				"tallies": []map[string]any{
					{"option": "chocolate", "count": 0, "voters": []string{}},
					{"option": "morango", "count": 0, "voters": []string{}},
				},
				"total_voters": 0,
				"computed_at":  computedAt.Unix(),
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"poll_id": "poll-1",
			"tallies": []map[string]any{
				{"option": "chocolate", "count": 2, "voters": []string{"5511999998888@s.whatsapp.net", "5511999997777@s.whatsapp.net"}},
				{"option": "morango", "count": 0, "voters": []string{}},
			},
			"total_voters": 2,
			"computed_at":  computedAt.Unix(),
		})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cli := pollsClienteREST(t, ts)

	antes, err := cli.ResultadoEnquete(context.Background(), "inst-1", "poll-1")
	if err != nil {
		t.Fatalf("ResultadoEnquete (antes) falhou: %v", err)
	}
	if antes.TotalVotantes != 0 || antes.Opcoes[0].Votos != 0 {
		t.Errorf("resultado antes de votos inesperado: %+v", antes)
	}
	if !antes.CalculadoEm.Equal(computedAt) {
		t.Errorf("computed_at inesperado: %v (esperava %v)", antes.CalculadoEm, computedAt)
	}

	votado = true

	depois, err := cli.ResultadoEnquete(context.Background(), "inst-1", "poll-1")
	if err != nil {
		t.Fatalf("ResultadoEnquete (depois) falhou: %v", err)
	}
	if depois.TotalVotantes != 2 {
		t.Errorf("total_voters inesperado: %d", depois.TotalVotantes)
	}
	if depois.Opcoes[0].Opcao != "chocolate" || depois.Opcoes[0].Votos != 2 || len(depois.Opcoes[0].Votantes) != 2 {
		t.Errorf("tally da opcao chocolate inesperado: %+v", depois.Opcoes[0])
	}
	if depois.Opcoes[1].Votos != 0 {
		t.Errorf("tally da opcao morango inesperado: %+v", depois.Opcoes[1])
	}
}

// TestResultadoEnquete_NaoEncontrada cobre 404 de poll_id inexistente.
func TestResultadoEnquete_NaoEncontrada(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/instances/{id}/polls/{poll_id}/results", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{"detail": "enquete nao encontrada"})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cli := pollsClienteREST(t, ts)

	_, err := cli.ResultadoEnquete(context.Background(), "inst-1", "poll-inexistente")
	if !errors.Is(err, syncz.ErrNaoEncontrado) {
		t.Fatalf("esperava ErrNaoEncontrado, obteve: %v", err)
	}
}

// TestEncerrarEnquete cobre o caso feliz de POST .../polls/{poll_id}/close --
// devolve a enquete com is_closed=true.
func TestEncerrarEnquete(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/instances/{id}/polls/{poll_id}/close", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("id") != "inst-1" || r.PathValue("poll_id") != "poll-1" {
			t.Errorf("path inesperado: id=%s poll_id=%s", r.PathValue("id"), r.PathValue("poll_id"))
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"poll_id":          "poll-1",
			"instance_id":      "inst-1",
			"group_jid":        "123456789@g.us",
			"question":         "Qual sabor?",
			"options":          []string{"chocolate", "morango"},
			"selectable_count": 1,
			"is_closed":        true,
			"created_at":       time.Now().UTC().Unix(),
		})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cli := pollsClienteREST(t, ts)

	enq, err := cli.EncerrarEnquete(context.Background(), "inst-1", "poll-1")
	if err != nil {
		t.Fatalf("EncerrarEnquete falhou: %v", err)
	}
	if !enq.Encerrada {
		t.Errorf("esperava enquete encerrada, obteve: %+v", enq)
	}
}

// TestEncerrarEnquete_NaoEncontrada cobre 404 de poll_id inexistente ao
// encerrar.
func TestEncerrarEnquete_NaoEncontrada(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/instances/{id}/polls/{poll_id}/close", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{"detail": "enquete nao encontrada"})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cli := pollsClienteREST(t, ts)

	_, err := cli.EncerrarEnquete(context.Background(), "inst-1", "poll-inexistente")
	if !errors.Is(err, syncz.ErrNaoEncontrado) {
		t.Fatalf("esperava ErrNaoEncontrado, obteve: %v", err)
	}
}

// TestEnquete_Sandbox cobre o Dublê: criar, consultar resultado (tally
// zerado, ja que o Dublê nao expõe um jeito de votar) e encerrar uma enquete
// fake sem tocar rede, mais o caso de erro (poll_id desconhecido).
func TestEnquete_Sandbox(t *testing.T) {
	sbx := syncz.NovoSandbox()
	defer sbx.Fechar()

	ctx := context.Background()
	enq, err := sbx.CriarEnquete(ctx, syncz.EntradaCriarEnquete{
		InstanciaID: "inst-sbx",
		GrupoJID:    "123456789@g.us",
		Pergunta:    "Qual sabor?",
		Opcoes:      []string{"chocolate", "morango"},
	})
	if err != nil {
		t.Fatalf("CriarEnquete falhou: %v", err)
	}
	if enq.PollID == "" || enq.Encerrada {
		t.Fatalf("enquete inesperada: %+v", enq)
	}

	res, err := sbx.ResultadoEnquete(ctx, "inst-sbx", enq.PollID)
	if err != nil {
		t.Fatalf("ResultadoEnquete falhou: %v", err)
	}
	if len(res.Opcoes) != 2 || res.TotalVotantes != 0 {
		t.Errorf("resultado inesperado: %+v", res)
	}

	fechada, err := sbx.EncerrarEnquete(ctx, "inst-sbx", enq.PollID)
	if err != nil {
		t.Fatalf("EncerrarEnquete falhou: %v", err)
	}
	if !fechada.Encerrada {
		t.Errorf("esperava enquete encerrada, obteve: %+v", fechada)
	}

	if _, err := sbx.CriarEnquete(ctx, syncz.EntradaCriarEnquete{
		InstanciaID: "inst-sbx",
		GrupoJID:    "123456789@g.us",
		Pergunta:    "invalida",
		Opcoes:      []string{"unica"},
	}); !errors.Is(err, syncz.ErrInvalido) {
		t.Fatalf("esperava ErrInvalido para menos de 2 opcoes, obteve: %v", err)
	}

	if _, err := sbx.ResultadoEnquete(ctx, "inst-sbx", "poll-nao-existe"); !errors.Is(err, syncz.ErrNaoEncontrado) {
		t.Fatalf("esperava ErrNaoEncontrado, obteve: %v", err)
	}
	if _, err := sbx.EncerrarEnquete(ctx, "inst-sbx", "poll-nao-existe"); !errors.Is(err, syncz.ErrNaoEncontrado) {
		t.Fatalf("esperava ErrNaoEncontrado, obteve: %v", err)
	}
}

// mockPollsServer implementa só os RPCs de enquete do SyncZapServer, para o
// teste de gRPC desta task -- embeda UnimplementedSyncZapServer como o resto
// do SDK faz (ver mockSyncZapServer em grpc_test.go).
type mockPollsServer struct {
	synczv1.UnimplementedSyncZapServer
	fechada bool
}

func (m *mockPollsServer) CreatePoll(_ context.Context, req *synczv1.CreatePollRequest) (*synczv1.Poll, error) {
	if req.GetGroupJid() == "nao-e-um-grupo" {
		return nil, status.Error(codes.InvalidArgument, "group_jid: precisa terminar em @g.us")
	}
	return &synczv1.Poll{
		PollId:          "poll-grpc-1",
		InstanceId:      req.GetInstanceId(),
		GroupJid:        req.GetGroupJid(),
		Question:        req.GetQuestion(),
		Options:         req.GetOptions(),
		SelectableCount: req.GetSelectableCount(),
		CreatedAt:       1700000000,
	}, nil
}

func (m *mockPollsServer) GetPollResults(_ context.Context, req *synczv1.GetPollResultsRequest) (*synczv1.PollResults, error) {
	if req.GetPollId() == "poll-inexistente" {
		return nil, status.Error(codes.NotFound, "enquete nao encontrada")
	}
	return &synczv1.PollResults{
		PollId: req.GetPollId(),
		Tallies: []*synczv1.PollOptionTally{
			{Option: "chocolate", Count: 1, Voters: []string{"5511999998888@s.whatsapp.net"}},
			{Option: "morango", Count: 0, Voters: []string{}},
		},
		TotalVoters: 1,
		ComputedAt:  1700000100,
	}, nil
}

func (m *mockPollsServer) ClosePoll(_ context.Context, req *synczv1.ClosePollRequest) (*synczv1.Poll, error) {
	m.fechada = true
	return &synczv1.Poll{
		PollId:     req.GetPollId(),
		InstanceId: req.GetInstanceId(),
		IsClosed:   true,
		CreatedAt:  1700000000,
	}, nil
}

func pollsClienteGRPC(t *testing.T) (syncz.Cliente, *mockPollsServer) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	s := grpc.NewServer()
	mockSrv := &mockPollsServer{}
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

// TestEnquete_GRPC documenta a decisao desta task (567): diferente de
// StatusMensagem/StatusOperacao (tasks 565/566), o proto gRPC TEM RPCs
// equivalentes para enquete (CreatePoll/GetPollResults/ClosePoll, conferido
// em proto/syncz/v1/syncz.proto) -- clienteGRPC consome os três de verdade em
// vez de devolver ErrTransporteNaoSuporta.
func TestEnquete_GRPC(t *testing.T) {
	cli, mockSrv := pollsClienteGRPC(t)
	ctx := context.Background()

	enq, err := cli.CriarEnquete(ctx, syncz.EntradaCriarEnquete{
		InstanciaID: "inst-1",
		GrupoJID:    "123456789@g.us",
		Pergunta:    "Qual sabor?",
		Opcoes:      []string{"chocolate", "morango"},
	})
	if err != nil {
		t.Fatalf("CriarEnquete falhou: %v", err)
	}
	if enq.PollID != "poll-grpc-1" || enq.GrupoJID != "123456789@g.us" {
		t.Errorf("enquete inesperada: %+v", enq)
	}

	_, err = cli.CriarEnquete(ctx, syncz.EntradaCriarEnquete{
		InstanciaID: "inst-1",
		GrupoJID:    "nao-e-um-grupo",
		Pergunta:    "Qual sabor?",
		Opcoes:      []string{"chocolate", "morango"},
	})
	if !errors.Is(err, syncz.ErrInvalido) {
		t.Fatalf("esperava ErrInvalido, obteve: %v", err)
	}

	res, err := cli.ResultadoEnquete(ctx, "inst-1", "poll-grpc-1")
	if err != nil {
		t.Fatalf("ResultadoEnquete falhou: %v", err)
	}
	if res.TotalVotantes != 1 || len(res.Opcoes) != 2 || res.Opcoes[0].Votos != 1 {
		t.Errorf("resultado inesperado: %+v", res)
	}

	_, err = cli.ResultadoEnquete(ctx, "inst-1", "poll-inexistente")
	if !errors.Is(err, syncz.ErrNaoEncontrado) {
		t.Fatalf("esperava ErrNaoEncontrado, obteve: %v", err)
	}

	fechada, err := cli.EncerrarEnquete(ctx, "inst-1", "poll-grpc-1")
	if err != nil {
		t.Fatalf("EncerrarEnquete falhou: %v", err)
	}
	if !fechada.Encerrada || !mockSrv.fechada {
		t.Errorf("esperava enquete encerrada: %+v", fechada)
	}
}
