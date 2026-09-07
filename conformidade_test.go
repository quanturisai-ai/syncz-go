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
	"google.golang.org/protobuf/types/known/timestamppb"

	syncz "github.com/quanturisai-ai/syncz-go"
	synczv1 "github.com/quanturisai-ai/syncz-go/synczv1"
)

// bateriaDeConformidade executa os mesmos fluxos contra qualquer implementação de Cliente (CA-03).
func bateriaDeConformidade(t *testing.T, cli syncz.Cliente) {
	ctx := context.Background()

	// 1. Criar Instância
	inst, err := cli.CriarInstancia(ctx, syncz.EntradaCriarInstancia{
		Nome:     "inst-conformidade",
		Telefone: "5511999999999",
	})
	if err != nil {
		t.Fatalf("CriarInstancia falhou: %v", err)
	}
	if inst.ID == "" {
		t.Errorf("esperava id nao vazio para instancia")
	}

	// 2. Estado de Pareamento
	par, err := cli.EstadoPareamento(ctx, inst.ID)
	if err != nil {
		t.Fatalf("EstadoPareamento falhou: %v", err)
	}
	if par.Seq < 1 {
		t.Errorf("esperava seq >= 1, obteve %d", par.Seq)
	}

	// 3. Envio de mensagem nova (primeira vez)
	rec1, err := cli.Enviar(ctx, syncz.EntradaEnvio{
		InstanciaID:       inst.ID,
		Para:              "5511888888888",
		Texto:             "ola conformidade",
		ChaveIdempotencia: "conf-key-1",
	})
	if err != nil {
		t.Fatalf("Enviar falhou: %v", err)
	}
	if rec1.MensagemID == "" {
		t.Errorf("esperava mensagem_id nao vazio")
	}
	if rec1.Duplicada {
		t.Errorf("primeiro envio nao deve ser marcado como duplicado")
	}

	// 4. Reenvio da mesma chave e mesmo texto (Idempotência -> Duplicada = true)
	rec2, err := cli.Enviar(ctx, syncz.EntradaEnvio{
		InstanciaID:       inst.ID,
		Para:              "5511888888888",
		Texto:             "ola conformidade",
		ChaveIdempotencia: "conf-key-1",
	})
	if err != nil {
		t.Fatalf("Enviar duplicado falhou: %v", err)
	}
	if !rec2.Duplicada {
		t.Errorf("reenvio com mesma chave deve ter Duplicada=true")
	}
	if rec2.MensagemID != rec1.MensagemID {
		t.Errorf("reenvio deve devolver mesmo MensagemID: %s vs %s", rec2.MensagemID, rec1.MensagemID)
	}

	// 5. Erro esperado em recurso inexistente
	_, err = cli.ObterInstancia(ctx, "instancia-inexistente-xyz")
	if !errors.Is(err, syncz.ErrNaoEncontrado) {
		t.Errorf("esperava ErrNaoEncontrado para instancia inexistente, obteve: %v", err)
	}
}

type conformidadeGRPCServer struct {
	synczv1.UnimplementedSyncZapServer
	mensagens map[string]string
}

func (s *conformidadeGRPCServer) CreateInstance(_ context.Context, req *synczv1.CreateInstanceRequest) (*synczv1.Instance, error) {
	return &synczv1.Instance{
		Id:        "inst-grpc-conf",
		Name:      req.GetName(),
		Phone:     req.GetPhone(),
		Status:    "created",
		CreatedAt: timestamppb.Now(),
		UpdatedAt: timestamppb.Now(),
	}, nil
}

func (s *conformidadeGRPCServer) GetInstance(_ context.Context, req *synczv1.GetInstanceRequest) (*synczv1.Instance, error) {
	if req.GetId() == "instancia-inexistente-xyz" {
		return nil, status.Error(codes.NotFound, "instancia nao encontrada")
	}
	return &synczv1.Instance{Id: req.GetId()}, nil
}

func (s *conformidadeGRPCServer) PairingState(_ context.Context, _ *synczv1.PairingStateRequest) (*synczv1.PairingStateInfo, error) {
	return &synczv1.PairingStateInfo{
		QrCode:    "2@qr-grpc-conf",
		Seq:       1,
		ExpiresAt: timestamppb.Now(),
	}, nil
}

func (s *conformidadeGRPCServer) SendMessage(_ context.Context, req *synczv1.SendMessageRequest) (*synczv1.SendMessageResponse, error) {
	if _, ok := s.mensagens[req.GetIdempotencyKey()]; ok {
		return &synczv1.SendMessageResponse{
			MessageId: "msg-grpc-conf-1",
			Status:    "duplicate",
			SentAt:    timestamppb.Now(),
		}, nil
	}
	s.mensagens[req.GetIdempotencyKey()] = req.GetText()
	return &synczv1.SendMessageResponse{
		MessageId: "msg-grpc-conf-1",
		Status:    "accepted",
		SentAt:    timestamppb.Now(),
	}, nil
}

func TestConformidadeDosTresClientes(t *testing.T) {
	// 1. Sandbox
	t.Run("Sandbox", func(t *testing.T) {
		sbx := syncz.NovoSandbox()
		bateriaDeConformidade(t, sbx)
	})

	// 2. REST (via httptest.Server)
	t.Run("REST", func(t *testing.T) {
		msgs := make(map[string]string)
		mux := http.NewServeMux()
		mux.HandleFunc("POST /api/v1/instances", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":         "inst-rest-conf",
				"name":       "inst-conformidade",
				"status":     "created",
				"created_at": time.Now().UTC().Format(time.RFC3339),
				"updated_at": time.Now().UTC().Format(time.RFC3339),
			})
		})
		mux.HandleFunc("GET /api/v1/instances/{id}", func(w http.ResponseWriter, r *http.Request) {
			if r.PathValue("id") == "instancia-inexistente-xyz" {
				w.WriteHeader(http.StatusNotFound)
				_ = json.NewEncoder(w).Encode(map[string]any{"detail": "instancia inexistente"})
				return
			}
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": r.PathValue("id")})
		})
		mux.HandleFunc("GET /api/v1/instances/{id}/pairing", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"qr_code": "2@qr-rest-conf",
				"seq":     1,
			})
		})
		mux.HandleFunc("POST /api/v1/instances/{id}/messages", func(w http.ResponseWriter, r *http.Request) {
			idem := r.Header.Get("Idempotency-Key")
			if _, ok := msgs[idem]; ok {
				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"message_id": "msg-rest-conf-1",
					"status":     "delivered",
				})
				return
			}
			msgs[idem] = "sent"
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"message_id": "msg-rest-conf-1",
				"status":     "accepted",
			})
		})

		srv := httptest.NewServer(mux)
		defer srv.Close()

		cli, err := syncz.Novo(syncz.Opcoes{
			BaseURL:    srv.URL,
			ChaveAPI:   "test-key",
			HTTPClient: srv.Client(),
		})
		if err != nil {
			t.Fatalf("criar cliente rest falhou: %v", err)
		}
		defer cli.Fechar()

		bateriaDeConformidade(t, cli)
	})

	// 3. gRPC (via bufconn)
	t.Run("gRPC", func(t *testing.T) {
		lis := bufconn.Listen(1024 * 1024)
		s := grpc.NewServer()
		synczv1.RegisterSyncZapServer(s, &conformidadeGRPCServer{mensagens: make(map[string]string)})

		go func() {
			_ = s.Serve(lis)
		}()
		defer s.Stop()

		ctx := context.Background()
		conn, err := grpc.DialContext(ctx, "bufnet",
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
			grpc.WithInsecure(),
		)
		if err != nil {
			t.Fatalf("dial grpc falhou: %v", err)
		}

		cli := syncz.NovoClienteGRPCComConn(conn, conn.Close)
		defer cli.Fechar()

		bateriaDeConformidade(t, cli)
	})
}
