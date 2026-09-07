package syncz_test

import (
	"context"
	"errors"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	syncz "github.com/quanturisai-ai/syncz-go"
	synczv1 "github.com/quanturisai-ai/syncz-go/synczv1"
)

type mockSyncZapServer struct {
	synczv1.UnimplementedSyncZapServer
	receivedToken              string
	ultimoCreateGroupWait      bool
	ultimoCreateGroupTimeoutMs int64
}

func (m *mockSyncZapServer) extractToken(ctx context.Context) {
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		vals := md.Get("authorization")
		if len(vals) > 0 {
			m.receivedToken = vals[0]
		}
	}
}

func (m *mockSyncZapServer) CreateInstance(ctx context.Context, req *synczv1.CreateInstanceRequest) (*synczv1.Instance, error) {
	m.extractToken(ctx)
	if req.GetName() == "invalido" {
		return nil, status.Error(codes.InvalidArgument, "nome invalido")
	}
	if req.GetName() == "nao-encontrado" {
		return nil, status.Error(codes.NotFound, "instancia nao encontrada")
	}
	return &synczv1.Instance{
		Id:        "inst-123",
		TenantId:  "ten-1",
		Name:      req.GetName(),
		Phone:     req.GetPhone(),
		Status:    "connected",
		CreatedAt: timestamppb.Now(),
		UpdatedAt: timestamppb.Now(),
	}, nil
}

func (m *mockSyncZapServer) CreateGroup(ctx context.Context, req *synczv1.CreateGroupRequest) (*synczv1.Group, error) {
	m.extractToken(ctx)
	m.ultimoCreateGroupWait = req.GetWait()
	m.ultimoCreateGroupTimeoutMs = req.GetWaitTimeoutMs()
	if !req.GetWait() {
		return &synczv1.Group{Subject: req.GetSubject(), Status: "accepted", OpId: "op-async-1"}, nil
	}
	return &synczv1.Group{
		Jid:          "120363999@g.us",
		Subject:      req.GetSubject(),
		Participants: req.GetParticipants(),
		OwnerJid:     "5511999999999@s.whatsapp.net",
	}, nil
}

func (m *mockSyncZapServer) SendMessage(ctx context.Context, req *synczv1.SendMessageRequest) (*synczv1.SendMessageResponse, error) {
	m.extractToken(ctx)
	if req.GetIdempotencyKey() == "dup-key" {
		return &synczv1.SendMessageResponse{
			MessageId: "msg-dup-1",
			Status:    "duplicate",
			SentAt:    timestamppb.Now(),
		}, nil
	}
	if req.GetText() == "erro-unauthenticated" {
		return nil, status.Error(codes.Unauthenticated, "token invalido")
	}
	return &synczv1.SendMessageResponse{
		MessageId: "msg-999",
		Status:    "accepted",
		SentAt:    timestamppb.Now(),
	}, nil
}

func TestOpcoesValidacao(t *testing.T) {
	_, err := syncz.Novo(syncz.Opcoes{})
	if err == nil {
		t.Fatal("esperava erro sem endereco")
	}

	_, err = syncz.Novo(syncz.Opcoes{
		BaseURL:      "http://localhost",
		EnderecoGRPC: "localhost:50051",
		ChaveAPI:     "token",
	})
	if err == nil {
		t.Fatal("esperava erro com BaseURL e EnderecoGRPC simultaneos")
	}

	_, err = syncz.Novo(syncz.Opcoes{
		EnderecoGRPC: "localhost:50051",
	})
	if err == nil {
		t.Fatal("esperava erro sem ChaveAPI")
	}
}

func TestClienteGRPC(t *testing.T) {
	lis := bufconn.Listen(1024 * 1024)
	s := grpc.NewServer()
	mockSrv := &mockSyncZapServer{}
	synczv1.RegisterSyncZapServer(s, mockSrv)

	go func() {
		if err := s.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			t.Logf("bufconn server: %v", err)
		}
	}()
	defer s.Stop()

	dialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer chave-teste-123")
			return invoker(ctx, method, req, reply, cc, opts...)
		}),
	)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}

	cli := syncz.NovoClienteGRPCComConn(conn, conn.Close)
	defer cli.Fechar()

	// 1. Criar Instancia com sucesso
	inst, err := cli.CriarInstancia(ctx, syncz.EntradaCriarInstancia{Nome: "teste", Telefone: "5511999999999"})
	if err != nil {
		t.Fatalf("CriarInstancia falhou: %v", err)
	}
	if inst.ID != "inst-123" {
		t.Errorf("esperava id inst-123, obteve %s", inst.ID)
	}
	if mockSrv.receivedToken != "Bearer chave-teste-123" {
		t.Errorf("token no header inesperado: %s", mockSrv.receivedToken)
	}

	// 2. Erro mapeado InvalidArgument -> ErrInvalido
	_, err = cli.CriarInstancia(ctx, syncz.EntradaCriarInstancia{Nome: "invalido"})
	if !errors.Is(err, syncz.ErrInvalido) {
		t.Errorf("esperava ErrInvalido, obteve: %v", err)
	}

	// 3. Erro mapeado NotFound -> ErrNaoEncontrado
	_, err = cli.CriarInstancia(ctx, syncz.EntradaCriarInstancia{Nome: "nao-encontrado"})
	if !errors.Is(err, syncz.ErrNaoEncontrado) {
		t.Errorf("esperava ErrNaoEncontrado, obteve: %v", err)
	}

	// 4. Envio de mensagem
	rec, err := cli.Enviar(ctx, syncz.EntradaEnvio{
		InstanciaID:       "inst-123",
		Para:              "5511999999999",
		Texto:             "ola mundo",
		ChaveIdempotencia: "chave-1",
	})
	if err != nil {
		t.Fatalf("Enviar falhou: %v", err)
	}
	if rec.MensagemID != "msg-999" || rec.Duplicada {
		t.Errorf("recibo inesperado: %+v", rec)
	}

	// 5. Envio duplicado
	recDup, err := cli.Enviar(ctx, syncz.EntradaEnvio{
		InstanciaID:       "inst-123",
		Para:              "5511999999999",
		Texto:             "ola mundo",
		ChaveIdempotencia: "dup-key",
	})
	if err != nil {
		t.Fatalf("Enviar duplicado falhou: %v", err)
	}
	if !recDup.Duplicada {
		t.Errorf("esperava Duplicada=true, obteve: %+v", recDup)
	}

	// 6. Erro de autenticação
	_, err = cli.Enviar(ctx, syncz.EntradaEnvio{
		InstanciaID: "inst-123",
		Texto:       "erro-unauthenticated",
	})
	if !errors.Is(err, syncz.ErrCredencial) {
		t.Errorf("esperava ErrCredencial, obteve: %v", err)
	}

	// 7. CriarGrupo por padrao (Assincrono=false, zero-value) PEDE espera
	// sincrona -- task 349: antes desta correcao o proto nem tinha campo wait,
	// entao nao havia como o SDK pedir isso, e CriarGrupo sempre voltava com
	// JID vazio quando edge e node sao processos separados (SEMPRE em
	// staging/producao). O mock devolve JID de verdade so' quando Wait=true.
	grupo, err := cli.CriarGrupo(ctx, syncz.EntradaGrupo{
		InstanciaID:   "inst-123",
		Assunto:       "Grupo Teste",
		Participantes: []string{"5511988887777"},
	})
	if err != nil {
		t.Fatalf("CriarGrupo falhou: %v", err)
	}
	if !mockSrv.ultimoCreateGroupWait {
		t.Fatal("REGRESSAO (task 349): CriarGrupo por padrao nao pediu wait=true ao servidor")
	}
	if mockSrv.ultimoCreateGroupTimeoutMs <= 0 {
		t.Errorf("esperava wait_timeout_ms > 0 no default, obteve %d", mockSrv.ultimoCreateGroupTimeoutMs)
	}
	if grupo.JID == "" {
		t.Errorf("esperava JID preenchido com wait=true, veio vazio (status=%q op_id=%q)", grupo.Status, grupo.OpID)
	}

	// 8. CriarGrupo com Assincrono=true explicito: sem wait, e o chamador
	// enxerga o recibo de aceite (Status/OpID) em vez de um JID vazio
	// silencioso.
	grupoAsync, err := cli.CriarGrupo(ctx, syncz.EntradaGrupo{
		InstanciaID:    "inst-123",
		Assunto:        "Grupo Assincrono",
		Participantes:  []string{"5511988887777"},
		EsperaSincrona: syncz.EsperaSincrona{Assincrono: true},
	})
	if err != nil {
		t.Fatalf("CriarGrupo assincrono falhou: %v", err)
	}
	if mockSrv.ultimoCreateGroupWait {
		t.Fatal("Assincrono=true deveria mandar wait=false ao servidor")
	}
	if grupoAsync.Status != "accepted" || grupoAsync.OpID == "" {
		t.Errorf("esperava recibo de aceite (status=accepted, op_id preenchido), obteve: %+v", grupoAsync)
	}
}
