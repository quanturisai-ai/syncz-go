package syncz

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	synczv1 "github.com/quanturisai-ai/syncz-go/synczv1"
)

type clienteGRPC struct {
	conn   *grpc.ClientConn
	cli    synczv1.SyncZapClient
	prazo  time.Duration
	fechar func() error
}

func novoClienteGRPC(o Opcoes) (Cliente, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(credencialUnary(o.ChaveAPI)),
		grpc.WithStreamInterceptor(credencialStream(o.ChaveAPI)),
	}

	conn, err := grpc.NewClient(o.EnderecoGRPC, opts...)
	if err != nil {
		return nil, fmt.Errorf("syncz: dial grpc: %w", err)
	}

	return &clienteGRPC{
		conn:   conn,
		cli:    synczv1.NewSyncZapClient(conn),
		prazo:  o.Prazo,
		fechar: conn.Close,
	}, nil
}

// NovoClienteGRPCComConn cria um cliente a partir de uma conexão gRPC existente (útil para testes com bufconn).
func NovoClienteGRPCComConn(cc grpc.ClientConnInterface, fechar func() error) Cliente {
	return &clienteGRPC{
		cli:    synczv1.NewSyncZapClient(cc),
		prazo:  30 * time.Second,
		fechar: fechar,
	}
}

func credencialUnary(chave string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+chave)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func credencialStream(chave string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+chave)
		return streamer(ctx, desc, cc, method, opts...)
	}
}

func (c *clienteGRPC) comPrazo(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	if c.prazo > 0 {
		return context.WithTimeout(ctx, c.prazo)
	}
	return ctx, func() {}
}

func (c *clienteGRPC) Fechar() error {
	if c.fechar != nil {
		return c.fechar()
	}
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *clienteGRPC) CriarInstancia(ctx context.Context, e EntradaCriarInstancia) (Instancia, error) {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	resp, err := c.cli.CreateInstance(ctx, &synczv1.CreateInstanceRequest{
		Name:  e.Nome,
		Phone: e.Telefone,
	})
	if err != nil {
		return Instancia{}, mapearErroGRPC(err)
	}
	return instanciaDoProto(resp), nil
}

func (c *clienteGRPC) ObterInstancia(ctx context.Context, id string) (Instancia, error) {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	resp, err := c.cli.GetInstance(ctx, &synczv1.GetInstanceRequest{Id: id})
	if err != nil {
		return Instancia{}, mapearErroGRPC(err)
	}
	return instanciaDoProto(resp), nil
}

func (c *clienteGRPC) ListarInstancias(ctx context.Context, e EntradaListarInstancias) (ListaInstancias, error) {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	resp, err := c.cli.ListInstances(ctx, &synczv1.ListInstancesRequest{
		Limit:  e.Limite,
		Offset: e.Offset,
	})
	if err != nil {
		return ListaInstancias{}, mapearErroGRPC(err)
	}

	lista := make([]Instancia, len(resp.GetInstances()))
	for i, item := range resp.GetInstances() {
		lista[i] = instanciaDoProto(item)
	}
	return ListaInstancias{
		Instancias: lista,
		Total:      resp.GetTotal(),
	}, nil
}

func (c *clienteGRPC) RevogarInstancia(ctx context.Context, id string) (Instancia, error) {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	resp, err := c.cli.RevokeInstance(ctx, &synczv1.RevokeInstanceRequest{Id: id})
	if err != nil {
		return Instancia{}, mapearErroGRPC(err)
	}
	return instanciaDoProto(resp), nil
}

func (c *clienteGRPC) NovoLinkWizard(ctx context.Context, instanciaID string) (LinkWizard, error) {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	resp, err := c.cli.NewWizardLink(ctx, &synczv1.NewWizardLinkRequest{InstanceId: instanciaID})
	if err != nil {
		return LinkWizard{}, mapearErroGRPC(err)
	}
	var exp time.Time
	if resp.GetExpiresAt() != nil {
		exp = resp.GetExpiresAt().AsTime()
	}
	return LinkWizard{
		InstanciaID: resp.GetInstanceId(),
		Token:       resp.GetToken(),
		URL:         resp.GetUrl(),
		ExpiraEm:    exp,
	}, nil
}

func (c *clienteGRPC) EstadoPareamento(ctx context.Context, instanciaID string) (Pareamento, error) {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	resp, err := c.cli.PairingState(ctx, &synczv1.PairingStateRequest{InstanceId: instanciaID})
	if err != nil {
		return Pareamento{}, mapearErroGRPC(err)
	}
	var exp time.Time
	if resp.GetExpiresAt() != nil {
		exp = resp.GetExpiresAt().AsTime()
	}
	return Pareamento{
		QRCode:   resp.GetQrCode(),
		PairCode: resp.GetPairCode(),
		Seq:      int(resp.GetSeq()),
		ExpiraEm: exp,
		Pareado:  resp.GetPaired(),
		Telefone: resp.GetPhone(),
	}, nil
}

func (c *clienteGRPC) Contrato(ctx context.Context, instanciaID string) (ContratoInfo, error) {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	resp, err := c.cli.GetContract(ctx, &synczv1.GetContractRequest{InstanceId: instanciaID})
	if err != nil {
		return ContratoInfo{}, mapearErroGRPC(err)
	}

	obrig := make([]EscopoInfo, len(resp.GetRequiredScopes()))
	for i, s := range resp.GetRequiredScopes() {
		obrig[i] = EscopoInfo{Valor: s.GetValue(), Descricao: s.GetDescription()}
	}

	opc := make([]EscopoInfo, len(resp.GetOptionalScopes()))
	for i, s := range resp.GetOptionalScopes() {
		opc[i] = EscopoInfo{Valor: s.GetValue(), Descricao: s.GetDescription()}
	}

	return ContratoInfo{
		TemplateID:          resp.GetTemplateId(),
		Nome:                resp.GetName(),
		Versao:              int(resp.GetVersion()),
		TermoMarkdown:       resp.GetTermsMarkdown(),
		TermoSHA256:         resp.GetTermsSha256(),
		EscoposObrigatorios: obrig,
		EscoposOpcionais:    opc,
		MarcaNome:           resp.GetBrandName(),
		MarcaLogoURL:        resp.GetBrandLogoUrl(),
		MarcaCor:            resp.GetBrandColor(),
		IntroMarkdown:       resp.GetIntroMarkdown(),
	}, nil
}

func (c *clienteGRPC) RegistrarAceite(ctx context.Context, e EntradaAceite) error {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	_, err := c.cli.GrantConsent(ctx, &synczv1.GrantConsentRequest{
		InstanceId: e.InstanciaID,
		Scopes:     e.EscoposOpcionais,
		UserIp:     e.IPTitular,
		UserAgent:  e.UserAgentTitular,
	})
	return mapearErroGRPC(err)
}

func (c *clienteGRPC) Enviar(ctx context.Context, e EntradaEnvio) (Recibo, error) {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	resp, err := c.cli.SendMessage(ctx, &synczv1.SendMessageRequest{
		InstanceId:     e.InstanciaID,
		To:             e.Para,
		Text:           e.Texto,
		IdempotencyKey: e.ChaveIdempotencia,
	})
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.AlreadyExists {
			return Recibo{MensagemID: st.Message(), Status: "duplicate", Duplicada: true}, nil
		}
		return Recibo{}, mapearErroGRPC(err)
	}

	var sentAt time.Time
	if resp.GetSentAt() != nil {
		sentAt = resp.GetSentAt().AsTime()
	}
	dup := strings.EqualFold(resp.GetStatus(), "duplicate")
	return Recibo{
		MensagemID: resp.GetMessageId(),
		Status:     resp.GetStatus(),
		EnviadoEm:  sentAt,
		Duplicada:  dup,
	}, nil
}

func (c *clienteGRPC) EnviarMidia(ctx context.Context, e EntradaEnvioMidia) (Recibo, error) {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	stream, err := c.cli.SendMedia(ctx)
	if err != nil {
		return Recibo{}, mapearErroGRPC(err)
	}

	if err := stream.Send(&synczv1.SendMediaChunk{
		Data: &synczv1.SendMediaChunk_Metadata{
			Metadata: &synczv1.MediaMetadata{
				InstanceId:     e.InstanciaID,
				To:             e.Para,
				MimeType:       e.MimeType,
				Filename:       e.NomeArquivo,
				Caption:        e.Legenda,
				IdempotencyKey: e.ChaveIdempotencia,
			},
		},
	}); err != nil {
		return Recibo{}, mapearErroGRPC(err)
	}

	chunkSize := 64 * 1024
	for offset := 0; offset < len(e.Conteudo); offset += chunkSize {
		end := offset + chunkSize
		if end > len(e.Conteudo) {
			end = len(e.Conteudo)
		}
		if err := stream.Send(&synczv1.SendMediaChunk{
			Data: &synczv1.SendMediaChunk_Chunk{
				Chunk: e.Conteudo[offset:end],
			},
		}); err != nil {
			return Recibo{}, mapearErroGRPC(err)
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.AlreadyExists {
			return Recibo{MensagemID: st.Message(), Status: "duplicate", Duplicada: true}, nil
		}
		return Recibo{}, mapearErroGRPC(err)
	}

	var sentAt time.Time
	if resp.GetSentAt() != nil {
		sentAt = resp.GetSentAt().AsTime()
	}
	dup := strings.EqualFold(resp.GetStatus(), "duplicate")
	return Recibo{
		MensagemID: resp.GetMessageId(),
		Status:     resp.GetStatus(),
		EnviadoEm:  sentAt,
		Duplicada:  dup,
	}, nil
}

func (c *clienteGRPC) CriarGrupo(ctx context.Context, e EntradaGrupo) (Grupo, error) {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	wait, timeoutMs := e.protoWait()
	resp, err := c.cli.CreateGroup(ctx, &synczv1.CreateGroupRequest{
		InstanceId:    e.InstanciaID,
		Subject:       e.Assunto,
		Participants:  e.Participantes,
		Wait:          wait,
		WaitTimeoutMs: timeoutMs,
	})
	if err != nil {
		return Grupo{}, mapearErroGRPC(err)
	}
	return Grupo{
		JID:           resp.GetJid(),
		Assunto:       resp.GetSubject(),
		Participantes: resp.GetParticipants(),
		DonoJID:       resp.GetOwnerJid(),
		Status:        resp.GetStatus(),
		OpID:          resp.GetOpId(),
	}, nil
}

func (c *clienteGRPC) AtualizarNomeGrupo(ctx context.Context, e EntradaAtualizarNomeGrupo) (Grupo, error) {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	wait, timeoutMs := e.protoWait()
	resp, err := c.cli.UpdateGroupName(ctx, &synczv1.UpdateGroupNameRequest{
		InstanceId:    e.InstanciaID,
		GroupJid:      e.GrupoJID,
		Subject:       e.Assunto,
		Wait:          wait,
		WaitTimeoutMs: timeoutMs,
	})
	if err != nil {
		return Grupo{}, mapearErroGRPC(err)
	}
	return Grupo{
		JID:           resp.GetJid(),
		Assunto:       resp.GetSubject(),
		Participantes: resp.GetParticipants(),
		DonoJID:       resp.GetOwnerJid(),
		Status:        resp.GetStatus(),
		OpID:          resp.GetOpId(),
	}, nil
}

func (c *clienteGRPC) AtualizarFotoGrupo(ctx context.Context, e EntradaAtualizarFotoGrupo) (Grupo, error) {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	stream, err := c.cli.UpdateGroupPhoto(ctx)
	if err != nil {
		return Grupo{}, mapearErroGRPC(err)
	}

	wait, timeoutMs := e.protoWait()
	if err := stream.Send(&synczv1.GroupPhotoChunk{
		Data: &synczv1.GroupPhotoChunk_Metadata{
			Metadata: &synczv1.GroupPhotoMetadata{
				InstanceId:    e.InstanciaID,
				GroupJid:      e.GrupoJID,
				MimeType:      e.MimeType,
				Wait:          wait,
				WaitTimeoutMs: timeoutMs,
			},
		},
	}); err != nil {
		return Grupo{}, mapearErroGRPC(err)
	}

	if len(e.Foto) > 0 {
		if err := stream.Send(&synczv1.GroupPhotoChunk{
			Data: &synczv1.GroupPhotoChunk_Chunk{
				Chunk: e.Foto,
			},
		}); err != nil {
			return Grupo{}, mapearErroGRPC(err)
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return Grupo{}, mapearErroGRPC(err)
	}
	return Grupo{
		JID:           resp.GetJid(),
		Assunto:       resp.GetSubject(),
		Participantes: resp.GetParticipants(),
		DonoJID:       resp.GetOwnerJid(),
		Status:        resp.GetStatus(),
		OpID:          resp.GetOpId(),
	}, nil
}

func (c *clienteGRPC) AdicionarParticipantes(ctx context.Context, e EntradaParticipantes) error {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	wait, timeoutMs := e.protoWait()
	_, err := c.cli.AddParticipants(ctx, &synczv1.AddParticipantsRequest{
		InstanceId:    e.InstanciaID,
		GroupJid:      e.GrupoJID,
		Participants:  e.Participantes,
		Wait:          wait,
		WaitTimeoutMs: timeoutMs,
	})
	return mapearErroGRPC(err)
}

func (c *clienteGRPC) RemoverParticipantes(ctx context.Context, e EntradaParticipantes) error {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	wait, timeoutMs := e.protoWait()
	_, err := c.cli.RemoveParticipants(ctx, &synczv1.RemoveParticipantsRequest{
		InstanceId:    e.InstanciaID,
		GroupJid:      e.GrupoJID,
		Participants:  e.Participantes,
		Wait:          wait,
		WaitTimeoutMs: timeoutMs,
	})
	return mapearErroGRPC(err)
}

func (c *clienteGRPC) LinkConviteGrupo(ctx context.Context, e EntradaLinkConvite) (string, error) {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	resp, err := c.cli.GroupInviteLink(ctx, &synczv1.GroupInviteLinkRequest{
		InstanceId: e.InstanciaID,
		GroupJid:   e.GrupoJID,
		Reset_:     e.Reset,
	})
	if err != nil {
		return "", mapearErroGRPC(err)
	}
	return resp.GetInviteUrl(), nil
}

func (c *clienteGRPC) SairGrupo(ctx context.Context, e EntradaSairGrupo) error {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	wait, timeoutMs := e.protoWait()
	_, err := c.cli.LeaveGroup(ctx, &synczv1.LeaveGroupRequest{
		InstanceId:    e.InstanciaID,
		GroupJid:      e.GrupoJID,
		Wait:          wait,
		WaitTimeoutMs: timeoutMs,
	})
	return mapearErroGRPC(err)
}

func (c *clienteGRPC) FixarChat(ctx context.Context, e EntradaFixarChat) error {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	wait, timeoutMs := e.protoWait()
	_, err := c.cli.PinChat(ctx, &synczv1.PinChatRequest{
		InstanceId:    e.InstanciaID,
		ChatJid:       e.ChatJID,
		Pin:           e.Fixar,
		Wait:          wait,
		WaitTimeoutMs: timeoutMs,
	})
	return mapearErroGRPC(err)
}

func (c *clienteGRPC) MarcarLido(ctx context.Context, e EntradaMarcarLido) error {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	wait, timeoutMs := e.protoWait()
	_, err := c.cli.MarkRead(ctx, &synczv1.MarkReadRequest{
		InstanceId:    e.InstanciaID,
		ChatJid:       e.ChatJID,
		MessageIds:    e.MensagensIDs,
		Wait:          wait,
		WaitTimeoutMs: timeoutMs,
	})
	return mapearErroGRPC(err)
}

func (c *clienteGRPC) DefinirLeituraChat(ctx context.Context, e EntradaDefinirLeituraChat) error {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	wait, timeoutMs := e.protoWait()
	_, err := c.cli.SetChatReadState(ctx, &synczv1.SetChatReadStateRequest{
		InstanceId:    e.InstanciaID,
		ChatJid:       e.ChatJID,
		Read:          e.Lida,
		Wait:          wait,
		WaitTimeoutMs: timeoutMs,
	})
	return mapearErroGRPC(err)
}

func (c *clienteGRPC) VerificarNumeros(ctx context.Context, e EntradaVerificarNumeros) ([]NumeroInfo, error) {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	resp, err := c.cli.CheckNumbers(ctx, &synczv1.CheckNumbersRequest{
		InstanceId: e.InstanciaID,
		Phones:     e.Telefones,
	})
	if err != nil {
		return nil, mapearErroGRPC(err)
	}

	itens := make([]NumeroInfo, len(resp.GetResults()))
	for i, n := range resp.GetResults() {
		itens[i] = NumeroInfo{
			Telefone:   n.GetPhone(),
			NoWhatsApp: n.GetIsInWhatsapp(),
			JID:        n.GetJid(),
			LID:        n.GetLid(),
		}
	}
	return itens, nil
}

func (c *clienteGRPC) ObterPolitica(ctx context.Context, instanciaID string) (Politica, error) {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	resp, err := c.cli.GetPolicy(ctx, &synczv1.GetPolicyRequest{
		InstanceId: instanciaID,
	})
	if err != nil {
		return Politica{}, mapearErroGRPC(err)
	}
	return Politica{
		InstanciaID: resp.GetInstanceId(),
		ConfigJSON:  resp.GetConfigJson(),
	}, nil
}

func (c *clienteGRPC) AtualizarPolitica(ctx context.Context, instanciaID string, configJSON []byte) (Politica, error) {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	resp, err := c.cli.UpdatePolicy(ctx, &synczv1.UpdatePolicyRequest{
		InstanceId: instanciaID,
		Policy: &synczv1.Policy{
			InstanceId: instanciaID,
			ConfigJson: configJSON,
		},
	})
	if err != nil {
		return Politica{}, mapearErroGRPC(err)
	}
	return Politica{
		InstanciaID: resp.GetInstanceId(),
		ConfigJSON:  resp.GetConfigJson(),
	}, nil
}

func (c *clienteGRPC) ObterUso(ctx context.Context, tenantID string) (Uso, error) {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	resp, err := c.cli.GetUsage(ctx, &synczv1.GetUsageRequest{
		TenantId: tenantID,
	})
	if err != nil {
		return Uso{}, mapearErroGRPC(err)
	}
	return Uso{
		TenantID:          resp.GetTenantId(),
		MensagensEnviadas: resp.GetMessagesSent(),
		InstanciasAtivas:  resp.GetInstancesActive(),
	}, nil
}

func instanciaDoProto(p *synczv1.Instance) Instancia {
	if p == nil {
		return Instancia{}
	}
	var created, updated time.Time
	if p.GetCreatedAt() != nil {
		created = p.GetCreatedAt().AsTime()
	}
	if p.GetUpdatedAt() != nil {
		updated = p.GetUpdatedAt().AsTime()
	}
	return Instancia{
		ID:           p.GetId(),
		TenantID:     p.GetTenantId(),
		Nome:         p.GetName(),
		Telefone:     p.GetPhone(),
		Status:       p.GetStatus(),
		CriadaEm:     created,
		AtualizadaEm: updated,
	}
}

// suppress unused import warning
var _ = io.EOF
