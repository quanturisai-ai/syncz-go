package syncz

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

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
		grpc.WithTransportCredentials(credencialTransporte(o)),
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

// usaTLSGRPC decide o transporte do canal gRPC. Ordem: GRPCSemTLS desliga,
// TLSGRPC liga, e sem opção explícita só a porta 443 explícita liga TLS. Um
// host não-loopback sem TLS continua em texto puro de propósito: redes internas
// (compose, k8s) usam "gateway:50051" em h2c desde a v0.1.0.
func usaTLSGRPC(o Opcoes) bool {
	if o.GRPCSemTLS {
		return false
	}
	if o.TLSGRPC != nil {
		return true
	}
	return portaGRPC(o.EnderecoGRPC) == "443"
}

// portaGRPC extrai a porta de um alvo gRPC ("host:porta", "dns:///host:porta",
// "passthrough:///host:porta"). Alvo sem porta explícita ou unix devolve "".
func portaGRPC(alvo string) string {
	if strings.HasPrefix(alvo, "unix:") || strings.HasPrefix(alvo, "unix-abstract:") {
		return ""
	}
	if i := strings.Index(alvo, ":///"); i >= 0 {
		alvo = alvo[i+len(":///"):]
	} else if i := strings.Index(alvo, "://"); i >= 0 {
		alvo = alvo[i+len("://"):]
		if j := strings.Index(alvo, "/"); j >= 0 {
			alvo = alvo[j+1:]
		}
	}
	_, porta, err := net.SplitHostPort(alvo)
	if err != nil {
		return ""
	}
	return porta
}

// credencialTransporte monta as credenciais de transporte a partir de Opcoes.
// Com TLS, a config do chamador é clonada e ganha piso TLS 1.2; RootCAs nil
// verifica o certificado contra as raízes do sistema.
func credencialTransporte(o Opcoes) credentials.TransportCredentials {
	if !usaTLSGRPC(o) {
		return insecure.NewCredentials()
	}
	cfg := &tls.Config{}
	if o.TLSGRPC != nil {
		cfg = o.TLSGRPC.Clone()
	}
	if cfg.MinVersion == 0 {
		cfg.MinVersion = tls.VersionTLS12
	}
	return credentials.NewTLS(cfg)
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

// Desparear (task 589, CA-16/CA-20) desfaz o pareamento sem apagar a instancia.
func (c *clienteGRPC) Desparear(ctx context.Context, instanciaID string) (Instancia, error) {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	resp, err := c.cli.UnpairInstance(ctx, &synczv1.UnpairInstanceRequest{Id: instanciaID})
	if err != nil {
		return Instancia{}, mapearErroGRPC(err)
	}
	return instanciaDoProto(resp), nil
}

// SolicitarConsentimento (task 589, CA-16/CA-13) pede ao titular, por DM
// pelo numero pareado, que autorize a instancia.
func (c *clienteGRPC) SolicitarConsentimento(ctx context.Context, e EntradaSolicitarConsentimento) (PedidoConsentimento, error) {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	resp, err := c.cli.RequestConsent(ctx, &synczv1.RequestConsentRequest{
		InstanceId: e.InstanciaID,
		Via:        e.Via,
		To:         e.Para,
		Text:       e.Texto,
	})
	if err != nil {
		return PedidoConsentimento{}, mapearErroGRPC(err)
	}
	var exp time.Time
	if resp.GetExpiresAt() != nil {
		exp = resp.GetExpiresAt().AsTime()
	}
	return PedidoConsentimento{
		RequestID: resp.GetRequestId(),
		ExpiraEm:  exp,
		Link:      resp.GetLink(),
	}, nil
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

	req := &synczv1.GrantConsentRequest{
		InstanceId: e.InstanciaID,
		Scopes:     e.EscoposOpcionais,
		UserIp:     e.IPTitular,
		UserAgent:  e.UserAgentTitular,
	}
	if len(e.Evidencia) > 0 {
		ev, err := structpb.NewStruct(e.Evidencia)
		if err != nil {
			return novoErroAPI(ErrInvalido, fmt.Sprintf("evidencia invalida: %v", err))
		}
		req.Evidence = ev
	}

	_, err := c.cli.GrantConsent(ctx, req)
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

// StatusMensagem: o proto gRPC (proto/syncz/v1/syncz.proto) não define nenhum
// RPC equivalente a GET .../messages/{message_id} -- só SendMessage/SendMedia
// (aceite) e StreamEvents (webhook). Decisão registrada na task 565, mesmo
// critério da 564 (AcompanharPareamento): lá o método ficou FORA da interface
// Cliente principal porque só o REST tinha stream; aqui a task pede
// explicitamente que StatusMensagem ENTRE na interface principal, então
// clienteGRPC precisa satisfazer o contrato -- devolve ErrTransporteNaoSuporta
// de forma explícita (comparável via errors.Is) em vez de inventar um valor ou
// falhar de outro jeito. Adicionar o RPC no proto é decisão maior (contrato
// compartilhado entre server e SDK, cross-repo) fora do escopo desta tarefa
// isolada de SDK client -- fica para uma task de extensão de contrato.
func (c *clienteGRPC) StatusMensagem(_ context.Context, _, _ string) (EstadoMensagem, error) {
	return EstadoMensagem{}, ErrTransporteNaoSuporta
}

// StatusOperacao: mesma decisão da task 565/StatusMensagem, agora para o
// GET .../operations/{op_id}. Conferido em proto/syncz/v1/syncz.proto (task
// 566): não existe RPC GetOperation nem equivalente -- os RPCs de grupo
// (CreateGroup, UpdateGroupName, ...) sempre devolvem o tipo final
// diretamente, sem op_id de acompanhamento assíncrono (o proto não modela o
// caminho "accepted" que o REST tem via ?wait=false). Devolve
// ErrTransporteNaoSuporta de forma explícita em vez de inventar um valor;
// adicionar o RPC é decisão de contrato cross-repo, fora do escopo desta
// tarefa isolada de SDK client.
func (c *clienteGRPC) StatusOperacao(_ context.Context, _, _ string) (EstadoOperacao, error) {
	return EstadoOperacao{}, ErrTransporteNaoSuporta
}

// AtualizarEstadoDesejado: o proto gRPC (proto/syncz/v1/syncz.proto) não tem
// nenhum RPC equivalente a PATCH .../instances/{id} -- os RPCs de instância
// existentes (CreateInstance, GetInstance, ListInstances, RevokeInstance,
// GenerateWizardLink) não cobrem atualização de desired_state, e não há
// UpdateInstance/SetDesiredState no proto (task 570, mesmo critério das
// tasks 565/566: conferido antes de decidir, não assumido). Devolve
// ErrTransporteNaoSuporta de forma explícita em vez de inventar um valor;
// adicionar o RPC é decisão de contrato cross-repo, fora do escopo desta
// tarefa isolada de SDK client.
func (c *clienteGRPC) AtualizarEstadoDesejado(_ context.Context, _, _ string) (Instancia, error) {
	return Instancia{}, ErrTransporteNaoSuporta
}

// enqueteFromProto traduz o Poll do proto (CreatePoll/ClosePoll) para o tipo
// Enquete do SDK -- ver proto/syncz/v1/syncz.proto (message Poll) e
// internal/transport/grpc/server_polls.go (pollViewParaProto, o inverso).
func enqueteFromProto(p *synczv1.Poll) Enquete {
	return Enquete{
		PollID:        p.GetPollId(),
		InstanciaID:   p.GetInstanceId(),
		GrupoJID:      p.GetGroupJid(),
		Pergunta:      p.GetQuestion(),
		Opcoes:        p.GetOptions(),
		Selecionaveis: int(p.GetSelectableCount()),
		Encerrada:     p.GetIsClosed(),
		CriadaEm:      parseUnixSeconds(p.GetCreatedAt()),
	}
}

// CriarEnquete: diferente de StatusMensagem/StatusOperacao (tasks 565/566),
// o proto TEM RPC equivalente -- CreatePoll, com wait/wait_timeout_ms iguais
// aos de CreateGroup (ver internal/transport/grpc/server_polls.go). Sem
// ErrTransporteNaoSuporta aqui.
func (c *clienteGRPC) CriarEnquete(ctx context.Context, e EntradaCriarEnquete) (Enquete, error) {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	wait, timeoutMs := e.protoWait()
	resp, err := c.cli.CreatePoll(ctx, &synczv1.CreatePollRequest{
		InstanceId:      e.InstanciaID,
		GroupJid:        e.GrupoJID,
		Question:        e.Pergunta,
		Options:         e.Opcoes,
		SelectableCount: int32(e.Selecionaveis),
		Wait:            wait,
		WaitTimeoutMs:   timeoutMs,
	})
	if err != nil {
		return Enquete{}, mapearErroGRPC(err)
	}
	return enqueteFromProto(resp), nil
}

// ResultadoEnquete consome o RPC GetPollResults.
func (c *clienteGRPC) ResultadoEnquete(ctx context.Context, instanciaID, pollID string) (EnqueteResultado, error) {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	resp, err := c.cli.GetPollResults(ctx, &synczv1.GetPollResultsRequest{
		InstanceId: instanciaID,
		PollId:     pollID,
	})
	if err != nil {
		return EnqueteResultado{}, mapearErroGRPC(err)
	}

	tallies := resp.GetTallies()
	opcoes := make([]EnqueteOpcaoResultado, len(tallies))
	for i, t := range tallies {
		opcoes[i] = EnqueteOpcaoResultado{
			Opcao:    t.GetOption(),
			Votos:    int(t.GetCount()),
			Votantes: t.GetVoters(),
		}
	}
	return EnqueteResultado{
		PollID:        resp.GetPollId(),
		Opcoes:        opcoes,
		TotalVotantes: int(resp.GetTotalVoters()),
		CalculadoEm:   parseUnixSeconds(resp.GetComputedAt()),
	}, nil
}

// EncerrarEnquete consome o RPC ClosePoll.
func (c *clienteGRPC) EncerrarEnquete(ctx context.Context, instanciaID, pollID string) (Enquete, error) {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	resp, err := c.cli.ClosePoll(ctx, &synczv1.ClosePollRequest{
		InstanceId: instanciaID,
		PollId:     pollID,
	})
	if err != nil {
		return Enquete{}, mapearErroGRPC(err)
	}
	return enqueteFromProto(resp), nil
}

// eventoGrupoFromProto traduz o GroupEvent do proto para o tipo EventoGrupo
// do SDK -- ver proto/syncz/v1/syncz.proto (message GroupEvent) e
// internal/transport/grpc/server_group_events.go (groupEventDetailParaProto/
// groupEventViewParaProto, o inverso).
func eventoGrupoFromProto(p *synczv1.GroupEvent) EventoGrupo {
	return EventoGrupo{
		EventoID:                p.GetEventId(),
		InstanciaID:             p.GetInstanceId(),
		GrupoJID:                p.GetGroupJid(),
		Titulo:                  p.GetTitle(),
		Descricao:               p.GetDescription(),
		Inicio:                  parseUnixSeconds(p.GetStartTime()),
		Fim:                     parseUnixSeconds(p.GetEndTime()),
		LembreteAntecedencia:    time.Duration(p.GetReminderOffsetSec()) * time.Second,
		LocalNome:               p.GetLocationName(),
		LinkEntrada:             p.GetJoinLink(),
		Cancelado:               p.GetIsCanceled(),
		ChamadaAgendada:         p.GetIsScheduleCall(),
		PermiteConvidadosExtras: p.GetExtraGuestsAllowed(),
		CriadoEm:                parseUnixSeconds(p.GetCreatedAt()),
		AtualizadoEm:            parseUnixSeconds(p.GetUpdatedAt()),
	}
}

// stringParaEventResponseType traduz a string de resposta RSVP
// (RespostaEventoGrupo* em syncz.go) para o enum do proto -- espelha
// internal/transport/grpc/server_group_events.go (stringParaEventResponseType,
// o mesmo mapeamento do lado servidor).
func stringParaEventResponseType(s string) synczv1.EventResponseType {
	switch s {
	case RespostaEventoGrupoIndo:
		return synczv1.EventResponseType_EVENT_RESPONSE_TYPE_GOING
	case RespostaEventoGrupoNaoIndo:
		return synczv1.EventResponseType_EVENT_RESPONSE_TYPE_NOT_GOING
	case RespostaEventoGrupoTalvez:
		return synczv1.EventResponseType_EVENT_RESPONSE_TYPE_MAYBE
	default:
		return synczv1.EventResponseType_EVENT_RESPONSE_TYPE_UNSPECIFIED
	}
}

// eventResponseTypeParaString e' o inverso de stringParaEventResponseType.
func eventResponseTypeParaString(t synczv1.EventResponseType) string {
	switch t {
	case synczv1.EventResponseType_EVENT_RESPONSE_TYPE_GOING:
		return RespostaEventoGrupoIndo
	case synczv1.EventResponseType_EVENT_RESPONSE_TYPE_NOT_GOING:
		return RespostaEventoGrupoNaoIndo
	case synczv1.EventResponseType_EVENT_RESPONSE_TYPE_MAYBE:
		return RespostaEventoGrupoTalvez
	default:
		return ""
	}
}

// CriarEventoGrupo: o proto TEM RPC equivalente -- CreateGroupEvent, com
// wait/wait_timeout_ms iguais aos de CreateGroup/CreatePoll (task 559).
func (c *clienteGRPC) CriarEventoGrupo(ctx context.Context, e EntradaCriarEventoGrupo) (EventoGrupo, error) {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	var endTime int64
	if !e.Fim.IsZero() {
		endTime = e.Fim.Unix()
	}

	wait, timeoutMs := e.protoWait()
	resp, err := c.cli.CreateGroupEvent(ctx, &synczv1.CreateGroupEventRequest{
		InstanceId:         e.InstanciaID,
		GroupJid:           e.GrupoJID,
		Title:              e.Titulo,
		Description:        e.Descricao,
		StartTime:          e.Inicio.Unix(),
		EndTime:            endTime,
		ReminderOffsetSec:  int64(e.LembreteAntecedencia.Seconds()),
		LocationName:       e.LocalNome,
		JoinLink:           e.LinkEntrada,
		IsScheduleCall:     e.ChamadaAgendada,
		ExtraGuestsAllowed: e.PermiteConvidadosExtras,
		EnsureReminder:     e.GarantirLembrete,
		Wait:               wait,
		WaitTimeoutMs:      timeoutMs,
	})
	if err != nil {
		return EventoGrupo{}, mapearErroGRPC(err)
	}
	return eventoGrupoFromProto(resp), nil
}

// AtualizarEventoGrupo consome o RPC UpdateGroupEvent -- só os campos
// ponteiro presentes em e são enviados (mesma convenção do REST).
func (c *clienteGRPC) AtualizarEventoGrupo(ctx context.Context, e EntradaAtualizarEventoGrupo) (EventoGrupo, error) {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	req := &synczv1.UpdateGroupEventRequest{
		InstanceId:  e.InstanciaID,
		EventId:     e.EventoID,
		Title:       e.Titulo,
		Description: e.Descricao,
	}
	if e.Inicio != nil {
		st := e.Inicio.Unix()
		req.StartTime = &st
	}
	if e.Fim != nil {
		et := e.Fim.Unix()
		req.EndTime = &et
	}
	if e.LembreteAntecedencia != nil {
		ro := int64(e.LembreteAntecedencia.Seconds())
		req.ReminderOffsetSec = &ro
	}
	req.LocationName = e.LocalNome
	req.JoinLink = e.LinkEntrada
	req.Wait, req.WaitTimeoutMs = e.protoWait()

	resp, err := c.cli.UpdateGroupEvent(ctx, req)
	if err != nil {
		return EventoGrupo{}, mapearErroGRPC(err)
	}
	return eventoGrupoFromProto(resp), nil
}

// CancelarEventoGrupo consome o RPC CancelGroupEvent.
func (c *clienteGRPC) CancelarEventoGrupo(ctx context.Context, e EntradaCancelarEventoGrupo) (EventoGrupo, error) {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	wait, timeoutMs := e.protoWait()
	resp, err := c.cli.CancelGroupEvent(ctx, &synczv1.CancelGroupEventRequest{
		InstanceId:    e.InstanciaID,
		EventId:       e.EventoID,
		Wait:          wait,
		WaitTimeoutMs: timeoutMs,
	})
	if err != nil {
		return EventoGrupo{}, mapearErroGRPC(err)
	}
	return eventoGrupoFromProto(resp), nil
}

// ResponderEventoGrupo consome o RPC SendEventResponse.
func (c *clienteGRPC) ResponderEventoGrupo(ctx context.Context, e EntradaResponderEventoGrupo) (RespostaEventoGrupo, error) {
	ctx, cancel := c.comPrazo(ctx)
	defer cancel()

	wait, timeoutMs := e.protoWait()
	resp, err := c.cli.SendEventResponse(ctx, &synczv1.SendEventResponseRequest{
		InstanceId:      e.InstanciaID,
		EventId:         e.EventoID,
		Response:        stringParaEventResponseType(e.Resposta),
		ExtraGuestCount: e.ConvidadosExtras,
		Wait:            wait,
		WaitTimeoutMs:   timeoutMs,
	})
	if err != nil {
		return RespostaEventoGrupo{}, mapearErroGRPC(err)
	}
	return RespostaEventoGrupo{
		EventoID:   resp.GetEventId(),
		MensagemID: resp.GetMessageId(),
		Resposta:   eventResponseTypeParaString(resp.GetResponse()),
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
	var pareadoEm *time.Time
	if p.GetPairedAt() != nil {
		t := p.GetPairedAt().AsTime()
		pareadoEm = &t
	}
	return Instancia{
		ID:            p.GetId(),
		TenantID:      p.GetTenantId(),
		Nome:          p.GetName(),
		Telefone:      p.GetPhone(),
		Status:        p.GetStatus(),
		PareadoEm:     pareadoEm,
		Consentimento: consentimentoDoProto(p.GetConsent()),
		CriadaEm:      created,
		AtualizadaEm:  updated,
	}
}

// consentimentoDoProto (task 589, CA-16) traduz o bloco Consent do proto
// (task 583, CA-08) para ConsentimentoInfo.
func consentimentoDoProto(c *synczv1.Consent) ConsentimentoInfo {
	if c == nil {
		return ConsentimentoInfo{}
	}
	info := ConsentimentoInfo{
		Status:         c.GetStatus(),
		Origem:         c.GetOrigin(),
		ConcedidoPor:   c.GetGrantedBy(),
		VersaoContrato: int(c.GetContractVersion()),
		Escopos:        c.GetScopes(),
	}
	if c.GetRequestedAt() != nil {
		t := c.GetRequestedAt().AsTime()
		info.SolicitadoEm = &t
	}
	if c.GetGrantedAt() != nil {
		t := c.GetGrantedAt().AsTime()
		info.ConcedidoEm = &t
	}
	if c.GetRevokedAt() != nil {
		t := c.GetRevokedAt().AsTime()
		info.RevogadoEm = &t
	}
	return info
}

// suppress unused import warning
var _ = io.EOF
