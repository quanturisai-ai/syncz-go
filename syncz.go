package syncz

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// EntradaCriarInstancia parâmetros para criação de instância.
type EntradaCriarInstancia struct {
	Nome     string
	Telefone string
}

// Instancia representa os metadados de uma instância do WhatsApp.
//
// EstadoDesejado só vem preenchido quando o método que produziu este valor
// decodifica o campo "desired_state" da resposta (hoje, só
// AtualizarEstadoDesejado) -- os demais métodos (CriarInstancia,
// ObterInstancia, ListarInstancias, RevogarInstancia) não decodificam esse
// campo, mesma convenção de decodificação parcial já usada no resto do SDK.
type Instancia struct {
	ID             string
	TenantID       string
	Nome           string
	Telefone       string
	Status         string
	EstadoDesejado string
	CriadaEm       time.Time
	AtualizadaEm   time.Time
}

// Valores válidos do parâmetro estado de AtualizarEstadoDesejado -- espelham
// os únicos dois valores que internal/service/instances.go (SetDesiredState)
// aceita; qualquer outro valor o servidor rejeita com 400/ErrInvalido.
const (
	EstadoDesejadoConectado    = "connected"
	EstadoDesejadoDesconectado = "disconnected"
)

// EntradaListarInstancias parâmetros de paginação para listagem de instâncias.
type EntradaListarInstancias struct {
	Limite int32
	Offset int32
}

// ListaInstancias resultado da listagem paginada.
type ListaInstancias struct {
	Instancias []Instancia
	Total      int32
}

// LinkWizard representa a URL e token para onboarding via wizard web.
type LinkWizard struct {
	InstanciaID string
	Token       string
	URL         string
	ExpiraEm    time.Time
}

// Pareamento reflete o estado atual do ciclo de pareamento (QR Code / Pareado).
type Pareamento struct {
	QRCode   string
	PairCode string
	Seq      int
	ExpiraEm time.Time
	Pareado  bool
	Telefone string
}

// ErrPareamentoExpirado sinaliza que o stream de pareamento (AcompanharPareamento)
// terminou porque o QR/pair code expirou sem que o dispositivo parasse -- é a
// tradução do evento nomeado "expired" que o servidor emite em
// GET .../pairing/stream (internal/transport/rest/pairing_stream.go). Não é
// erro de rede: o servidor fecha o stream de propósito, e o SDK reflete isso
// aqui em vez de silenciar o canal.
var ErrPareamentoExpirado = errors.New("syncz: pareamento expirado")

// EventoPareamento é um item do stream de AcompanharPareamento.
//
// Estado vem preenchido nos eventos "qr" (novo QR/pair code, Pareado=false) e
// "paired" (Pareado=true, Telefone do dispositivo). Err vem preenchido em dois
// casos, e o canal fecha logo em seguida em ambos: o servidor emitiu "expired"
// (Err=ErrPareamentoExpirado, comparável com errors.Is) ou a conexão caiu por
// erro de rede/decodificação (Err envolve o erro original via %w). Fim normal
// do stream sem erro (contexto cancelado, ou o servidor fechou após "paired")
// apenas fecha o canal, sem um evento de erro final.
type EventoPareamento struct {
	Estado Pareamento
	Err    error
}

// EscopoInfo detalha um escopo de permissão.
type EscopoInfo struct {
	Valor     string
	Descricao string
}

// ContratoInfo descreve o contrato de governança e termos da instância.
type ContratoInfo struct {
	TemplateID          string
	Nome                string
	Versao              int
	TermoMarkdown       string
	TermoSHA256         string
	EscoposObrigatorios []EscopoInfo
	EscoposOpcionais    []EscopoInfo
	MarcaNome           string
	MarcaLogoURL        string
	MarcaCor            string
	IntroMarkdown       string
}

// EntradaAceite registra o consentimento do titular.
type EntradaAceite struct {
	InstanciaID      string
	EscoposOpcionais []string
	IPTitular        string
	UserAgentTitular string
}

// EntradaEnvio parâmetros para envio de mensagem de texto.
type EntradaEnvio struct {
	InstanciaID       string
	Para              string
	Texto             string
	ChaveIdempotencia string
}

// EntradaEnvioMidia parâmetros para envio de arquivo/mídia.
type EntradaEnvioMidia struct {
	InstanciaID       string
	Para              string
	MimeType          string
	NomeArquivo       string
	Legenda           string
	ChaveIdempotencia string
	Conteudo          []byte
}

// Recibo confirmação de aceite/envio pelo gateway.
type Recibo struct {
	MensagemID string
	Status     string
	EnviadoEm  time.Time
	Duplicada  bool
}

// EstadoMensagem reflete o ciclo de vida de UMA mensagem outbound já aceita
// pelo gateway (GET .../messages/{message_id}). Enviar/EnviarMidia só
// devolvem o aceite inicial (202 + Recibo.Status="accepted"); este é o único
// jeito do tenant descobrir se a mensagem virou sent/failed/delivered/read
// depois, sem inspecionar webhook nenhum (achado F13-11 da matriz do SDK).
//
// EnviadaEm/EntregueEm/LidaEm vêm com time.Time zero quando a mensagem ainda
// não passou por aquele estágio -- mesma convenção do resto do SDK (ver
// parseTimestamp), sem ponteiro.
type EstadoMensagem struct {
	MensagemID    string
	Status        string
	WAMensagemID  string
	ErrorCode     string
	ErrorMessage  string
	Tentativas    int
	EnfileiradaEm time.Time
	EnviadaEm     time.Time
	EntregueEm    time.Time
	LidaEm        time.Time
}

// EstadoOperacao reflete o estado de UMA provider-op assíncrona já aceita
// pelo gateway (GET .../operations/{op_id}) -- o jeito do tenant acompanhar
// uma chamada que voltou "accepted" (EsperaSincrona.Assincrono=true, ou o
// timeout de espera venceu antes do provider confirmar: ver Grupo.Status/OpID
// e EsperaSincrona) sem inspecionar webhook nenhum, mesmo gap F13-11 da
// matriz do SDK que motivou StatusMensagem (task 565) -- lá para mensagem
// outbound, aqui para o restante das operações assíncronas de grupo
// (create_group, update_group_name, etc.).
//
// Result vem cru (json.RawMessage) porque o formato depende da operação --
// o servidor não normaliza (ver internal/transport/rest/operations.go
// toOperationResp: "result é repassado cru do provider_ops.result"). Para
// create_group, por exemplo, o chamador decodifica {"jid":...,"subject":...,
// "participants":[...]}. Vem nil enquanto Status="pending". Erro vem
// preenchido só quando Status="failed" (mensagem de erro do provider).
//
// CriadaEm sempre vem preenchida; FinalizadaEm vem com time.Time zero
// enquanto a operação ainda está pending (mesma convenção do resto do SDK,
// ver parseTimestamp).
type EstadoOperacao struct {
	OpID         string
	Operacao     string
	Status       string
	Result       json.RawMessage
	Erro         string
	CriadaEm     time.Time
	FinalizadaEm time.Time
}

// EntradaCriarEnquete parâmetros para criar uma enquete (ou checklist, se
// Selecionaveis == len(Opcoes)) num grupo -- POST .../polls.
//
// EsperaSincrona funciona igual à de EntradaGrupo: o servidor só tem caminho
// assíncrono via fila (queue.OpCreatePoll, internal/service/polls_remote.go,
// task 559) quando a instância não tem provider local (D2 -- sempre o caso em
// staging/produção, edge e node são processos diferentes). Sem esperar,
// Enquete.PollID vem preenchido mesmo assim -- é o identificador da
// provider-op enfileirada (ver createRemoteNoWait), endereçável depois via
// Cliente.StatusOperacao (task 566) -- não existe um par Status/OpID
// separado aqui como em Grupo porque o servidor não modela essa distinção
// para enquetes.
type EntradaCriarEnquete struct {
	InstanciaID   string
	GrupoJID      string
	Pergunta      string
	Opcoes        []string
	Selecionaveis int
	EsperaSincrona
}

// Enquete dados de uma enquete (ou checklist) de grupo (GET/POST .../polls,
// POST .../polls/{poll_id}/close). CriadaEm vem zero quando o servidor ainda
// não confirmou created_at (caminho assíncrono sem espera).
type Enquete struct {
	PollID        string
	InstanciaID   string
	GrupoJID      string
	Pergunta      string
	Opcoes        []string
	Selecionaveis int
	Encerrada     bool
	CriadaEm      time.Time
}

// EnqueteOpcaoResultado é a contagem e a lista de votantes de uma opção,
// na mesma ordem de Enquete.Opcoes.
type EnqueteOpcaoResultado struct {
	Opcao    string
	Votos    int
	Votantes []string
}

// EnqueteResultado é o tally consolidado de uma enquete
// (GET .../polls/{poll_id}/results). Votos chegam cifrados e só são
// decifráveis pela instância conectada no momento -- se ela esteve fora do
// ar, aqueles votos se perderam para o servidor; use CalculadoEm para saber
// a idade do dado e não assuma completude (mesmo aviso do proto
// PollResults, proto/syncz/v1/syncz.proto).
type EnqueteResultado struct {
	PollID        string
	Opcoes        []EnqueteOpcaoResultado
	TotalVotantes int
	CalculadoEm   time.Time
}

// esperaSincronaPadrao é o timeout aplicado quando o chamador pede espera
// síncrona (o padrão) sem informar TimeoutEspera. Espelha o
// providerOpDefaultTimeout do servidor (internal/service/provider_ops.go) --
// mantê-los iguais evita que o SDK peça um timeout que o servidor já sabe que
// vai truncar.
const esperaSincronaPadrao = 5 * time.Second

// EsperaSincrona controla se uma operação de grupo deve bloquear até o efeito
// real acontecer no WhatsApp (JID definitivo do grupo, foto realmente
// aplicada, participante realmente adicionado) ou voltar imediatamente com um
// recibo de aceite para acompanhar depois.
//
// Vale para toda operação de grupo que tem caminho assíncrono no servidor
// quando o edge não tem o provider local (D2 -- SEMPRE o caso em staging e
// produção, onde edge e node são processos/máquinas diferentes por design):
// sem esperar, o valor prático da resposta (o JID do grupo, por exemplo)
// ainda não existe -- Grupo.JID vem vazio e Grupo.Status vem "accepted".
//
// O zero-value (Assincrono=false) já pede espera síncrona com o timeout
// default do servidor: é essa a razão de existir deste tipo -- antes dele o
// SDK não tinha NENHUM jeito de pedir espera (nem em REST, que o servidor já
// aceitava via ?wait=, nem em gRPC, que não tinha campo nenhum para isso), e
// toda chamada caía no caminho assíncrono sempre, mesmo quando o chamador
// claramente precisava do resultado ali (task 349). Peça Assincrono=true só
// quando seu fluxo de fato não precisa do resultado na hora -- por exemplo,
// disparar e tratar via evento/webhook depois.
type EsperaSincrona struct {
	Assincrono    bool
	TimeoutEspera time.Duration
}

func (e EsperaSincrona) timeout() time.Duration {
	if e.TimeoutEspera > 0 {
		return e.TimeoutEspera
	}
	return esperaSincronaPadrao
}

// protoWait traduz EsperaSincrona para os campos wait/wait_timeout_ms do
// proto -- a contraparte gRPC de comWaitQuery (sdk/go/rest.go), que traduz
// para a query string ?wait= que o REST usa.
func (e EsperaSincrona) protoWait() (wait bool, timeoutMs int64) {
	if e.Assincrono {
		return false, 0
	}
	return true, e.timeout().Milliseconds()
}

// Valores válidos de EntradaResponderEventoGrupo.Resposta -- espelham o
// EventResponseType do proto (proto/syncz/v1/syncz.proto) e a validação do
// servidor (internal/service/group_events.go, validarSendEventResponse).
const (
	RespostaEventoGrupoIndo    = "going"
	RespostaEventoGrupoNaoIndo = "not_going"
	RespostaEventoGrupoTalvez  = "maybe"
)

// EntradaCriarEventoGrupo parâmetros para criar um evento de grupo (o card
// com data/hora/local que aparece fixado no grupo) -- POST .../group-events.
//
// EsperaSincrona funciona igual à de EntradaGrupo/EntradaCriarEnquete: o
// servidor só tem caminho assíncrono via fila quando a instância não tem
// provider local (D2 -- sempre o caso em staging/produção, task 559).
//
// GarantirLembrete espelha optional bool ensure_reminder do proto -- nil
// significa "garanta o lembrete" (default do servidor), o mesmo motivo de
// EnsureReminder ser *bool em CreateGroupEventInput
// (internal/service/group_events.go): proto3 não distingue false de ausente
// em bool comum.
type EntradaCriarEventoGrupo struct {
	InstanciaID             string
	GrupoJID                string
	Titulo                  string
	Descricao               string
	Inicio                  time.Time
	Fim                     time.Time // zero = sem fim
	LembreteAntecedencia    time.Duration
	LocalNome               string
	LinkEntrada             string
	ChamadaAgendada         bool
	PermiteConvidadosExtras bool
	GarantirLembrete        *bool
	EsperaSincrona
}

// EventoGrupo dados de um evento de grupo (GET/POST/PATCH .../group-events,
// POST .../group-events/{event_id}/cancel). EventoID é o wa_message_id da
// mensagem de criação -- é a chave usada em AtualizarEventoGrupo,
// ResponderEventoGrupo e CancelarEventoGrupo.
//
// Fim vem zero quando o evento não tem horário de término
// (Fim.IsZero() -- mesma convenção do resto do SDK, ver parseTimestamp).
type EventoGrupo struct {
	EventoID                string
	InstanciaID             string
	GrupoJID                string
	Titulo                  string
	Descricao               string
	Inicio                  time.Time
	Fim                     time.Time
	LembreteAntecedencia    time.Duration
	LocalNome               string
	LinkEntrada             string
	Cancelado               bool
	ChamadaAgendada         bool
	PermiteConvidadosExtras bool
	CriadoEm                time.Time
	AtualizadoEm            time.Time
}

// EntradaAtualizarEventoGrupo parâmetros para PATCH .../group-events/{event_id}.
// Só os campos ponteiro PRESENTES (não-nil) são aplicados -- os ausentes
// ficam como estão, espelhando UpdateGroupEventInput
// (internal/service/group_events.go) e o UpdateGroupEventRequest do proto.
type EntradaAtualizarEventoGrupo struct {
	InstanciaID          string
	EventoID             string
	Titulo               *string
	Descricao            *string
	Inicio               *time.Time
	Fim                  *time.Time
	LembreteAntecedencia *time.Duration
	LocalNome            *string
	LinkEntrada          *string
	EsperaSincrona
}

// EntradaCancelarEventoGrupo parâmetros para POST .../group-events/{event_id}/cancel.
// Cancelar marca o card como riscado no grupo -- não apaga a mensagem.
// Cancelar duas vezes é idempotente (mesmo comportamento do servidor).
type EntradaCancelarEventoGrupo struct {
	InstanciaID string
	EventoID    string
	EsperaSincrona
}

// EntradaResponderEventoGrupo parâmetros para POST .../group-events/{event_id}/response
// (RSVP a um evento de grupo). Resposta usa as constantes
// RespostaEventoGrupo*. ConvidadosExtras só faz sentido quando
// Resposta == RespostaEventoGrupoIndo e o evento permite convidados extras
// (PermiteConvidadosExtras na criação) -- o servidor rejeita com ErrInvalido
// fora disso.
type EntradaResponderEventoGrupo struct {
	InstanciaID      string
	EventoID         string
	Resposta         string
	ConvidadosExtras int32
	EsperaSincrona
}

// RespostaEventoGrupo resultado de ResponderEventoGrupo -- MensagemID é o
// wa_message_id da mensagem de RSVP enviada pela instância.
type RespostaEventoGrupo struct {
	EventoID   string
	MensagemID string
	Resposta   string
}

// EntradaGrupo parâmetros para criação de grupo.
type EntradaGrupo struct {
	InstanciaID   string
	Assunto       string
	Participantes []string
	EsperaSincrona
}

// Grupo dados de um grupo WhatsApp.
type Grupo struct {
	JID           string
	Assunto       string
	Participantes []string
	DonoJID       string
	// Status/OpID só vêm preenchidos quando a chamada NÃO esperou o efeito
	// real acontecer (Assincrono=true, ou Assincrono=false mas o timeout
	// venceu antes do provider confirmar): Status="accepted" e OpID identifica
	// a operação para acompanhamento posterior (evento/webhook). Com espera
	// bem-sucedida, os dois vêm vazios e JID já é o definitivo.
	Status string
	OpID   string
}

// EntradaAtualizarNomeGrupo parâmetros para renomear grupo.
type EntradaAtualizarNomeGrupo struct {
	InstanciaID string
	GrupoJID    string
	Assunto     string
	EsperaSincrona
}

// EntradaAtualizarFotoGrupo parâmetros para atualizar a foto de perfil do grupo.
type EntradaAtualizarFotoGrupo struct {
	InstanciaID string
	GrupoJID    string
	MimeType    string
	Foto        []byte
	EsperaSincrona
}

// EntradaParticipantes parâmetros para adição/remoção de membros no grupo.
type EntradaParticipantes struct {
	InstanciaID   string
	GrupoJID      string
	Participantes []string
	EsperaSincrona
}

// EntradaLinkConvite parâmetros para gerar link de convite do grupo.
//
// Sem EsperaSincrona: LinkConviteGrupo sempre espera no servidor (não tem
// caminho assíncrono), então não há o que pedir aqui.
type EntradaLinkConvite struct {
	InstanciaID string
	GrupoJID    string
	Reset       bool
}

// EntradaSairGrupo parâmetros para sair de um grupo.
type EntradaSairGrupo struct {
	InstanciaID string
	GrupoJID    string
	EsperaSincrona
}

// EntradaFixarChat parâmetros para fixar ou desfixar conversa.
type EntradaFixarChat struct {
	InstanciaID string
	ChatJID     string
	Fixar       bool
	EsperaSincrona
}

// EntradaMarcarLido parâmetros para marcar mensagens como lidas.
type EntradaMarcarLido struct {
	InstanciaID  string
	ChatJID      string
	MensagensIDs []string
	EsperaSincrona
}

// EntradaDefinirLeituraChat parâmetros para marcar uma conversa como lida ou
// não lida via mutação de app state -- distinto de MarcarLido, que só manda
// recibo de mensagens específicas. Funciona igual para DM com contato,
// self-chat e grupo.
type EntradaDefinirLeituraChat struct {
	InstanciaID string
	ChatJID     string
	Lida        bool
	EsperaSincrona
}

// NumeroInfo resultado de verificação de número no WhatsApp.
type NumeroInfo struct {
	Telefone   string
	NoWhatsApp bool
	JID        string
	LID        string
}

// EntradaVerificarNumeros parâmetros para consulta de números.
type EntradaVerificarNumeros struct {
	InstanciaID string
	Telefones   []string
}

// Politica configuração de política de uma instância.
type Politica struct {
	InstanciaID string
	ConfigJSON  []byte
}

// Uso estatísticas agregadas de consumo do tenant.
type Uso struct {
	TenantID          string
	MensagensEnviadas int64
	InstanciasAtivas  int64
}

// Evento é um item do outbox durável de uma instância (GET
// .../events/stream) -- contraparte SDK de internal/core.Event. Payload vem
// cru (json.RawMessage) porque o formato depende de Tipo
// (message.received, instance.connected, ...), o mesmo motivo de
// EstadoOperacao.Result vir cru.
type Evento struct {
	ID          string
	Tipo        string
	InstanciaID string
	ChatJID     string
	Seq         int64
	OcorridoEm  time.Time
	Payload     json.RawMessage
	TraceID     string
}

// EventoStream é um item do stream de AcompanharEventos. Err vem preenchido
// quando a conexão caiu (rede) ou o payload de um bloco veio corrompido; o
// canal fecha logo em seguida. Fim normal do stream (o servidor terminou o
// catch-up e encerrou a conexão, ou o ctx foi cancelado) apenas fecha o
// canal, sem EventoStream.Err.
type EventoStream struct {
	Evento Evento
	Err    error
}

// ClienteEventosStream é implementado pelos transportes de Cliente que
// suportam o stream de eventos por SSE (GET .../events/stream), mesmo
// critério de ClientePareamentoStream logo abaixo: hoje só o transporte REST
// implementa (ver AcompanharEventos em rest.go). O gRPC tem um RPC própio
// (syncv1.SyncZap_StreamEventsServer, internal/transport/grpc/stream.go) com
// forma diferente (filtro por múltiplas instâncias e tipos de evento) --
// somar esse método a Cliente, ou unificar as duas formas numa interface só,
// forçaria uma tradução que nenhum chamador pediu ainda; o Dublê Sandbox
// também não tem stream de eventos. Descubra o suporte por asserção de tipo:
//
//	if streamer, ok := cli.(syncz.ClienteEventosStream); ok {
//	    eventos, err := streamer.AcompanharEventos(ctx, instanciaID, 0)
//	    ...
//	}
//
// fromSeq replaya o outbox a partir dessa sequência (inclusive); 0 (ou
// negativo) pede o outbox inteiro -- espelha from_seq da query string do
// servidor, que trata ausente e "0" da mesma forma
// (internal/transport/sse/stream.go, parseFromSeq).
//
// IMPORTANTE: o servidor hoje faz só catch-up -- ele despeja os eventos com
// seq >= fromSeq já persistidos e encerra a conexão (ver
// internal/transport/sse/handler.go, ServeHTTP: chama loadCatchUpJSON e
// escreve tudo de uma vez, sem tail contínuo). O canal devolvido aqui fecha
// sozinho, sem erro, assim que o servidor esgota o catch-up -- não é um
// stream ao vivo. Drene o canal até fechar para liberar a conexão HTTP
// subjacente; cancelar ctx também libera.
type ClienteEventosStream interface {
	AcompanharEventos(ctx context.Context, instanciaID string, fromSeq int64) (<-chan EventoStream, error)
}

// ClientePareamentoStream é implementado pelos transportes de Cliente que
// suportam o stream de pareamento por SSE (GET .../pairing/stream),
// complementando o polling de Cliente.EstadoPareamento. Hoje só o transporte
// REST implementa (ver AcompanharPareamento em rest.go): o gRPC não tem um RPC
// de streaming equivalente para pairing, e o Dublê Sandbox tampouco -- somar
// esse método a Cliente forçaria os três transportes a implementá-lo, fora do
// escopo desta capacidade. Descubra o suporte por asserção de tipo:
//
//	if streamer, ok := cli.(syncz.ClientePareamentoStream); ok {
//	    eventos, err := streamer.AcompanharPareamento(ctx, instanciaID)
//	    ...
//	}
//
// O canal devolvido fecha quando: o ctx é cancelado, o servidor encerra o
// stream normalmente (após o evento "paired"), ou ocorre erro (rede, ou o
// evento "expired" -- ver EventoPareamento). Drene o canal até fechar para
// liberar a conexão HTTP subjacente; cancelar ctx também libera.
type ClientePareamentoStream interface {
	AcompanharPareamento(ctx context.Context, instanciaID string) (<-chan EventoPareamento, error)
}

// Cliente contrato unificado de comunicação com o gateway sync-zap.
// Implementado pelos transportes gRPC, REST e pelo Dublê Sandbox.
type Cliente interface {
	CriarInstancia(ctx context.Context, e EntradaCriarInstancia) (Instancia, error)
	ObterInstancia(ctx context.Context, id string) (Instancia, error)
	ListarInstancias(ctx context.Context, e EntradaListarInstancias) (ListaInstancias, error)
	RevogarInstancia(ctx context.Context, id string) (Instancia, error)
	AtualizarEstadoDesejado(ctx context.Context, instanciaID, estado string) (Instancia, error)
	NovoLinkWizard(ctx context.Context, instanciaID string) (LinkWizard, error)

	EstadoPareamento(ctx context.Context, instanciaID string) (Pareamento, error)
	Contrato(ctx context.Context, instanciaID string) (ContratoInfo, error)
	RegistrarAceite(ctx context.Context, e EntradaAceite) error

	Enviar(ctx context.Context, e EntradaEnvio) (Recibo, error)
	EnviarMidia(ctx context.Context, e EntradaEnvioMidia) (Recibo, error)
	StatusMensagem(ctx context.Context, instanciaID, mensagemID string) (EstadoMensagem, error)
	StatusOperacao(ctx context.Context, instanciaID, opID string) (EstadoOperacao, error)

	CriarEnquete(ctx context.Context, e EntradaCriarEnquete) (Enquete, error)
	ResultadoEnquete(ctx context.Context, instanciaID, pollID string) (EnqueteResultado, error)
	EncerrarEnquete(ctx context.Context, instanciaID, pollID string) (Enquete, error)

	CriarEventoGrupo(ctx context.Context, e EntradaCriarEventoGrupo) (EventoGrupo, error)
	AtualizarEventoGrupo(ctx context.Context, e EntradaAtualizarEventoGrupo) (EventoGrupo, error)
	ResponderEventoGrupo(ctx context.Context, e EntradaResponderEventoGrupo) (RespostaEventoGrupo, error)
	CancelarEventoGrupo(ctx context.Context, e EntradaCancelarEventoGrupo) (EventoGrupo, error)

	CriarGrupo(ctx context.Context, e EntradaGrupo) (Grupo, error)
	AtualizarNomeGrupo(ctx context.Context, e EntradaAtualizarNomeGrupo) (Grupo, error)
	AtualizarFotoGrupo(ctx context.Context, e EntradaAtualizarFotoGrupo) (Grupo, error)
	AdicionarParticipantes(ctx context.Context, e EntradaParticipantes) error
	RemoverParticipantes(ctx context.Context, e EntradaParticipantes) error
	LinkConviteGrupo(ctx context.Context, e EntradaLinkConvite) (string, error)
	SairGrupo(ctx context.Context, e EntradaSairGrupo) error

	FixarChat(ctx context.Context, e EntradaFixarChat) error
	MarcarLido(ctx context.Context, e EntradaMarcarLido) error
	DefinirLeituraChat(ctx context.Context, e EntradaDefinirLeituraChat) error
	VerificarNumeros(ctx context.Context, e EntradaVerificarNumeros) ([]NumeroInfo, error)

	ObterPolitica(ctx context.Context, instanciaID string) (Politica, error)
	AtualizarPolitica(ctx context.Context, instanciaID string, configJSON []byte) (Politica, error)

	ObterUso(ctx context.Context, tenantID string) (Uso, error)

	Fechar() error
}
