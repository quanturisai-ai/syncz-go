package syncz

import (
	"context"
	"time"
)

// EntradaCriarInstancia parâmetros para criação de instância.
type EntradaCriarInstancia struct {
	Nome     string
	Telefone string
}

// Instancia representa os metadados de uma instância do WhatsApp.
type Instancia struct {
	ID           string
	TenantID     string
	Nome         string
	Telefone     string
	Status       string
	CriadaEm     time.Time
	AtualizadaEm time.Time
}

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

// Cliente contrato unificado de comunicação com o gateway sync-zap.
// Implementado pelos transportes gRPC, REST e pelo Dublê Sandbox.
type Cliente interface {
	CriarInstancia(ctx context.Context, e EntradaCriarInstancia) (Instancia, error)
	ObterInstancia(ctx context.Context, id string) (Instancia, error)
	ListarInstancias(ctx context.Context, e EntradaListarInstancias) (ListaInstancias, error)
	RevogarInstancia(ctx context.Context, id string) (Instancia, error)
	NovoLinkWizard(ctx context.Context, instanciaID string) (LinkWizard, error)

	EstadoPareamento(ctx context.Context, instanciaID string) (Pareamento, error)
	Contrato(ctx context.Context, instanciaID string) (ContratoInfo, error)
	RegistrarAceite(ctx context.Context, e EntradaAceite) error

	Enviar(ctx context.Context, e EntradaEnvio) (Recibo, error)
	EnviarMidia(ctx context.Context, e EntradaEnvioMidia) (Recibo, error)

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
