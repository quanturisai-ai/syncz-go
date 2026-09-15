package syncz

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// SandboxCliente estende a interface Cliente com capacidades de inspeção e
// simulação de cenários em testes unitários e de integração sem tocar na rede.
type SandboxCliente interface {
	Cliente
	Enviadas() []EntradaEnvio
	EnviadasMidia() []EntradaEnvioMidia
	Recibos() []Recibo
	Falhar(err error)
	InjetarPareamento(instanciaID, phone string)
}

type sandboxCliente struct {
	mu            sync.Mutex
	instancias    map[string]*Instancia
	recibos       map[string]Recibo
	conteudoMsg   map[string]string
	statusMsg     map[string]EstadoMensagem
	operacoes     map[string]EstadoOperacao
	enviadas      []EntradaEnvio
	enviadasMidia []EntradaEnvioMidia
	grupos        map[string]*Grupo
	fotosGrupo    map[string][]byte
	linksConvite  map[string]string
	pareamentos   map[string]*Pareamento
	contratos     map[string]*ContratoInfo
	politicas     map[string][]byte
	enquetes      map[string]*Enquete
	eventosGrupo  map[string]*EventoGrupo
	proximoErro   error
	seqMsg        int
	seqInst       int
	seqOp         int
	seqEnquete    int
	seqEvento     int
}

// NovoSandbox cria uma instância em memória do Cliente sync-zap (Dublê).
func NovoSandbox() SandboxCliente {
	return &sandboxCliente{
		instancias:   make(map[string]*Instancia),
		recibos:      make(map[string]Recibo),
		conteudoMsg:  make(map[string]string),
		statusMsg:    make(map[string]EstadoMensagem),
		operacoes:    make(map[string]EstadoOperacao),
		grupos:       make(map[string]*Grupo),
		fotosGrupo:   make(map[string][]byte),
		linksConvite: make(map[string]string),
		pareamentos:  make(map[string]*Pareamento),
		contratos:    make(map[string]*ContratoInfo),
		politicas:    make(map[string][]byte),
		enquetes:     make(map[string]*Enquete),
		eventosGrupo: make(map[string]*EventoGrupo),
	}
}

func (s *sandboxCliente) Fechar() error {
	return nil
}

func (s *sandboxCliente) Falhar(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.proximoErro = err
}

func (s *sandboxCliente) Enviadas() []EntradaEnvio {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]EntradaEnvio, len(s.enviadas))
	copy(cp, s.enviadas)
	return cp
}

func (s *sandboxCliente) EnviadasMidia() []EntradaEnvioMidia {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]EntradaEnvioMidia, len(s.enviadasMidia))
	copy(cp, s.enviadasMidia)
	return cp
}

func (s *sandboxCliente) Recibos() []Recibo {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := make([]Recibo, 0, len(s.recibos))
	for _, r := range s.recibos {
		list = append(list, r)
	}
	return list
}

func (s *sandboxCliente) InjetarPareamento(instanciaID, phone string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.pareamentos[instanciaID]
	if !ok {
		p = &Pareamento{
			Seq:      1,
			ExpiraEm: time.Now().Add(24 * time.Hour),
		}
		s.pareamentos[instanciaID] = p
	}
	p.Pareado = true
	p.Telefone = phone
	p.QRCode = ""
	p.PairCode = ""
}

func (s *sandboxCliente) checarErro() error {
	if s.proximoErro != nil {
		err := s.proximoErro
		s.proximoErro = nil
		return err
	}
	return nil
}

func (s *sandboxCliente) CriarInstancia(_ context.Context, e EntradaCriarInstancia) (Instancia, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checarErro(); err != nil {
		return Instancia{}, err
	}

	if e.Nome == "" {
		return Instancia{}, novoErroAPI(ErrInvalido, "nome da instancia obrigatorio")
	}

	s.seqInst++
	id := fmt.Sprintf("inst-sbx-%d", s.seqInst)
	now := time.Now().UTC()

	inst := &Instancia{
		ID:           id,
		TenantID:     "tenant-sandbox",
		Nome:         e.Nome,
		Telefone:     e.Telefone,
		Status:       "created",
		CriadaEm:     now,
		AtualizadaEm: now,
	}
	s.instancias[id] = inst

	s.pareamentos[id] = &Pareamento{
		QRCode:   "2@fake-qr-code-sandbox",
		PairCode: "1234-5678",
		Seq:      1,
		ExpiraEm: now.Add(60 * time.Second),
		Pareado:  false,
	}

	return *inst, nil
}

func (s *sandboxCliente) ObterInstancia(_ context.Context, id string) (Instancia, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checarErro(); err != nil {
		return Instancia{}, err
	}

	inst, ok := s.instancias[id]
	if !ok {
		return Instancia{}, novoErroAPI(ErrNaoEncontrado, "instancia nao encontrada")
	}
	return *inst, nil
}

func (s *sandboxCliente) ListarInstancias(_ context.Context, e EntradaListarInstancias) (ListaInstancias, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checarErro(); err != nil {
		return ListaInstancias{}, err
	}

	lista := make([]Instancia, 0, len(s.instancias))
	for _, inst := range s.instancias {
		lista = append(lista, *inst)
	}

	return ListaInstancias{
		Instancias: lista,
		Total:      int32(len(lista)),
	}, nil
}

func (s *sandboxCliente) RevogarInstancia(_ context.Context, id string) (Instancia, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checarErro(); err != nil {
		return Instancia{}, err
	}

	inst, ok := s.instancias[id]
	if !ok {
		return Instancia{}, novoErroAPI(ErrNaoEncontrado, "instancia nao encontrada")
	}
	inst.Status = "revoked"
	inst.AtualizadaEm = time.Now().UTC()
	return *inst, nil
}

// AtualizarEstadoDesejado simula PATCH .../instances/{id}: grava
// EstadoDesejado e, ao desconectar uma instância "connected", também rebate
// Status para "disconnected" -- mesmo efeito colateral que
// internal/service/instances.go (applyDesiredState) tem no servidor real
// (desconexão de instância conectada dispara EventConnectionLost). Estado
// fora de EstadoDesejadoConectado/EstadoDesejadoDesconectado devolve
// ErrInvalido, mesma validação do servidor (SetDesiredState).
func (s *sandboxCliente) AtualizarEstadoDesejado(_ context.Context, instanciaID, estado string) (Instancia, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checarErro(); err != nil {
		return Instancia{}, err
	}

	if estado != EstadoDesejadoConectado && estado != EstadoDesejadoDesconectado {
		return Instancia{}, novoErroAPI(ErrInvalido, "estado invalido, esperado 'connected' ou 'disconnected'")
	}

	inst, ok := s.instancias[instanciaID]
	if !ok {
		return Instancia{}, novoErroAPI(ErrNaoEncontrado, "instancia nao encontrada")
	}

	inst.EstadoDesejado = estado
	if estado == EstadoDesejadoDesconectado && inst.Status == "connected" {
		inst.Status = "disconnected"
	}
	inst.AtualizadaEm = time.Now().UTC()
	return *inst, nil
}

func (s *sandboxCliente) NovoLinkWizard(_ context.Context, instanciaID string) (LinkWizard, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checarErro(); err != nil {
		return LinkWizard{}, err
	}

	if _, ok := s.instancias[instanciaID]; !ok {
		return LinkWizard{}, novoErroAPI(ErrNaoEncontrado, "instancia nao encontrada")
	}

	token := fmt.Sprintf("wiz-sbx-%s", instanciaID)
	return LinkWizard{
		InstanciaID: instanciaID,
		Token:       token,
		URL:         "https://sync-zap.local/wizard/" + token,
		ExpiraEm:    time.Now().Add(24 * time.Hour),
	}, nil
}

func (s *sandboxCliente) EstadoPareamento(_ context.Context, instanciaID string) (Pareamento, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checarErro(); err != nil {
		return Pareamento{}, err
	}

	p, ok := s.pareamentos[instanciaID]
	if !ok {
		p = &Pareamento{
			QRCode:   "2@fake-qr-code-sandbox",
			PairCode: "1234-5678",
			Seq:      1,
			ExpiraEm: time.Now().Add(60 * time.Second),
			Pareado:  false,
		}
		s.pareamentos[instanciaID] = p
	}
	return *p, nil
}

func (s *sandboxCliente) Contrato(_ context.Context, instanciaID string) (ContratoInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checarErro(); err != nil {
		return ContratoInfo{}, err
	}

	c, ok := s.contratos[instanciaID]
	if !ok {
		c = &ContratoInfo{
			TemplateID:    "tmpl-padrao",
			Nome:          "Contrato Padrao Sandbox",
			Versao:        1,
			TermoMarkdown: "# Termos de Uso Sandbox",
			TermoSHA256:   "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			EscoposObrigatorios: []EscopoInfo{
				{Valor: "messages.read", Descricao: "Leitura de mensagens"},
				{Valor: "messages.send", Descricao: "Envio de mensagens"},
			},
			MarcaNome: "SyncZap",
		}
		s.contratos[instanciaID] = c
	}
	return *c, nil
}

func (s *sandboxCliente) RegistrarAceite(_ context.Context, e EntradaAceite) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checarErro(); err != nil {
		return err
	}

	if e.InstanciaID == "" {
		return novoErroAPI(ErrInvalido, "instancia_id obrigatorio")
	}
	return nil
}

func (s *sandboxCliente) Enviar(_ context.Context, e EntradaEnvio) (Recibo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Se falha programada, falha antes de salvar (A-04: falha não queima a chave de idempotência)
	if err := s.checarErro(); err != nil {
		return Recibo{}, err
	}

	if e.InstanciaID == "" {
		return Recibo{}, novoErroAPI(ErrInvalido, "instancia_id obrigatorio")
	}
	if e.Para == "" {
		return Recibo{}, novoErroAPI(ErrInvalido, "destinatario 'para' obrigatorio")
	}
	if e.ChaveIdempotencia == "" {
		return Recibo{}, novoErroAPI(ErrInvalido, "chave de idempotencia obrigatoria")
	}

	// Idempotência real: mesma chave + mesmo texto = mesmo recibo, Duplicada: true
	if rec, ok := s.recibos[e.ChaveIdempotencia]; ok {
		prevText := s.conteudoMsg[e.ChaveIdempotencia]
		if prevText != e.Texto {
			return Recibo{}, novoErroAPI(ErrConflito, "chave de idempotencia reenviada com conteudo diferente")
		}
		rec.Duplicada = true
		return rec, nil
	}

	s.seqMsg++
	msgID := fmt.Sprintf("msg-sbx-%d", s.seqMsg)
	rec := Recibo{
		MensagemID: msgID,
		Status:     "delivered",
		EnviadoEm:  time.Now().UTC(),
		Duplicada:  false,
	}

	s.recibos[e.ChaveIdempotencia] = rec
	s.conteudoMsg[e.ChaveIdempotencia] = e.Texto
	s.enviadas = append(s.enviadas, e)
	s.statusMsg[msgID] = EstadoMensagem{
		MensagemID:    msgID,
		Status:        rec.Status,
		WAMensagemID:  "wa-" + msgID,
		Tentativas:    1,
		EnfileiradaEm: rec.EnviadoEm,
		EnviadaEm:     rec.EnviadoEm,
		EntregueEm:    rec.EnviadoEm,
	}

	return rec, nil
}

func (s *sandboxCliente) EnviarMidia(_ context.Context, e EntradaEnvioMidia) (Recibo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checarErro(); err != nil {
		return Recibo{}, err
	}

	if e.InstanciaID == "" || e.Para == "" || e.ChaveIdempotencia == "" {
		return Recibo{}, novoErroAPI(ErrInvalido, "campos obrigatorios ausentes")
	}

	if rec, ok := s.recibos[e.ChaveIdempotencia]; ok {
		rec.Duplicada = true
		return rec, nil
	}

	s.seqMsg++
	msgID := fmt.Sprintf("msg-sbx-%d", s.seqMsg)
	rec := Recibo{
		MensagemID: msgID,
		Status:     "delivered",
		EnviadoEm:  time.Now().UTC(),
		Duplicada:  false,
	}

	s.recibos[e.ChaveIdempotencia] = rec
	s.enviadasMidia = append(s.enviadasMidia, e)
	s.statusMsg[msgID] = EstadoMensagem{
		MensagemID:    msgID,
		Status:        rec.Status,
		WAMensagemID:  "wa-" + msgID,
		Tentativas:    1,
		EnfileiradaEm: rec.EnviadoEm,
		EnviadaEm:     rec.EnviadoEm,
		EntregueEm:    rec.EnviadoEm,
	}

	return rec, nil
}

func (s *sandboxCliente) StatusMensagem(_ context.Context, _ string, mensagemID string) (EstadoMensagem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checarErro(); err != nil {
		return EstadoMensagem{}, err
	}

	st, ok := s.statusMsg[mensagemID]
	if !ok {
		return EstadoMensagem{}, novoErroAPI(ErrNaoEncontrado, "mensagem nao encontrada")
	}
	return st, nil
}

func (s *sandboxCliente) StatusOperacao(_ context.Context, _ string, opID string) (EstadoOperacao, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checarErro(); err != nil {
		return EstadoOperacao{}, err
	}

	op, ok := s.operacoes[opID]
	if !ok {
		return EstadoOperacao{}, novoErroAPI(ErrNaoEncontrado, "operacao nao encontrada")
	}
	return op, nil
}

// CriarEnquete cria uma enquete fake em memória -- mesma validação mínima do
// servidor real (group_jid, 2..12 opções, ver internal/service/polls.go
// Polls.validar), sem tocar rede nenhuma.
func (s *sandboxCliente) CriarEnquete(_ context.Context, e EntradaCriarEnquete) (Enquete, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checarErro(); err != nil {
		return Enquete{}, err
	}

	if e.GrupoJID == "" {
		return Enquete{}, novoErroAPI(ErrInvalido, "group_jid obrigatorio")
	}
	if len(e.Opcoes) < 2 || len(e.Opcoes) > 12 {
		return Enquete{}, novoErroAPI(ErrInvalido, "options exige 2..12 itens")
	}

	selecionaveis := e.Selecionaveis
	if selecionaveis == 0 {
		selecionaveis = 1
	}

	s.seqEnquete++
	enq := &Enquete{
		PollID:        fmt.Sprintf("poll-sbx-%d", s.seqEnquete),
		InstanciaID:   e.InstanciaID,
		GrupoJID:      e.GrupoJID,
		Pergunta:      e.Pergunta,
		Opcoes:        append([]string(nil), e.Opcoes...),
		Selecionaveis: selecionaveis,
		CriadaEm:      time.Now().UTC(),
	}
	s.enquetes[enq.PollID] = enq
	return *enq, nil
}

// ResultadoEnquete devolve o tally da enquete fake -- como o Dublê não expõe
// um método para votar (Cliente não tem esse RPC, ver EnqueteResultado em
// syncz.go), o tally sempre vem zerado por opção: cobre o contrato (mesma
// ordem de Opcoes, TotalVotantes) sem fingir um voto que ninguém deu.
func (s *sandboxCliente) ResultadoEnquete(_ context.Context, _ string, pollID string) (EnqueteResultado, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checarErro(); err != nil {
		return EnqueteResultado{}, err
	}

	enq, ok := s.enquetes[pollID]
	if !ok {
		return EnqueteResultado{}, novoErroAPI(ErrNaoEncontrado, "enquete nao encontrada")
	}

	opcoes := make([]EnqueteOpcaoResultado, len(enq.Opcoes))
	for i, opt := range enq.Opcoes {
		opcoes[i] = EnqueteOpcaoResultado{Opcao: opt, Votantes: []string{}}
	}
	return EnqueteResultado{
		PollID:      pollID,
		Opcoes:      opcoes,
		CalculadoEm: time.Now().UTC(),
	}, nil
}

// EncerrarEnquete marca a enquete fake como encerrada.
func (s *sandboxCliente) EncerrarEnquete(_ context.Context, _ string, pollID string) (Enquete, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checarErro(); err != nil {
		return Enquete{}, err
	}

	enq, ok := s.enquetes[pollID]
	if !ok {
		return Enquete{}, novoErroAPI(ErrNaoEncontrado, "enquete nao encontrada")
	}
	enq.Encerrada = true
	return *enq, nil
}

// CriarEventoGrupo cria um evento de grupo fake em memória -- mesma validação
// mínima do servidor real (group_jid e title obrigatórios, ver
// internal/service/group_events.go validarCreateGroupEvent), sem tocar rede
// nenhuma.
func (s *sandboxCliente) CriarEventoGrupo(_ context.Context, e EntradaCriarEventoGrupo) (EventoGrupo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checarErro(); err != nil {
		return EventoGrupo{}, err
	}

	if e.GrupoJID == "" {
		return EventoGrupo{}, novoErroAPI(ErrInvalido, "group_jid obrigatorio")
	}
	if e.Titulo == "" {
		return EventoGrupo{}, novoErroAPI(ErrInvalido, "title obrigatorio")
	}

	s.seqEvento++
	agora := time.Now().UTC()
	ev := &EventoGrupo{
		EventoID:                fmt.Sprintf("evt-sbx-%d", s.seqEvento),
		InstanciaID:             e.InstanciaID,
		GrupoJID:                e.GrupoJID,
		Titulo:                  e.Titulo,
		Descricao:               e.Descricao,
		Inicio:                  e.Inicio,
		Fim:                     e.Fim,
		LembreteAntecedencia:    e.LembreteAntecedencia,
		LocalNome:               e.LocalNome,
		LinkEntrada:             e.LinkEntrada,
		ChamadaAgendada:         e.ChamadaAgendada,
		PermiteConvidadosExtras: e.PermiteConvidadosExtras,
		CriadoEm:                agora,
		AtualizadoEm:            agora,
	}
	s.eventosGrupo[ev.EventoID] = ev
	return *ev, nil
}

// AtualizarEventoGrupo altera o evento fake em memória -- só os campos
// ponteiro presentes em e são aplicados, mesma convenção dos transportes
// reais.
func (s *sandboxCliente) AtualizarEventoGrupo(_ context.Context, e EntradaAtualizarEventoGrupo) (EventoGrupo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checarErro(); err != nil {
		return EventoGrupo{}, err
	}

	ev, ok := s.eventosGrupo[e.EventoID]
	if !ok {
		return EventoGrupo{}, novoErroAPI(ErrNaoEncontrado, "evento de grupo nao encontrado")
	}

	if e.Titulo != nil {
		ev.Titulo = *e.Titulo
	}
	if e.Descricao != nil {
		ev.Descricao = *e.Descricao
	}
	if e.Inicio != nil {
		ev.Inicio = *e.Inicio
	}
	if e.Fim != nil {
		ev.Fim = *e.Fim
	}
	if e.LembreteAntecedencia != nil {
		ev.LembreteAntecedencia = *e.LembreteAntecedencia
	}
	if e.LocalNome != nil {
		ev.LocalNome = *e.LocalNome
	}
	if e.LinkEntrada != nil {
		ev.LinkEntrada = *e.LinkEntrada
	}
	ev.AtualizadoEm = time.Now().UTC()

	return *ev, nil
}

// CancelarEventoGrupo marca o evento fake como cancelado -- idempotente,
// mesmo comportamento do servidor real.
func (s *sandboxCliente) CancelarEventoGrupo(_ context.Context, e EntradaCancelarEventoGrupo) (EventoGrupo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checarErro(); err != nil {
		return EventoGrupo{}, err
	}

	ev, ok := s.eventosGrupo[e.EventoID]
	if !ok {
		return EventoGrupo{}, novoErroAPI(ErrNaoEncontrado, "evento de grupo nao encontrado")
	}
	ev.Cancelado = true
	ev.AtualizadoEm = time.Now().UTC()
	return *ev, nil
}

// ResponderEventoGrupo simula o RSVP a um evento de grupo fake -- não
// mantém lista de votantes/confirmados (o Dublê não expõe leitura disso, mesmo
// espírito de ResultadoEnquete não fingir votos que ninguém deu), só valida e
// devolve o recibo.
func (s *sandboxCliente) ResponderEventoGrupo(_ context.Context, e EntradaResponderEventoGrupo) (RespostaEventoGrupo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checarErro(); err != nil {
		return RespostaEventoGrupo{}, err
	}

	ev, ok := s.eventosGrupo[e.EventoID]
	if !ok {
		return RespostaEventoGrupo{}, novoErroAPI(ErrNaoEncontrado, "evento de grupo nao encontrado")
	}
	if e.Resposta != RespostaEventoGrupoIndo && e.Resposta != RespostaEventoGrupoNaoIndo && e.Resposta != RespostaEventoGrupoTalvez {
		return RespostaEventoGrupo{}, novoErroAPI(ErrInvalido, "response invalido")
	}
	if e.ConvidadosExtras > 0 {
		if e.Resposta != RespostaEventoGrupoIndo {
			return RespostaEventoGrupo{}, novoErroAPI(ErrInvalido, "extra_guest_count so faz sentido quando response e going")
		}
		if !ev.PermiteConvidadosExtras {
			return RespostaEventoGrupo{}, novoErroAPI(ErrInvalido, "evento nao permite convidados extras")
		}
	}

	s.seqEvento++
	return RespostaEventoGrupo{
		EventoID:   ev.EventoID,
		MensagemID: fmt.Sprintf("evt-sbx-rsvp-%d", s.seqEvento),
		Resposta:   e.Resposta,
	}, nil
}

func (s *sandboxCliente) CriarGrupo(_ context.Context, e EntradaGrupo) (Grupo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checarErro(); err != nil {
		return Grupo{}, err
	}

	if e.Assunto == "" {
		return Grupo{}, novoErroAPI(ErrInvalido, "assunto do grupo obrigatorio")
	}

	jid := fmt.Sprintf("120363000%d@g.us", len(s.grupos)+1)
	g := &Grupo{
		JID:           jid,
		Assunto:       e.Assunto,
		Participantes: e.Participantes,
		DonoJID:       "5511999999999@s.whatsapp.net",
	}
	s.grupos[jid] = g

	// Assincrono=true espelha o caminho "accepted" que o transporte REST
	// devolve quando o chamador pede EsperaSincrona.Assincrono (ver
	// EsperaSincrona em syncz.go): o Dublê não tem provider real nem delay
	// pra simular, então resolve a operação como "done" na hora, mas a
	// devolve endereçável via StatusOperacao -- coerente com statusMsg
	// (mesmo padrão desta task para Enviar/StatusMensagem, task 565).
	if e.Assincrono {
		opID := s.registrarOperacaoDoneLocked("create_group", map[string]any{
			"jid":          g.JID,
			"subject":      g.Assunto,
			"participants": g.Participantes,
		})
		return Grupo{Status: "accepted", OpID: opID}, nil
	}

	return *g, nil
}

// registrarOperacaoDoneLocked cria uma EstadoOperacao já em status "done" e a
// registra no mapa em memória, devolvendo o opID gerado. Chamador precisa
// segurar s.mu. Usado pelas operações de grupo que aceitam EsperaSincrona
// quando o chamador pede o caminho assíncrono (Assincrono=true) -- o Dublê
// não tem provider real para atrasar, então resolve na hora, mas ainda assim
// só devolve o resultado via StatusOperacao, igual ao servidor real faria
// para um provider mais lento.
func (s *sandboxCliente) registrarOperacaoDoneLocked(operacao string, resultado any) string {
	s.seqOp++
	opID := fmt.Sprintf("op-sbx-%d", s.seqOp)
	resultJSON, _ := json.Marshal(resultado)
	agora := time.Now().UTC()
	s.operacoes[opID] = EstadoOperacao{
		OpID:         opID,
		Operacao:     operacao,
		Status:       "done",
		Result:       resultJSON,
		CriadaEm:     agora,
		FinalizadaEm: agora,
	}
	return opID
}

func (s *sandboxCliente) AtualizarNomeGrupo(_ context.Context, e EntradaAtualizarNomeGrupo) (Grupo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checarErro(); err != nil {
		return Grupo{}, err
	}

	g, ok := s.grupos[e.GrupoJID]
	if !ok {
		g = &Grupo{JID: e.GrupoJID}
		s.grupos[e.GrupoJID] = g
	}
	g.Assunto = e.Assunto
	return *g, nil
}

func (s *sandboxCliente) AtualizarFotoGrupo(_ context.Context, e EntradaAtualizarFotoGrupo) (Grupo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checarErro(); err != nil {
		return Grupo{}, err
	}

	s.fotosGrupo[e.GrupoJID] = e.Foto
	g, ok := s.grupos[e.GrupoJID]
	if !ok {
		g = &Grupo{JID: e.GrupoJID}
		s.grupos[e.GrupoJID] = g
	}
	return *g, nil
}

func (s *sandboxCliente) AdicionarParticipantes(_ context.Context, e EntradaParticipantes) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checarErro(); err != nil {
		return err
	}

	g, ok := s.grupos[e.GrupoJID]
	if ok {
		g.Participantes = append(g.Participantes, e.Participantes...)
	}
	return nil
}

func (s *sandboxCliente) RemoverParticipantes(_ context.Context, e EntradaParticipantes) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.checarErro()
}

func (s *sandboxCliente) LinkConviteGrupo(_ context.Context, e EntradaLinkConvite) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checarErro(); err != nil {
		return "", err
	}

	link, ok := s.linksConvite[e.GrupoJID]
	if !ok || e.Reset {
		link = fmt.Sprintf("https://chat.whatsapp.com/invite-%s", e.GrupoJID)
		s.linksConvite[e.GrupoJID] = link
	}
	return link, nil
}

func (s *sandboxCliente) SairGrupo(_ context.Context, _ EntradaSairGrupo) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checarErro()
}

func (s *sandboxCliente) FixarChat(_ context.Context, _ EntradaFixarChat) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checarErro()
}

func (s *sandboxCliente) MarcarLido(_ context.Context, _ EntradaMarcarLido) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checarErro()
}

func (s *sandboxCliente) DefinirLeituraChat(_ context.Context, _ EntradaDefinirLeituraChat) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checarErro()
}

func (s *sandboxCliente) VerificarNumeros(_ context.Context, e EntradaVerificarNumeros) ([]NumeroInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checarErro(); err != nil {
		return nil, err
	}

	resultado := make([]NumeroInfo, len(e.Telefones))
	for i, tel := range e.Telefones {
		resultado[i] = NumeroInfo{
			Telefone:   tel,
			NoWhatsApp: true,
			JID:        tel + "@s.whatsapp.net",
		}
	}
	return resultado, nil
}

func (s *sandboxCliente) ObterPolitica(_ context.Context, instanciaID string) (Politica, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checarErro(); err != nil {
		return Politica{}, err
	}

	cfg, ok := s.politicas[instanciaID]
	if !ok {
		cfg = []byte(`{"mode":"standard"}`)
	}
	return Politica{
		InstanciaID: instanciaID,
		ConfigJSON:  cfg,
	}, nil
}

func (s *sandboxCliente) AtualizarPolitica(_ context.Context, instanciaID string, configJSON []byte) (Politica, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checarErro(); err != nil {
		return Politica{}, err
	}

	s.politicas[instanciaID] = configJSON
	return Politica{
		InstanciaID: instanciaID,
		ConfigJSON:  configJSON,
	}, nil
}

func (s *sandboxCliente) ObterUso(_ context.Context, tenantID string) (Uso, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checarErro(); err != nil {
		return Uso{}, err
	}

	return Uso{
		TenantID:          tenantID,
		MensagensEnviadas: int64(len(s.enviadas)),
		InstanciasAtivas:  int64(len(s.instancias)),
	}, nil
}
