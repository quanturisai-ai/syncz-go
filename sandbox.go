package syncz

import (
	"context"
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
	enviadas      []EntradaEnvio
	enviadasMidia []EntradaEnvioMidia
	grupos        map[string]*Grupo
	fotosGrupo    map[string][]byte
	linksConvite  map[string]string
	pareamentos   map[string]*Pareamento
	contratos     map[string]*ContratoInfo
	politicas     map[string][]byte
	proximoErro   error
	seqMsg        int
	seqInst       int
}

// NovoSandbox cria uma instância em memória do Cliente sync-zap (Dublê).
func NovoSandbox() SandboxCliente {
	return &sandboxCliente{
		instancias:   make(map[string]*Instancia),
		recibos:      make(map[string]Recibo),
		conteudoMsg:  make(map[string]string),
		grupos:       make(map[string]*Grupo),
		fotosGrupo:   make(map[string][]byte),
		linksConvite: make(map[string]string),
		pareamentos:  make(map[string]*Pareamento),
		contratos:    make(map[string]*ContratoInfo),
		politicas:    make(map[string][]byte),
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

	return rec, nil
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
	return *g, nil
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
