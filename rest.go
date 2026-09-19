package syncz

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type clienteREST struct {
	baseURL    string
	chaveAPI   string
	httpClient *http.Client
	prazo      time.Duration
}

func novoClienteREST(o Opcoes) (Cliente, error) {
	client := o.HTTPClient
	if client == nil {
		client = &http.Client{
			Timeout: o.Prazo,
		}
	}

	return &clienteREST{
		baseURL:    strings.TrimRight(o.BaseURL, "/"),
		chaveAPI:   o.ChaveAPI,
		httpClient: client,
		prazo:      o.Prazo,
	}, nil
}

// comWaitQuery acrescenta ?wait=<duracao> (ou &wait= se path ja' tem query
// string) ao path, a menos que o chamador tenha pedido Assincrono=true. E' a
// contraparte REST de ctxComWait no transporte gRPC (internal/transport/grpc):
// o mesmo EsperaSincrona vira query string aqui e campos wait/wait_timeout_ms
// la'.
func comWaitQuery(path string, e EsperaSincrona) string {
	if e.Assincrono {
		return path
	}
	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}
	return path + sep + "wait=" + url.QueryEscape(e.timeout().String())
}

func (c *clienteREST) Fechar() error {
	if tr, ok := c.httpClient.Transport.(*http.Transport); ok {
		tr.CloseIdleConnections()
	}
	return nil
}

type erroProblemDetails struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail"`
	Field    string `json:"field"`
	Instance string `json:"instance"`
}

func (c *clienteREST) requisicao(ctx context.Context, metodo, path string, corpo any, idemKey string) (*http.Response, []byte, error) {
	var bodyReader io.Reader
	var contentType string

	if corpo != nil {
		data, err := json.Marshal(corpo)
		if err != nil {
			return nil, nil, fmt.Errorf("syncz: serializar json: %w", err)
		}
		bodyReader = bytes.NewReader(data)
		contentType = "application/json"
	}

	u := c.baseURL + path
	req, err := http.NewRequestWithContext(ctx, metodo, u, bodyReader)
	if err != nil {
		return nil, nil, fmt.Errorf("syncz: criar requisicao http: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.chaveAPI)
	req.Header.Set("Accept", "application/json")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("syncz: erro de rede: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("syncz: ler resposta: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, nil, erroPorStatusHTTP(resp.StatusCode, respBytes)
	}

	return resp, respBytes, nil
}

// erroPorStatusHTTP traduz um status HTTP >= 400 e o corpo (problem+json ou
// texto puro) num dos erros sentinela do SDK. Compartilhado por requisicao e
// por AcompanharPareamento, que abre a conexão SSE por fora do helper
// requisicao (precisa manter o corpo aberto em caso de sucesso).
func erroPorStatusHTTP(status int, respBytes []byte) error {
	var prob erroProblemDetails
	_ = json.Unmarshal(respBytes, &prob)

	msg := prob.Detail
	if msg == "" {
		msg = prob.Title
	}
	if msg == "" {
		msg = string(respBytes)
	}

	switch status {
	case http.StatusNotFound:
		return novoErroAPI(ErrNaoEncontrado, msg)
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		return novoErroAPI(ErrInvalido, msg)
	case http.StatusUnauthorized:
		return novoErroAPI(ErrCredencial, msg)
	case http.StatusForbidden:
		return novoErroAPI(ErrSemPermissao, msg)
	case http.StatusConflict, http.StatusPreconditionFailed:
		return novoErroAPI(ErrConflito, msg)
	case http.StatusTooManyRequests, http.StatusServiceUnavailable:
		return novoErroAPI(ErrIndisponivel, msg)
	default:
		return novoErroAPI(fmt.Errorf("syncz: erro http %d", status), msg)
	}
}

func parseTimestamp(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, _ := time.Parse(time.RFC3339, s)
	return t
}

// parseUnixSeconds traduz um timestamp Unix em segundos (formato usado pelos
// DTOs de enquete -- pollResp.CreatedAt, pollResultsResp.ComputedAt, ver
// internal/transport/rest/dto.go) para time.Time, com a mesma convenção do
// resto do SDK: zero-value quando o campo não veio preenchido (parseTimestamp
// acima é a contraparte para os campos que o servidor manda em RFC3339).
func parseUnixSeconds(sec int64) time.Time {
	if sec == 0 {
		return time.Time{}
	}
	return time.Unix(sec, 0).UTC()
}

// instanciaRespDTO e' o formato de instancia devolvido por
// CriarInstancia/ObterInstancia/ListarInstancias/RevogarInstancia/Desparear --
// um unico shape para as 5 respostas (internal/transport/rest/dto.go,
// instanceResp), decodificado por instanciaDoResp (task 589, CA-16 --
// unifica os 4 blocos antes duplicados).
type instanciaRespDTO struct {
	ID        string         `json:"id"`
	TenantID  string         `json:"tenant_id"`
	Name      string         `json:"name"`
	Phone     string         `json:"phone"`
	Status    string         `json:"status"`
	PairedAt  string         `json:"paired_at,omitempty"`
	Consent   consentRespDTO `json:"consent"`
	CreatedAt string         `json:"created_at"`
	UpdatedAt string         `json:"updated_at"`
}

// consentRespDTO espelha internal/transport/rest/dto_consent.go (consentDTO).
type consentRespDTO struct {
	Status          string   `json:"status"`
	Origin          string   `json:"origin,omitempty"`
	GrantedBy       string   `json:"granted_by,omitempty"`
	RequestedAt     string   `json:"requested_at,omitempty"`
	GrantedAt       string   `json:"granted_at,omitempty"`
	RevokedAt       string   `json:"revoked_at,omitempty"`
	ContractVersion int      `json:"contract_version,omitempty"`
	Scopes          []string `json:"scopes,omitempty"`
}

func instanciaDoResp(res instanciaRespDTO) Instancia {
	var pareadoEm *time.Time
	if res.PairedAt != "" {
		t := parseTimestamp(res.PairedAt)
		pareadoEm = &t
	}
	info := ConsentimentoInfo{
		Status:         res.Consent.Status,
		Origem:         res.Consent.Origin,
		ConcedidoPor:   res.Consent.GrantedBy,
		VersaoContrato: res.Consent.ContractVersion,
		Escopos:        res.Consent.Scopes,
	}
	if res.Consent.RequestedAt != "" {
		t := parseTimestamp(res.Consent.RequestedAt)
		info.SolicitadoEm = &t
	}
	if res.Consent.GrantedAt != "" {
		t := parseTimestamp(res.Consent.GrantedAt)
		info.ConcedidoEm = &t
	}
	if res.Consent.RevokedAt != "" {
		t := parseTimestamp(res.Consent.RevokedAt)
		info.RevogadoEm = &t
	}
	return Instancia{
		ID:            res.ID,
		TenantID:      res.TenantID,
		Nome:          res.Name,
		Telefone:      res.Phone,
		Status:        res.Status,
		PareadoEm:     pareadoEm,
		Consentimento: info,
		CriadaEm:      parseTimestamp(res.CreatedAt),
		AtualizadaEm:  parseTimestamp(res.UpdatedAt),
	}
}

func (c *clienteREST) CriarInstancia(ctx context.Context, e EntradaCriarInstancia) (Instancia, error) {
	corpo := map[string]string{
		"name": e.Nome,
	}
	if e.Telefone != "" {
		corpo["phone"] = e.Telefone
	}

	_, respBytes, err := c.requisicao(ctx, http.MethodPost, "/api/v1/instances", corpo, "")
	if err != nil {
		return Instancia{}, err
	}

	var res instanciaRespDTO
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return Instancia{}, fmt.Errorf("syncz: desserializar instancia: %w", err)
	}
	return instanciaDoResp(res), nil
}

func (c *clienteREST) ObterInstancia(ctx context.Context, id string) (Instancia, error) {
	_, respBytes, err := c.requisicao(ctx, http.MethodGet, "/api/v1/instances/"+url.PathEscape(id), nil, "")
	if err != nil {
		return Instancia{}, err
	}

	var res instanciaRespDTO
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return Instancia{}, fmt.Errorf("syncz: desserializar instancia: %w", err)
	}
	return instanciaDoResp(res), nil
}

func (c *clienteREST) ListarInstancias(ctx context.Context, e EntradaListarInstancias) (ListaInstancias, error) {
	path := fmt.Sprintf("/api/v1/instances?limit=%d&offset=%d", e.Limite, e.Offset)
	_, respBytes, err := c.requisicao(ctx, http.MethodGet, path, nil, "")
	if err != nil {
		return ListaInstancias{}, err
	}

	var res struct {
		Instances []instanciaRespDTO `json:"instances"`
		Total     int32              `json:"total"`
	}
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return ListaInstancias{}, fmt.Errorf("syncz: desserializar lista de instancias: %w", err)
	}

	lista := make([]Instancia, len(res.Instances))
	for i, item := range res.Instances {
		lista[i] = instanciaDoResp(item)
	}

	return ListaInstancias{
		Instancias: lista,
		Total:      res.Total,
	}, nil
}

func (c *clienteREST) RevogarInstancia(ctx context.Context, id string) (Instancia, error) {
	_, respBytes, err := c.requisicao(ctx, http.MethodPost, "/api/v1/instances/"+url.PathEscape(id)+"/revoke", nil, "")
	if err != nil {
		return Instancia{}, err
	}

	var res instanciaRespDTO
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return Instancia{}, fmt.Errorf("syncz: desserializar instancia revogada: %w", err)
	}
	return instanciaDoResp(res), nil
}

// Desparear (task 589, CA-16/CA-20) desfaz o pareamento sem apagar a instancia.
func (c *clienteREST) Desparear(ctx context.Context, instanciaID string) (Instancia, error) {
	_, respBytes, err := c.requisicao(ctx, http.MethodPost, "/api/v1/instances/"+url.PathEscape(instanciaID)+"/unpair", nil, "")
	if err != nil {
		return Instancia{}, err
	}

	var res instanciaRespDTO
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return Instancia{}, fmt.Errorf("syncz: desserializar instancia desparada: %w", err)
	}
	return instanciaDoResp(res), nil
}

// AtualizarEstadoDesejado consulta/atualiza via PATCH .../instances/{id} --
// ver internal/transport/rest/instances.go (handleSetDesiredState) e dto.go
// (setDesiredStateReq/instanceResp) para o formato real. estado aceita só
// EstadoDesejadoConectado ("connected") ou EstadoDesejadoDesconectado
// ("disconnected") -- qualquer outro valor o servidor rejeita com
// ErrInvalido (internal/service/instances.go, SetDesiredState). Instância
// inexistente devolve ErrNaoEncontrado (comparável via errors.Is), mesmo
// padrão de erroPorStatusHTTP usado pelo resto do cliente REST.
func (c *clienteREST) AtualizarEstadoDesejado(ctx context.Context, instanciaID, estado string) (Instancia, error) {
	body := struct {
		DesiredState string `json:"desired_state"`
	}{DesiredState: estado}

	path := "/api/v1/instances/" + url.PathEscape(instanciaID)
	_, respBytes, err := c.requisicao(ctx, http.MethodPatch, path, body, "")
	if err != nil {
		return Instancia{}, err
	}

	var res struct {
		ID           string `json:"id"`
		TenantID     string `json:"tenant_id"`
		Name         string `json:"name"`
		Phone        string `json:"phone"`
		Status       string `json:"status"`
		DesiredState string `json:"desired_state"`
		CreatedAt    string `json:"created_at"`
		UpdatedAt    string `json:"updated_at"`
	}
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return Instancia{}, fmt.Errorf("syncz: desserializar instancia: %w", err)
	}

	return Instancia{
		ID:             res.ID,
		TenantID:       res.TenantID,
		Nome:           res.Name,
		Telefone:       res.Phone,
		Status:         res.Status,
		EstadoDesejado: res.DesiredState,
		CriadaEm:       parseTimestamp(res.CreatedAt),
		AtualizadaEm:   parseTimestamp(res.UpdatedAt),
	}, nil
}

func (c *clienteREST) NovoLinkWizard(ctx context.Context, instanciaID string) (LinkWizard, error) {
	_, respBytes, err := c.requisicao(ctx, http.MethodPost, "/api/v1/instances/"+url.PathEscape(instanciaID)+"/wizard-link", nil, "")
	if err != nil {
		return LinkWizard{}, err
	}

	var res struct {
		InstanceID string `json:"instance_id"`
		Token      string `json:"token"`
		URL        string `json:"url"`
		ExpiresAt  string `json:"expires_at"`
	}
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return LinkWizard{}, fmt.Errorf("syncz: desserializar link wizard: %w", err)
	}

	return LinkWizard{
		InstanciaID: res.InstanceID,
		Token:       res.Token,
		URL:         res.URL,
		ExpiraEm:    parseTimestamp(res.ExpiresAt),
	}, nil
}

func (c *clienteREST) EstadoPareamento(ctx context.Context, instanciaID string) (Pareamento, error) {
	_, respBytes, err := c.requisicao(ctx, http.MethodGet, "/api/v1/instances/"+url.PathEscape(instanciaID)+"/pairing", nil, "")
	if err != nil {
		return Pareamento{}, err
	}

	var res struct {
		QRCode    string `json:"qr_code"`
		PairCode  string `json:"pair_code"`
		Seq       int    `json:"seq"`
		ExpiresAt string `json:"expires_at"`
		Paired    bool   `json:"paired"`
		Phone     string `json:"phone"`
	}
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return Pareamento{}, fmt.Errorf("syncz: desserializar estado pareamento: %w", err)
	}

	return Pareamento{
		QRCode:   res.QRCode,
		PairCode: res.PairCode,
		Seq:      res.Seq,
		ExpiraEm: parseTimestamp(res.ExpiresAt),
		Pareado:  res.Paired,
		Telefone: res.Phone,
	}, nil
}

// AcompanharPareamento abre GET .../pairing/stream (SSE) e traduz cada evento
// nomeado (event: qr|paired|expired -- ver internal/transport/rest/pairing_stream.go
// e internal/transport/sse/stream.go, que é quem define o formato real de
// fio: linha "event: <nome>", linha "data: <json>", linha em branco) num
// EventoPareamento no canal devolvido. O parser é um bufio.Scanner simples de
// propósito -- não há biblioteca de SSE no módulo, e este formato não precisa
// de uma.
func (c *clienteREST) AcompanharPareamento(ctx context.Context, instanciaID string) (<-chan EventoPareamento, error) {
	u := c.baseURL + "/api/v1/instances/" + url.PathEscape(instanciaID) + "/pairing/stream"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("syncz: criar requisicao stream pareamento: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.chaveAPI)
	req.Header.Set("Accept", "text/event-stream")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("syncz: erro de rede: %w", err)
	}

	if resp.StatusCode >= 400 {
		defer resp.Body.Close()
		respBytes, _ := io.ReadAll(resp.Body)
		return nil, erroPorStatusHTTP(resp.StatusCode, respBytes)
	}

	eventos := make(chan EventoPareamento)
	go lerStreamPareamento(ctx, resp.Body, eventos)
	return eventos, nil
}

// sseBloco é um bloco bruto de evento SSE -- as linhas "event:"/"data:"
// acumuladas até a linha em branco que fecha o bloco (formato de fio definido
// por internal/transport/sse/stream.go: Stream.Send). Nome vem vazio para o
// formato sem linha "event:" (usado por GET /events/stream); AcompanharPareamento
// sempre recebe nome preenchido.
type sseBloco struct {
	nome  string
	dados string
}

// lerBlocosSSE varre body linha a linha, entregando cada bloco completo a
// processar, até o servidor fechar a conexão (fim normal) ou processar pedir
// parada (devolvendo false -- ex.: ctx cancelado do lado do consumidor).
// Compartilhado por AcompanharPareamento e AcompanharEventos: os dois
// transmitem sobre text/event-stream com o mesmo formato de linha, só o
// decodificador de cada bloco muda.
//
// erroDe, quando não nil, é chamado com o erro de rede real (conexão caindo
// no meio da leitura) -- cancelamento de ctx (ctx.Err() != nil) não conta
// como erro a reportar, mesma convenção do resto do SDK.
func lerBlocosSSE(ctx context.Context, body io.ReadCloser, processar func(sseBloco) bool, erroDe func(error)) {
	defer body.Close()

	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 4096), 1<<20)

	var eventName string
	var dataLines []string

	for scanner.Scan() {
		linha := scanner.Text()
		if linha == "" {
			if eventName != "" || len(dataLines) > 0 {
				bloco := sseBloco{nome: eventName, dados: strings.Join(dataLines, "\n")}
				eventName, dataLines = "", nil
				if !processar(bloco) {
					return
				}
			}
			continue
		}
		switch {
		case strings.HasPrefix(linha, "event:"):
			eventName = strings.TrimSpace(strings.TrimPrefix(linha, "event:"))
		case strings.HasPrefix(linha, "data:"):
			dataLines = append(dataLines, strings.TrimPrefix(strings.TrimPrefix(linha, "data:"), " "))
		}
	}

	if err := scanner.Err(); err != nil && ctx.Err() == nil && erroDe != nil {
		erroDe(err)
	}
}

// lerStreamPareamento consome o corpo SSE do stream de pareamento até o
// servidor fechar a conexão (fim normal, sem erro no canal) ou ctx ser
// cancelado (idem: cancelamento não é uma falha a reportar). Erro de rede
// real -- conexão caindo no meio -- vira o último EventoPareamento antes do
// fechamento.
func lerStreamPareamento(ctx context.Context, body io.ReadCloser, eventos chan<- EventoPareamento) {
	defer close(eventos)

	enviar := func(ev EventoPareamento) bool {
		select {
		case eventos <- ev:
			return true
		case <-ctx.Done():
			return false
		}
	}

	lerBlocosSSE(ctx, body, func(b sseBloco) bool {
		ev, ok := decodificarEventoPareamento(b.nome, b.dados)
		if !ok {
			return true
		}
		return enviar(ev)
	}, func(err error) {
		enviar(EventoPareamento{Err: fmt.Errorf("syncz: stream pareamento: %w", err)})
	})
}

// decodificarEventoPareamento traduz um bloco "event: <nome>" + "data: <json>"
// no EventoPareamento correspondente. ok=false descarta o bloco (nome
// desconhecido/vazio, ex. comentário de keep-alive) sem gerar evento.
func decodificarEventoPareamento(nome, data string) (EventoPareamento, bool) {
	switch nome {
	case "qr":
		var dto struct {
			QRCode    string `json:"qr_code"`
			PairCode  string `json:"pair_code"`
			Seq       int    `json:"seq"`
			ExpiresAt string `json:"expires_at"`
		}
		if err := json.Unmarshal([]byte(data), &dto); err != nil {
			return EventoPareamento{Err: fmt.Errorf("syncz: desserializar evento qr: %w", err)}, true
		}
		return EventoPareamento{Estado: Pareamento{
			QRCode:   dto.QRCode,
			PairCode: dto.PairCode,
			Seq:      dto.Seq,
			ExpiraEm: parseTimestamp(dto.ExpiresAt),
		}}, true
	case "paired":
		var dto struct {
			Phone string `json:"phone"`
		}
		if err := json.Unmarshal([]byte(data), &dto); err != nil {
			return EventoPareamento{Err: fmt.Errorf("syncz: desserializar evento paired: %w", err)}, true
		}
		return EventoPareamento{Estado: Pareamento{Pareado: true, Telefone: dto.Phone}}, true
	case "expired":
		return EventoPareamento{Err: ErrPareamentoExpirado}, true
	default:
		return EventoPareamento{}, false
	}
}

// AcompanharEventos abre GET .../events/stream (SSE) e traduz cada bloco
// "data: <json>" (sem "event:" -- ver internal/transport/sse/stream.go,
// Stream.Send com name="") num EventoStream no canal devolvido. instanciaID é
// obrigatório (erro sem chamada de rede se vazio, mesma validação que o
// servidor faz em parseInstanceIDs -- falhar cedo evita uma requisição que já
// sabemos que volta 400). fromSeq <= 0 omite o parâmetro da query (replay
// completo); ver a documentação de ClienteEventosStream em syncz.go sobre o
// servidor fazer só catch-up, sem tail ao vivo.
func (c *clienteREST) AcompanharEventos(ctx context.Context, instanciaID string, fromSeq int64) (<-chan EventoStream, error) {
	if strings.TrimSpace(instanciaID) == "" {
		return nil, novoErroAPI(ErrInvalido, "instance_id obrigatorio")
	}

	q := url.Values{}
	q.Set("instance_id", instanciaID)
	if fromSeq > 0 {
		q.Set("from_seq", strconv.FormatInt(fromSeq, 10))
	}
	u := c.baseURL + "/api/v1/events/stream?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("syncz: criar requisicao stream eventos: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.chaveAPI)
	req.Header.Set("Accept", "text/event-stream")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("syncz: erro de rede: %w", err)
	}

	if resp.StatusCode >= 400 {
		defer resp.Body.Close()
		respBytes, _ := io.ReadAll(resp.Body)
		return nil, erroPorStatusHTTP(resp.StatusCode, respBytes)
	}

	eventos := make(chan EventoStream)
	go lerStreamEventos(ctx, resp.Body, eventos)
	return eventos, nil
}

// lerStreamEventos consome o corpo SSE do stream de eventos até o servidor
// terminar o catch-up e fechar a conexão (fim normal, sem erro no canal) ou
// ctx ser cancelado. Erro de rede real -- conexão caindo no meio -- vira o
// último EventoStream antes do fechamento.
func lerStreamEventos(ctx context.Context, body io.ReadCloser, eventos chan<- EventoStream) {
	defer close(eventos)

	enviar := func(ev EventoStream) bool {
		select {
		case eventos <- ev:
			return true
		case <-ctx.Done():
			return false
		}
	}

	lerBlocosSSE(ctx, body, func(b sseBloco) bool {
		ev, ok := decodificarEventoStream(b.dados)
		if !ok {
			return true
		}
		return enviar(ev)
	}, func(err error) {
		enviar(EventoStream{Err: fmt.Errorf("syncz: stream eventos: %w", err)})
	})
}

// decodificarEventoStream traduz o "data:" de um bloco no EventoStream
// correspondente -- DTO espelha internal/core.Event (json.Marshal direto,
// ver internal/transport/sse/stream.go loadCatchUpJSON). ok=false descarta o
// bloco (data vazio, ex. comentário de keep-alive) sem gerar evento.
func decodificarEventoStream(data string) (EventoStream, bool) {
	if strings.TrimSpace(data) == "" {
		return EventoStream{}, false
	}

	var dto struct {
		ID         string          `json:"id"`
		Type       string          `json:"type"`
		InstanceID string          `json:"instance_id"`
		ChatJID    string          `json:"chat_jid"`
		Seq        int64           `json:"seq"`
		OccurredAt string          `json:"occurred_at"`
		Payload    json.RawMessage `json:"payload"`
		TraceID    string          `json:"trace_id"`
	}
	if err := json.Unmarshal([]byte(data), &dto); err != nil {
		return EventoStream{Err: fmt.Errorf("syncz: desserializar evento: %w", err)}, true
	}

	return EventoStream{Evento: Evento{
		ID:          dto.ID,
		Tipo:        dto.Type,
		InstanciaID: dto.InstanceID,
		ChatJID:     dto.ChatJID,
		Seq:         dto.Seq,
		OcorridoEm:  parseTimestamp(dto.OccurredAt),
		Payload:     dto.Payload,
		TraceID:     dto.TraceID,
	}}, true
}

func (c *clienteREST) Contrato(ctx context.Context, instanciaID string) (ContratoInfo, error) {
	_, respBytes, err := c.requisicao(ctx, http.MethodGet, "/api/v1/instances/"+url.PathEscape(instanciaID)+"/contract", nil, "")
	if err != nil {
		return ContratoInfo{}, err
	}

	var res struct {
		TemplateID     string `json:"template_id"`
		Name           string `json:"name"`
		Version        int    `json:"version"`
		TermsMarkdown  string `json:"terms_markdown"`
		TermsSHA256    string `json:"terms_sha256"`
		RequiredScopes []struct {
			Value       string `json:"value"`
			Description string `json:"description"`
		} `json:"required_scopes"`
		OptionalScopes []struct {
			Value       string `json:"value"`
			Description string `json:"description"`
		} `json:"optional_scopes"`
		BrandName     string `json:"brand_name"`
		BrandLogoURL  string `json:"brand_logo_url"`
		BrandColor    string `json:"brand_color"`
		IntroMarkdown string `json:"intro_markdown"`
	}
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return ContratoInfo{}, fmt.Errorf("syncz: desserializar contrato: %w", err)
	}

	obrig := make([]EscopoInfo, len(res.RequiredScopes))
	for i, s := range res.RequiredScopes {
		obrig[i] = EscopoInfo{Valor: s.Value, Descricao: s.Description}
	}
	opc := make([]EscopoInfo, len(res.OptionalScopes))
	for i, s := range res.OptionalScopes {
		opc[i] = EscopoInfo{Valor: s.Value, Descricao: s.Description}
	}

	return ContratoInfo{
		TemplateID:          res.TemplateID,
		Nome:                res.Name,
		Versao:              res.Version,
		TermoMarkdown:       res.TermsMarkdown,
		TermoSHA256:         res.TermsSHA256,
		EscoposObrigatorios: obrig,
		EscoposOpcionais:    opc,
		MarcaNome:           res.BrandName,
		MarcaLogoURL:        res.BrandLogoURL,
		MarcaCor:            res.BrandColor,
		IntroMarkdown:       res.IntroMarkdown,
	}, nil
}

func (c *clienteREST) RegistrarAceite(ctx context.Context, e EntradaAceite) error {
	corpo := map[string]any{
		"scopes":     e.EscoposOpcionais,
		"user_ip":    e.IPTitular,
		"user_agent": e.UserAgentTitular,
	}
	if len(e.Evidencia) > 0 {
		corpo["evidence"] = e.Evidencia
	}
	_, _, err := c.requisicao(ctx, http.MethodPost, "/api/v1/instances/"+url.PathEscape(e.InstanciaID)+"/consent", corpo, "")
	return err
}

// SolicitarConsentimento (task 589, CA-16/CA-13) pede ao titular, por DM
// pelo numero pareado, que autorize a instancia.
func (c *clienteREST) SolicitarConsentimento(ctx context.Context, e EntradaSolicitarConsentimento) (PedidoConsentimento, error) {
	corpo := map[string]any{
		"via": e.Via,
	}
	if e.Para != "" {
		corpo["to"] = e.Para
	}
	if e.Texto != "" {
		corpo["text"] = e.Texto
	}

	_, respBytes, err := c.requisicao(ctx, http.MethodPost, "/api/v1/instances/"+url.PathEscape(e.InstanciaID)+"/consent/request", corpo, "")
	if err != nil {
		return PedidoConsentimento{}, err
	}

	var res struct {
		RequestID string `json:"request_id"`
		ExpiresAt string `json:"expires_at"`
		Link      string `json:"link"`
	}
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return PedidoConsentimento{}, fmt.Errorf("syncz: desserializar pedido de consentimento: %w", err)
	}
	return PedidoConsentimento{
		RequestID: res.RequestID,
		ExpiraEm:  parseTimestamp(res.ExpiresAt),
		Link:      res.Link,
	}, nil
}

// ObterContratoTenant (task 589, CA-16) le GET /api/v1/contract -- so' o
// transporte REST implementa (ClienteContratoTenant; descoberta de contrato
// e' HTTP-only, T-22).
func (c *clienteREST) ObterContratoTenant(ctx context.Context) (ContratoTenant, error) {
	_, respBytes, err := c.requisicao(ctx, http.MethodGet, "/api/v1/contract", nil, "")
	if err != nil {
		return ContratoTenant{}, err
	}

	var res struct {
		APIVersion        string   `json:"api_version"`
		ContractHash      string   `json:"contract_hash"`
		ContractVersion   string   `json:"contract_version"`
		SupportedVersions []string `json:"supported_versions"`
		PreConsent        struct {
			Send    bool `json:"send"`
			Receive bool `json:"receive"`
		} `json:"pre_consent"`
	}
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return ContratoTenant{}, fmt.Errorf("syncz: desserializar contrato do tenant: %w", err)
	}
	return ContratoTenant{
		VersaoAPI:         res.APIVersion,
		HashContrato:      res.ContractHash,
		VersaoContrato:    res.ContractVersion,
		VersoesSuportadas: res.SupportedVersions,
		PreConsentimento: PreConsentimentoInfo{
			Enviar:  res.PreConsent.Send,
			Receber: res.PreConsent.Receive,
		},
	}, nil
}

func (c *clienteREST) Enviar(ctx context.Context, e EntradaEnvio) (Recibo, error) {
	corpo := map[string]any{
		"type": "text",
		"to": map[string]string{
			"phone": e.Para,
		},
		"text": map[string]string{
			"body": e.Texto,
		},
	}
	if strings.Contains(e.Para, "@") {
		corpo["to"] = map[string]string{"jid": e.Para}
	}

	resp, respBytes, err := c.requisicao(ctx, http.MethodPost, "/api/v1/instances/"+url.PathEscape(e.InstanciaID)+"/messages", corpo, e.ChaveIdempotencia)
	if err != nil {
		return Recibo{}, err
	}

	var res struct {
		MessageID string `json:"message_id"`
		Status    string `json:"status"`
	}
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return Recibo{}, fmt.Errorf("syncz: desserializar recibo: %w", err)
	}

	// 200 = duplicata reconhecida, 202 = novo aceite
	duplicada := resp.StatusCode == http.StatusOK || strings.EqualFold(res.Status, "duplicate")

	return Recibo{
		MensagemID: res.MessageID,
		Status:     res.Status,
		EnviadoEm:  time.Now().UTC(),
		Duplicada:  duplicada,
	}, nil
}

func (c *clienteREST) EnviarMidia(ctx context.Context, e EntradaEnvioMidia) (Recibo, error) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	if strings.Contains(e.Para, "@") {
		_ = writer.WriteField("to_jid", e.Para)
	} else {
		_ = writer.WriteField("to_phone", e.Para)
	}
	if e.Legenda != "" {
		_ = writer.WriteField("caption", e.Legenda)
	}

	filename := e.NomeArquivo
	if filename == "" {
		filename = "arquivo.bin"
	}
	part, err := writer.CreateFormFile("file", filename)
	if err != nil {
		return Recibo{}, fmt.Errorf("syncz: criar form file: %w", err)
	}
	if _, err := part.Write(e.Conteudo); err != nil {
		return Recibo{}, fmt.Errorf("syncz: escrever bytes do arquivo: %w", err)
	}
	if err := writer.Close(); err != nil {
		return Recibo{}, fmt.Errorf("syncz: fechar multipart writer: %w", err)
	}

	u := c.baseURL + "/api/v1/instances/" + url.PathEscape(e.InstanciaID) + "/messages"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, body)
	if err != nil {
		return Recibo{}, fmt.Errorf("syncz: criar requisicao midia: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.chaveAPI)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("Accept", "application/json")
	if e.ChaveIdempotencia != "" {
		req.Header.Set("Idempotency-Key", e.ChaveIdempotencia)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return Recibo{}, fmt.Errorf("syncz: envio midia rede: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return Recibo{}, fmt.Errorf("syncz: ler resposta midia: %w", err)
	}

	if resp.StatusCode >= 400 {
		var prob erroProblemDetails
		_ = json.Unmarshal(respBytes, &prob)
		msg := prob.Detail
		if msg == "" {
			msg = prob.Title
		}
		if msg == "" {
			msg = string(respBytes)
		}
		switch resp.StatusCode {
		case http.StatusNotFound:
			return Recibo{}, novoErroAPI(ErrNaoEncontrado, msg)
		case http.StatusBadRequest, http.StatusUnprocessableEntity:
			return Recibo{}, novoErroAPI(ErrInvalido, msg)
		case http.StatusUnauthorized:
			return Recibo{}, novoErroAPI(ErrCredencial, msg)
		case http.StatusForbidden:
			return Recibo{}, novoErroAPI(ErrSemPermissao, msg)
		case http.StatusConflict, http.StatusPreconditionFailed:
			return Recibo{}, novoErroAPI(ErrConflito, msg)
		default:
			return Recibo{}, novoErroAPI(fmt.Errorf("syncz: erro http %d", resp.StatusCode), msg)
		}
	}

	var res struct {
		MessageID string `json:"message_id"`
		Status    string `json:"status"`
	}
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return Recibo{}, fmt.Errorf("syncz: desserializar recibo midia: %w", err)
	}

	duplicada := resp.StatusCode == http.StatusOK || strings.EqualFold(res.Status, "duplicate")

	return Recibo{
		MensagemID: res.MessageID,
		Status:     res.Status,
		EnviadoEm:  time.Now().UTC(),
		Duplicada:  duplicada,
	}, nil
}

// StatusMensagem consulta GET .../messages/{message_id} -- ver
// internal/transport/rest/messages.go (handleGetMessageStatus/messageStatusResp)
// para o formato de resposta real. Campos opcionais (wa_message_id, error_code,
// error_message, sent_at, delivered_at, read_at) vêm ausentes/zero quando a
// mensagem ainda não passou por aquele estágio.
func (c *clienteREST) StatusMensagem(ctx context.Context, instanciaID, mensagemID string) (EstadoMensagem, error) {
	path := "/api/v1/instances/" + url.PathEscape(instanciaID) + "/messages/" + url.PathEscape(mensagemID)
	_, respBytes, err := c.requisicao(ctx, http.MethodGet, path, nil, "")
	if err != nil {
		return EstadoMensagem{}, err
	}

	var res struct {
		MessageID    string `json:"message_id"`
		Status       string `json:"status"`
		WAMessageID  string `json:"wa_message_id"`
		ErrorCode    string `json:"error_code"`
		ErrorMessage string `json:"error_message"`
		SendAttempts int    `json:"send_attempts"`
		QueuedAt     string `json:"queued_at"`
		SentAt       string `json:"sent_at"`
		DeliveredAt  string `json:"delivered_at"`
		ReadAt       string `json:"read_at"`
	}
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return EstadoMensagem{}, fmt.Errorf("syncz: desserializar status mensagem: %w", err)
	}

	return EstadoMensagem{
		MensagemID:    res.MessageID,
		Status:        res.Status,
		WAMensagemID:  res.WAMessageID,
		ErrorCode:     res.ErrorCode,
		ErrorMessage:  res.ErrorMessage,
		Tentativas:    res.SendAttempts,
		EnfileiradaEm: parseTimestamp(res.QueuedAt),
		EnviadaEm:     parseTimestamp(res.SentAt),
		EntregueEm:    parseTimestamp(res.DeliveredAt),
		LidaEm:        parseTimestamp(res.ReadAt),
	}, nil
}

// StatusOperacao consulta GET .../operations/{op_id} -- ver
// internal/transport/rest/operations.go (handleGetOperation/operationResp)
// para o formato de resposta real. Result vem cru (json.RawMessage): o
// formato depende de qual operação é (create_group, update_group_name, ...).
func (c *clienteREST) StatusOperacao(ctx context.Context, instanciaID, opID string) (EstadoOperacao, error) {
	path := "/api/v1/instances/" + url.PathEscape(instanciaID) + "/operations/" + url.PathEscape(opID)
	_, respBytes, err := c.requisicao(ctx, http.MethodGet, path, nil, "")
	if err != nil {
		return EstadoOperacao{}, err
	}

	var res struct {
		OpID       string          `json:"op_id"`
		Operation  string          `json:"operation"`
		Status     string          `json:"status"`
		Result     json.RawMessage `json:"result"`
		Error      string          `json:"error"`
		CreatedAt  string          `json:"created_at"`
		FinishedAt string          `json:"finished_at"`
	}
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return EstadoOperacao{}, fmt.Errorf("syncz: desserializar status operacao: %w", err)
	}

	return EstadoOperacao{
		OpID:         res.OpID,
		Operacao:     res.Operation,
		Status:       res.Status,
		Result:       res.Result,
		Erro:         res.Error,
		CriadaEm:     parseTimestamp(res.CreatedAt),
		FinalizadaEm: parseTimestamp(res.FinishedAt),
	}, nil
}

// pollResp é o formato comum das respostas de criar/encerrar enquete -- ver
// internal/transport/rest/dto.go (pollResp) e polls.go (toPollResp).
type pollResp struct {
	PollID          string   `json:"poll_id"`
	InstanceID      string   `json:"instance_id"`
	GroupJID        string   `json:"group_jid"`
	Question        string   `json:"question"`
	Options         []string `json:"options"`
	SelectableCount int      `json:"selectable_count"`
	IsClosed        bool     `json:"is_closed"`
	CreatedAt       int64    `json:"created_at"`
}

func enqueteFromResp(res pollResp) Enquete {
	return Enquete{
		PollID:        res.PollID,
		InstanciaID:   res.InstanceID,
		GrupoJID:      res.GroupJID,
		Pergunta:      res.Question,
		Opcoes:        res.Options,
		Selecionaveis: res.SelectableCount,
		Encerrada:     res.IsClosed,
		CriadaEm:      parseUnixSeconds(res.CreatedAt),
	}
}

// CriarEnquete consulta POST .../polls -- ver internal/transport/rest/polls.go
// (handleCreatePoll). Suporta ?wait= igual a CriarGrupo: o servidor só tem
// caminho assíncrono quando a instância não tem provider local (task 559).
func (c *clienteREST) CriarEnquete(ctx context.Context, e EntradaCriarEnquete) (Enquete, error) {
	corpo := map[string]any{
		"group_jid":        e.GrupoJID,
		"question":         e.Pergunta,
		"options":          e.Opcoes,
		"selectable_count": e.Selecionaveis,
	}
	path := comWaitQuery("/api/v1/instances/"+url.PathEscape(e.InstanciaID)+"/polls", e.EsperaSincrona)
	_, respBytes, err := c.requisicao(ctx, http.MethodPost, path, corpo, "")
	if err != nil {
		return Enquete{}, err
	}

	var res pollResp
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return Enquete{}, fmt.Errorf("syncz: desserializar enquete: %w", err)
	}
	return enqueteFromResp(res), nil
}

// ResultadoEnquete consulta GET .../polls/{poll_id}/results -- ver
// internal/transport/rest/polls.go (handleGetPollResults/toPollResultsResp).
func (c *clienteREST) ResultadoEnquete(ctx context.Context, instanciaID, pollID string) (EnqueteResultado, error) {
	path := "/api/v1/instances/" + url.PathEscape(instanciaID) + "/polls/" + url.PathEscape(pollID) + "/results"
	_, respBytes, err := c.requisicao(ctx, http.MethodGet, path, nil, "")
	if err != nil {
		return EnqueteResultado{}, err
	}

	var res struct {
		PollID  string `json:"poll_id"`
		Tallies []struct {
			Option string   `json:"option"`
			Count  int      `json:"count"`
			Voters []string `json:"voters"`
		} `json:"tallies"`
		TotalVoters int   `json:"total_voters"`
		ComputedAt  int64 `json:"computed_at"`
	}
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return EnqueteResultado{}, fmt.Errorf("syncz: desserializar resultado de enquete: %w", err)
	}

	opcoes := make([]EnqueteOpcaoResultado, len(res.Tallies))
	for i, t := range res.Tallies {
		opcoes[i] = EnqueteOpcaoResultado{
			Opcao:    t.Option,
			Votos:    t.Count,
			Votantes: t.Voters,
		}
	}
	return EnqueteResultado{
		PollID:        res.PollID,
		Opcoes:        opcoes,
		TotalVotantes: res.TotalVoters,
		CalculadoEm:   parseUnixSeconds(res.ComputedAt),
	}, nil
}

// EncerrarEnquete consulta POST .../polls/{poll_id}/close -- ver
// internal/transport/rest/polls.go (handleClosePoll). Sem EsperaSincrona:
// fechar é sempre local ao servidor (marca is_closed no banco), sem caminho
// assíncrono via provider (mesmo motivo de EntradaLinkConvite não ter).
func (c *clienteREST) EncerrarEnquete(ctx context.Context, instanciaID, pollID string) (Enquete, error) {
	path := "/api/v1/instances/" + url.PathEscape(instanciaID) + "/polls/" + url.PathEscape(pollID) + "/close"
	_, respBytes, err := c.requisicao(ctx, http.MethodPost, path, nil, "")
	if err != nil {
		return Enquete{}, err
	}

	var res pollResp
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return Enquete{}, fmt.Errorf("syncz: desserializar enquete: %w", err)
	}
	return enqueteFromResp(res), nil
}

// groupEventResp é o formato comum das respostas de evento de grupo -- ver
// internal/transport/rest/dto.go (groupEventResp) e group_events.go
// (toGroupEventResp).
type groupEventResp struct {
	EventID            string `json:"event_id"`
	InstanceID         string `json:"instance_id"`
	GroupJID           string `json:"group_jid"`
	Title              string `json:"title"`
	Description        string `json:"description"`
	StartTime          int64  `json:"start_time"`
	EndTime            int64  `json:"end_time"`
	ReminderOffsetSec  int64  `json:"reminder_offset_sec"`
	LocationName       string `json:"location_name"`
	JoinLink           string `json:"join_link"`
	IsCanceled         bool   `json:"is_canceled"`
	ExtraGuestsAllowed bool   `json:"extra_guests_allowed"`
	IsScheduleCall     bool   `json:"is_schedule_call"`
	CreatedAt          int64  `json:"created_at"`
	UpdatedAt          int64  `json:"updated_at"`
}

func eventoGrupoFromResp(res groupEventResp) EventoGrupo {
	return EventoGrupo{
		EventoID:                res.EventID,
		InstanciaID:             res.InstanceID,
		GrupoJID:                res.GroupJID,
		Titulo:                  res.Title,
		Descricao:               res.Description,
		Inicio:                  parseUnixSeconds(res.StartTime),
		Fim:                     parseUnixSeconds(res.EndTime),
		LembreteAntecedencia:    time.Duration(res.ReminderOffsetSec) * time.Second,
		LocalNome:               res.LocationName,
		LinkEntrada:             res.JoinLink,
		Cancelado:               res.IsCanceled,
		ChamadaAgendada:         res.IsScheduleCall,
		PermiteConvidadosExtras: res.ExtraGuestsAllowed,
		CriadoEm:                parseUnixSeconds(res.CreatedAt),
		AtualizadoEm:            parseUnixSeconds(res.UpdatedAt),
	}
}

// CriarEventoGrupo consulta POST .../group-events -- ver
// internal/transport/rest/group_events.go (handleCreateGroupEvent). Suporta
// ?wait= igual a CriarGrupo/CriarEnquete: o servidor só tem caminho
// assíncrono quando a instância não tem provider local (task 559).
func (c *clienteREST) CriarEventoGrupo(ctx context.Context, e EntradaCriarEventoGrupo) (EventoGrupo, error) {
	corpo := map[string]any{
		"group_jid":            e.GrupoJID,
		"title":                e.Titulo,
		"description":          e.Descricao,
		"start_time":           strconv.FormatInt(e.Inicio.Unix(), 10),
		"reminder_offset_sec":  int64(e.LembreteAntecedencia.Seconds()),
		"location_name":        e.LocalNome,
		"join_link":            e.LinkEntrada,
		"is_schedule_call":     e.ChamadaAgendada,
		"extra_guests_allowed": e.PermiteConvidadosExtras,
	}
	if !e.Fim.IsZero() {
		corpo["end_time"] = strconv.FormatInt(e.Fim.Unix(), 10)
	}
	if e.GarantirLembrete != nil {
		corpo["ensure_reminder"] = *e.GarantirLembrete
	}

	path := comWaitQuery("/api/v1/instances/"+url.PathEscape(e.InstanciaID)+"/group-events", e.EsperaSincrona)
	_, respBytes, err := c.requisicao(ctx, http.MethodPost, path, corpo, "")
	if err != nil {
		return EventoGrupo{}, err
	}

	var res groupEventResp
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return EventoGrupo{}, fmt.Errorf("syncz: desserializar evento de grupo: %w", err)
	}
	return eventoGrupoFromResp(res), nil
}

// AtualizarEventoGrupo consulta PATCH .../group-events/{event_id} -- ver
// internal/transport/rest/group_events.go (handleUpdateGroupEvent). Só os
// campos ponteiro presentes em e são enviados; os demais ficam como estão no
// servidor.
func (c *clienteREST) AtualizarEventoGrupo(ctx context.Context, e EntradaAtualizarEventoGrupo) (EventoGrupo, error) {
	corpo := map[string]any{}
	if e.Titulo != nil {
		corpo["title"] = *e.Titulo
	}
	if e.Descricao != nil {
		corpo["description"] = *e.Descricao
	}
	if e.Inicio != nil {
		corpo["start_time"] = strconv.FormatInt(e.Inicio.Unix(), 10)
	}
	if e.Fim != nil {
		corpo["end_time"] = strconv.FormatInt(e.Fim.Unix(), 10)
	}
	if e.LembreteAntecedencia != nil {
		corpo["reminder_offset_sec"] = int64(e.LembreteAntecedencia.Seconds())
	}
	if e.LocalNome != nil {
		corpo["location_name"] = *e.LocalNome
	}
	if e.LinkEntrada != nil {
		corpo["join_link"] = *e.LinkEntrada
	}

	path := comWaitQuery(fmt.Sprintf("/api/v1/instances/%s/group-events/%s", url.PathEscape(e.InstanciaID), url.PathEscape(e.EventoID)), e.EsperaSincrona)
	_, respBytes, err := c.requisicao(ctx, http.MethodPatch, path, corpo, "")
	if err != nil {
		return EventoGrupo{}, err
	}

	var res groupEventResp
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return EventoGrupo{}, fmt.Errorf("syncz: desserializar evento de grupo atualizado: %w", err)
	}
	return eventoGrupoFromResp(res), nil
}

// CancelarEventoGrupo consulta POST .../group-events/{event_id}/cancel -- ver
// internal/transport/rest/group_events.go (handleCancelGroupEvent). Cancelar
// duas vezes é idempotente (mesmo comportamento do servidor).
func (c *clienteREST) CancelarEventoGrupo(ctx context.Context, e EntradaCancelarEventoGrupo) (EventoGrupo, error) {
	path := comWaitQuery(fmt.Sprintf("/api/v1/instances/%s/group-events/%s/cancel", url.PathEscape(e.InstanciaID), url.PathEscape(e.EventoID)), e.EsperaSincrona)
	_, respBytes, err := c.requisicao(ctx, http.MethodPost, path, nil, "")
	if err != nil {
		return EventoGrupo{}, err
	}

	var res groupEventResp
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return EventoGrupo{}, fmt.Errorf("syncz: desserializar evento de grupo cancelado: %w", err)
	}
	return eventoGrupoFromResp(res), nil
}

// ResponderEventoGrupo consulta POST .../group-events/{event_id}/response --
// ver internal/transport/rest/group_events.go (handleSendEventResponse).
func (c *clienteREST) ResponderEventoGrupo(ctx context.Context, e EntradaResponderEventoGrupo) (RespostaEventoGrupo, error) {
	corpo := map[string]any{
		"response":          e.Resposta,
		"extra_guest_count": e.ConvidadosExtras,
	}
	path := comWaitQuery(fmt.Sprintf("/api/v1/instances/%s/group-events/%s/response", url.PathEscape(e.InstanciaID), url.PathEscape(e.EventoID)), e.EsperaSincrona)
	_, respBytes, err := c.requisicao(ctx, http.MethodPost, path, corpo, "")
	if err != nil {
		return RespostaEventoGrupo{}, err
	}

	var res struct {
		EventID   string `json:"event_id"`
		MessageID string `json:"message_id"`
		Response  string `json:"response"`
	}
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return RespostaEventoGrupo{}, fmt.Errorf("syncz: desserializar resposta de evento de grupo: %w", err)
	}
	return RespostaEventoGrupo{
		EventoID:   res.EventID,
		MensagemID: res.MessageID,
		Resposta:   res.Response,
	}, nil
}

func (c *clienteREST) CriarGrupo(ctx context.Context, e EntradaGrupo) (Grupo, error) {
	corpo := map[string]any{
		"subject":      e.Assunto,
		"participants": e.Participantes,
	}
	path := comWaitQuery("/api/v1/instances/"+url.PathEscape(e.InstanciaID)+"/groups", e.EsperaSincrona)
	_, respBytes, err := c.requisicao(ctx, http.MethodPost, path, corpo, "")
	if err != nil {
		return Grupo{}, err
	}

	var res struct {
		JID          string   `json:"jid"`
		Subject      string   `json:"subject"`
		Participants []string `json:"participants"`
		OwnerJID     string   `json:"owner_jid"`
		Status       string   `json:"status"`
		OpID         string   `json:"op_id"`
	}
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return Grupo{}, fmt.Errorf("syncz: desserializar grupo: %w", err)
	}

	return Grupo{
		JID:           res.JID,
		Assunto:       res.Subject,
		Participantes: res.Participants,
		DonoJID:       res.OwnerJID,
		Status:        res.Status,
		OpID:          res.OpID,
	}, nil
}

func (c *clienteREST) AtualizarNomeGrupo(ctx context.Context, e EntradaAtualizarNomeGrupo) (Grupo, error) {
	corpo := map[string]any{
		"subject": e.Assunto,
	}
	path := comWaitQuery(fmt.Sprintf("/api/v1/instances/%s/groups/%s", url.PathEscape(e.InstanciaID), url.PathEscape(e.GrupoJID)), e.EsperaSincrona)
	_, respBytes, err := c.requisicao(ctx, http.MethodPatch, path, corpo, "")
	if err != nil {
		return Grupo{}, err
	}

	var res struct {
		JID          string   `json:"jid"`
		Subject      string   `json:"subject"`
		Participants []string `json:"participants"`
		OwnerJID     string   `json:"owner_jid"`
		Status       string   `json:"status"`
		OpID         string   `json:"op_id"`
	}
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return Grupo{}, fmt.Errorf("syncz: desserializar grupo renomeado: %w", err)
	}

	return Grupo{
		JID:           res.JID,
		Assunto:       res.Subject,
		Participantes: res.Participants,
		DonoJID:       res.OwnerJID,
		Status:        res.Status,
		OpID:          res.OpID,
	}, nil
}

func (c *clienteREST) AtualizarFotoGrupo(ctx context.Context, e EntradaAtualizarFotoGrupo) (Grupo, error) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("file", "group_photo.jpg")
	if err != nil {
		return Grupo{}, fmt.Errorf("syncz: criar form file foto: %w", err)
	}
	if _, err := part.Write(e.Foto); err != nil {
		return Grupo{}, fmt.Errorf("syncz: escrever foto: %w", err)
	}
	if err := writer.Close(); err != nil {
		return Grupo{}, fmt.Errorf("syncz: fechar writer foto: %w", err)
	}

	path := comWaitQuery(fmt.Sprintf("/api/v1/instances/%s/groups/%s/photo", url.PathEscape(e.InstanciaID), url.PathEscape(e.GrupoJID)), e.EsperaSincrona)
	u := c.baseURL + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, body)
	if err != nil {
		return Grupo{}, fmt.Errorf("syncz: criar requisicao foto: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.chaveAPI)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return Grupo{}, fmt.Errorf("syncz: envio foto rede: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return Grupo{}, fmt.Errorf("syncz: ler resposta foto: %w", err)
	}

	if resp.StatusCode >= 400 {
		return Grupo{}, novoErroAPI(ErrInvalido, string(respBytes))
	}

	var res struct {
		JID          string   `json:"jid"`
		Subject      string   `json:"subject"`
		Participants []string `json:"participants"`
		OwnerJID     string   `json:"owner_jid"`
		Status       string   `json:"status"`
		OpID         string   `json:"op_id"`
	}
	_ = json.Unmarshal(respBytes, &res)

	return Grupo{
		JID:           res.JID,
		Assunto:       res.Subject,
		Participantes: res.Participants,
		DonoJID:       res.OwnerJID,
		Status:        res.Status,
		OpID:          res.OpID,
	}, nil
}

func (c *clienteREST) AdicionarParticipantes(ctx context.Context, e EntradaParticipantes) error {
	corpo := map[string]any{
		"phones": e.Participantes,
	}
	path := comWaitQuery(fmt.Sprintf("/api/v1/instances/%s/groups/%s/participants", url.PathEscape(e.InstanciaID), url.PathEscape(e.GrupoJID)), e.EsperaSincrona)
	_, _, err := c.requisicao(ctx, http.MethodPost, path, corpo, "")
	return err
}

func (c *clienteREST) RemoverParticipantes(ctx context.Context, e EntradaParticipantes) error {
	corpo := map[string]any{
		"phones": e.Participantes,
	}
	path := comWaitQuery(fmt.Sprintf("/api/v1/instances/%s/groups/%s/participants", url.PathEscape(e.InstanciaID), url.PathEscape(e.GrupoJID)), e.EsperaSincrona)
	_, _, err := c.requisicao(ctx, http.MethodDelete, path, corpo, "")
	return err
}

func (c *clienteREST) LinkConviteGrupo(ctx context.Context, e EntradaLinkConvite) (string, error) {
	path := fmt.Sprintf("/api/v1/instances/%s/groups/%s/invite-link?reset=%t", url.PathEscape(e.InstanciaID), url.PathEscape(e.GrupoJID), e.Reset)
	_, respBytes, err := c.requisicao(ctx, http.MethodGet, path, nil, "")
	if err != nil {
		return "", err
	}

	var res struct {
		InviteURL string `json:"invite_url"`
	}
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return "", fmt.Errorf("syncz: desserializar invite link: %w", err)
	}

	return res.InviteURL, nil
}

func (c *clienteREST) SairGrupo(ctx context.Context, e EntradaSairGrupo) error {
	path := comWaitQuery(fmt.Sprintf("/api/v1/instances/%s/groups/%s/leave", url.PathEscape(e.InstanciaID), url.PathEscape(e.GrupoJID)), e.EsperaSincrona)
	_, _, err := c.requisicao(ctx, http.MethodPost, path, nil, "")
	return err
}

func (c *clienteREST) FixarChat(ctx context.Context, e EntradaFixarChat) error {
	corpo := map[string]any{
		"pinned": e.Fixar,
	}
	path := comWaitQuery(fmt.Sprintf("/api/v1/instances/%s/chats/%s/pin", url.PathEscape(e.InstanciaID), url.PathEscape(e.ChatJID)), e.EsperaSincrona)
	_, _, err := c.requisicao(ctx, http.MethodPost, path, corpo, "")
	return err
}

func (c *clienteREST) MarcarLido(ctx context.Context, e EntradaMarcarLido) error {
	corpo := map[string]any{
		"message_ids": e.MensagensIDs,
	}
	path := comWaitQuery(fmt.Sprintf("/api/v1/instances/%s/chats/%s/read", url.PathEscape(e.InstanciaID), url.PathEscape(e.ChatJID)), e.EsperaSincrona)
	_, _, err := c.requisicao(ctx, http.MethodPost, path, corpo, "")
	return err
}

func (c *clienteREST) DefinirLeituraChat(ctx context.Context, e EntradaDefinirLeituraChat) error {
	corpo := map[string]any{
		"read": e.Lida,
	}
	path := comWaitQuery(fmt.Sprintf("/api/v1/instances/%s/chats/%s/read-state", url.PathEscape(e.InstanciaID), url.PathEscape(e.ChatJID)), e.EsperaSincrona)
	_, _, err := c.requisicao(ctx, http.MethodPost, path, corpo, "")
	return err
}

func (c *clienteREST) VerificarNumeros(ctx context.Context, e EntradaVerificarNumeros) ([]NumeroInfo, error) {
	corpo := map[string]any{
		"phones": e.Telefones,
	}
	path := fmt.Sprintf("/api/v1/instances/%s/check-numbers", url.PathEscape(e.InstanciaID))
	_, respBytes, err := c.requisicao(ctx, http.MethodPost, path, corpo, "")
	if err != nil {
		return nil, err
	}

	var res struct {
		Results []struct {
			Phone        string `json:"phone"`
			IsInWhatsApp bool   `json:"is_in_whatsapp"`
			JID          string `json:"jid"`
			LID          string `json:"lid"`
		} `json:"results"`
	}
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return nil, fmt.Errorf("syncz: desserializar check numbers: %w", err)
	}

	itens := make([]NumeroInfo, len(res.Results))
	for i, r := range res.Results {
		itens[i] = NumeroInfo{
			Telefone:   r.Phone,
			NoWhatsApp: r.IsInWhatsApp,
			JID:        r.JID,
			LID:        r.LID,
		}
	}

	return itens, nil
}

func (c *clienteREST) ObterPolitica(ctx context.Context, instanciaID string) (Politica, error) {
	path := fmt.Sprintf("/api/v1/instances/%s/policy", url.PathEscape(instanciaID))
	_, respBytes, err := c.requisicao(ctx, http.MethodGet, path, nil, "")
	if err != nil {
		return Politica{}, err
	}

	return Politica{
		InstanciaID: instanciaID,
		ConfigJSON:  respBytes,
	}, nil
}

func (c *clienteREST) AtualizarPolitica(ctx context.Context, instanciaID string, configJSON []byte) (Politica, error) {
	path := fmt.Sprintf("/api/v1/instances/%s/policy", url.PathEscape(instanciaID))
	u := c.baseURL + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, bytes.NewReader(configJSON))
	if err != nil {
		return Politica{}, fmt.Errorf("syncz: criar requisicao politica: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.chaveAPI)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return Politica{}, fmt.Errorf("syncz: envio politica rede: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return Politica{}, fmt.Errorf("syncz: ler resposta politica: %w", err)
	}

	if resp.StatusCode >= 400 {
		return Politica{}, novoErroAPI(ErrInvalido, string(respBytes))
	}

	return Politica{
		InstanciaID: instanciaID,
		ConfigJSON:  respBytes,
	}, nil
}

func (c *clienteREST) ObterUso(ctx context.Context, tenantID string) (Uso, error) {
	_, respBytes, err := c.requisicao(ctx, http.MethodGet, "/api/v1/usage", nil, "")
	if err != nil {
		return Uso{}, err
	}

	var res struct {
		From   string           `json:"from"`
		To     string           `json:"to"`
		Totals map[string]int64 `json:"totals"`
	}
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return Uso{}, fmt.Errorf("syncz: desserializar uso: %w", err)
	}

	var msgOut, instDays int64
	if res.Totals != nil {
		msgOut = res.Totals["msg_out"]
		instDays = res.Totals["instance_day"]
	}

	return Uso{
		TenantID:          tenantID,
		MensagensEnviadas: msgOut,
		InstanciasAtivas:  instDays,
	}, nil
}

// suppress unused import warning
var _ = strconv.Itoa
