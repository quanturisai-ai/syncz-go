package syncz

import (
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
			return nil, nil, novoErroAPI(ErrNaoEncontrado, msg)
		case http.StatusBadRequest, http.StatusUnprocessableEntity:
			return nil, nil, novoErroAPI(ErrInvalido, msg)
		case http.StatusUnauthorized:
			return nil, nil, novoErroAPI(ErrCredencial, msg)
		case http.StatusForbidden:
			return nil, nil, novoErroAPI(ErrSemPermissao, msg)
		case http.StatusConflict, http.StatusPreconditionFailed:
			return nil, nil, novoErroAPI(ErrConflito, msg)
		case http.StatusTooManyRequests, http.StatusServiceUnavailable:
			return nil, nil, novoErroAPI(ErrIndisponivel, msg)
		default:
			return nil, nil, novoErroAPI(fmt.Errorf("syncz: erro http %d", resp.StatusCode), msg)
		}
	}

	return resp, respBytes, nil
}

func parseTimestamp(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, _ := time.Parse(time.RFC3339, s)
	return t
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

	var res struct {
		ID        string `json:"id"`
		TenantID  string `json:"tenant_id"`
		Name      string `json:"name"`
		Phone     string `json:"phone"`
		Status    string `json:"status"`
		CreatedAt string `json:"created_at"`
		UpdatedAt string `json:"updated_at"`
	}
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return Instancia{}, fmt.Errorf("syncz: desserializar instancia: %w", err)
	}

	return Instancia{
		ID:           res.ID,
		TenantID:     res.TenantID,
		Nome:         res.Name,
		Telefone:     res.Phone,
		Status:       res.Status,
		CriadaEm:     parseTimestamp(res.CreatedAt),
		AtualizadaEm: parseTimestamp(res.UpdatedAt),
	}, nil
}

func (c *clienteREST) ObterInstancia(ctx context.Context, id string) (Instancia, error) {
	_, respBytes, err := c.requisicao(ctx, http.MethodGet, "/api/v1/instances/"+url.PathEscape(id), nil, "")
	if err != nil {
		return Instancia{}, err
	}

	var res struct {
		ID        string `json:"id"`
		TenantID  string `json:"tenant_id"`
		Name      string `json:"name"`
		Phone     string `json:"phone"`
		Status    string `json:"status"`
		CreatedAt string `json:"created_at"`
		UpdatedAt string `json:"updated_at"`
	}
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return Instancia{}, fmt.Errorf("syncz: desserializar instancia: %w", err)
	}

	return Instancia{
		ID:           res.ID,
		TenantID:     res.TenantID,
		Nome:         res.Name,
		Telefone:     res.Phone,
		Status:       res.Status,
		CriadaEm:     parseTimestamp(res.CreatedAt),
		AtualizadaEm: parseTimestamp(res.UpdatedAt),
	}, nil
}

func (c *clienteREST) ListarInstancias(ctx context.Context, e EntradaListarInstancias) (ListaInstancias, error) {
	path := fmt.Sprintf("/api/v1/instances?limit=%d&offset=%d", e.Limite, e.Offset)
	_, respBytes, err := c.requisicao(ctx, http.MethodGet, path, nil, "")
	if err != nil {
		return ListaInstancias{}, err
	}

	var res struct {
		Instances []struct {
			ID        string `json:"id"`
			TenantID  string `json:"tenant_id"`
			Name      string `json:"name"`
			Phone     string `json:"phone"`
			Status    string `json:"status"`
			CreatedAt string `json:"created_at"`
			UpdatedAt string `json:"updated_at"`
		} `json:"instances"`
		Total int32 `json:"total"`
	}
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return ListaInstancias{}, fmt.Errorf("syncz: desserializar lista de instancias: %w", err)
	}

	lista := make([]Instancia, len(res.Instances))
	for i, item := range res.Instances {
		lista[i] = Instancia{
			ID:           item.ID,
			TenantID:     item.TenantID,
			Nome:         item.Name,
			Telefone:     item.Phone,
			Status:       item.Status,
			CriadaEm:     parseTimestamp(item.CreatedAt),
			AtualizadaEm: parseTimestamp(item.UpdatedAt),
		}
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

	var res struct {
		ID        string `json:"id"`
		TenantID  string `json:"tenant_id"`
		Name      string `json:"name"`
		Phone     string `json:"phone"`
		Status    string `json:"status"`
		CreatedAt string `json:"created_at"`
		UpdatedAt string `json:"updated_at"`
	}
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return Instancia{}, fmt.Errorf("syncz: desserializar instancia revogada: %w", err)
	}

	return Instancia{
		ID:           res.ID,
		TenantID:     res.TenantID,
		Nome:         res.Name,
		Telefone:     res.Phone,
		Status:       res.Status,
		CriadaEm:     parseTimestamp(res.CreatedAt),
		AtualizadaEm: parseTimestamp(res.UpdatedAt),
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
	_, _, err := c.requisicao(ctx, http.MethodPost, "/api/v1/instances/"+url.PathEscape(e.InstanciaID)+"/consent", corpo, "")
	return err
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
