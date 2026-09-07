package syncz

import (
	"net/http"
	"time"
)

// Opcoes define a configuração para criação de um Cliente do SDK.
type Opcoes struct {
	BaseURL       string        // Endpoint HTTP/REST (exclusivo com EnderecoGRPC).
	EnderecoGRPC  string        // Endpoint host:porta gRPC (exclusivo com BaseURL).
	ChaveAPI      string        // Token Bearer / Chave de API obrigatória.
	Prazo         time.Duration // Timeout padrão por requisição (default 30s).
	JanelaWebhook time.Duration // Janela de validação do timestamp do webhook (default 5min).
	HTTPClient    *http.Client  // Cliente HTTP customizado (opcional, para testes).
}

func (o *Opcoes) normalizar() error {
	if o.BaseURL != "" && o.EnderecoGRPC != "" {
		return novoErroAPI(ErrConfiguracao, "BaseURL e EnderecoGRPC sao mutuamente exclusivos")
	}
	if o.BaseURL == "" && o.EnderecoGRPC == "" {
		return novoErroAPI(ErrConfiguracao, "deve informar BaseURL ou EnderecoGRPC")
	}
	if o.ChaveAPI == "" {
		return novoErroAPI(ErrConfiguracao, "ChaveAPI obrigatoria")
	}
	if o.Prazo <= 0 {
		o.Prazo = 30 * time.Second
	}
	if o.JanelaWebhook <= 0 {
		o.JanelaWebhook = JanelaWebhookPadrao
	}
	return nil
}

// Novo inicializa um novo Cliente sync-zap baseado nas opções fornecidas.
func Novo(o Opcoes) (Cliente, error) {
	if err := o.normalizar(); err != nil {
		return nil, err
	}
	if o.EnderecoGRPC != "" {
		return novoClienteGRPC(o)
	}
	return novoClienteREST(o)
}
