package syncz

import (
	"crypto/tls"
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

	// TLSGRPC força TLS no canal gRPC com esta configuração, em qualquer porta.
	// Nil segue a escolha automática: TLS quando EnderecoGRPC termina em ":443",
	// texto puro nos demais casos (ex.: "localhost:50051"). RootCAs nil usa as
	// raízes do sistema; ServerName vazio usa o host de EnderecoGRPC.
	TLSGRPC *tls.Config
	// GRPCSemTLS força texto puro (h2c) no canal gRPC, inclusive na porta 443.
	// Exclusivo com TLSGRPC.
	GRPCSemTLS bool
}

func (o *Opcoes) normalizar() error {
	if o.BaseURL != "" && o.EnderecoGRPC != "" {
		return novoErroAPI(ErrConfiguracao, "BaseURL e EnderecoGRPC sao mutuamente exclusivos")
	}
	if o.BaseURL == "" && o.EnderecoGRPC == "" {
		return novoErroAPI(ErrConfiguracao, "deve informar BaseURL ou EnderecoGRPC")
	}
	if o.TLSGRPC != nil && o.GRPCSemTLS {
		return novoErroAPI(ErrConfiguracao, "TLSGRPC e GRPCSemTLS sao mutuamente exclusivos")
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
