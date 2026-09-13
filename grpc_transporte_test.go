package syncz

import (
	"crypto/tls"
	"errors"
	"testing"
)

// T-A7b: escolha da credencial de transporte do canal gRPC. O caso sem opção
// nenhuma precisa continuar em texto puro para quem usa v0.1.2 fora da 443.
func TestEscolhaDoTransporteGRPC(t *testing.T) {
	casos := []struct {
		nome   string
		opcoes Opcoes
		tls    bool
	}{
		{"localhost sem opcao segue texto puro", Opcoes{EnderecoGRPC: "localhost:50051"}, false},
		{"host interno nao-loopback segue texto puro", Opcoes{EnderecoGRPC: "gateway.exemplo:50051"}, false},
		{"ip sem opcao segue texto puro", Opcoes{EnderecoGRPC: "10.0.0.5:9090"}, false},
		{"porta 443 liga TLS sozinha", Opcoes{EnderecoGRPC: "api.exemplo.com:443"}, true},
		{"porta 443 com esquema dns liga TLS", Opcoes{EnderecoGRPC: "dns:///api.exemplo.com:443"}, true},
		{"ipv6 na 443 liga TLS", Opcoes{EnderecoGRPC: "[::1]:443"}, true},
		{"porta 4430 nao e 443", Opcoes{EnderecoGRPC: "api.exemplo.com:4430"}, false},
		{"sem porta explicita segue texto puro", Opcoes{EnderecoGRPC: "api.exemplo.com"}, false},
		{"unix segue texto puro", Opcoes{EnderecoGRPC: "unix:///tmp/syncz.sock"}, false},
		{"TLSGRPC forca TLS em qualquer porta", Opcoes{EnderecoGRPC: "localhost:50051", TLSGRPC: &tls.Config{}}, true},
		{"GRPCSemTLS desliga TLS na 443", Opcoes{EnderecoGRPC: "api.exemplo.com:443", GRPCSemTLS: true}, false},
	}
	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			if got := usaTLSGRPC(c.opcoes); got != c.tls {
				t.Fatalf("usaTLSGRPC(%q) = %v, esperava %v", c.opcoes.EnderecoGRPC, got, c.tls)
			}
			protocolo := credencialTransporte(c.opcoes).Info().SecurityProtocol
			esperado := "insecure"
			if c.tls {
				esperado = "tls"
			}
			if protocolo != esperado {
				t.Fatalf("SecurityProtocol = %q, esperava %q", protocolo, esperado)
			}
		})
	}
}

func TestTLSGRPCNaoMutaConfigDoChamador(t *testing.T) {
	cfg := &tls.Config{ServerName: "api.exemplo.com"}
	_ = credencialTransporte(Opcoes{EnderecoGRPC: "x:1", TLSGRPC: cfg})
	if cfg.MinVersion != 0 {
		t.Fatalf("a config do chamador foi alterada: MinVersion=%d", cfg.MinVersion)
	}
}

func TestTLSGRPCEGRPCSemTLSSaoExclusivos(t *testing.T) {
	o := Opcoes{EnderecoGRPC: "localhost:50051", ChaveAPI: "k", TLSGRPC: &tls.Config{}, GRPCSemTLS: true}
	if err := o.normalizar(); !errors.Is(err, ErrConfiguracao) {
		t.Fatalf("esperava ErrConfiguracao, obteve %v", err)
	}
}
