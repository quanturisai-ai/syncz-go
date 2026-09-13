package syncz_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	syncz "github.com/quanturisai-ai/syncz-go"
	synczv1 "github.com/quanturisai-ai/syncz-go/synczv1"
)

// certAutoassinado gera, em memória, um certificado para 127.0.0.1/localhost.
// Nada vai a disco: o repositório não guarda chave privada, nem de teste.
func certAutoassinado(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	chave, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("gerar chave: %v", err)
	}
	modelo := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "syncz-teste"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, modelo, modelo, &chave.PublicKey, chave)
	if err != nil {
		t.Fatalf("criar certificado: %v", err)
	}
	folha, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("ler certificado: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(folha)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: chave, Leaf: folha}, pool
}

// subirServidorGRPC sobe o mock em TCP real (127.0.0.1:0), com ou sem TLS.
func subirServidorGRPC(t *testing.T, opts ...grpc.ServerOption) (string, *mockSyncZapServer) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer(opts...)
	mock := &mockSyncZapServer{}
	synczv1.RegisterSyncZapServer(s, mock)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	return lis.Addr().String(), mock
}

func criarComNovo(t *testing.T, o syncz.Opcoes) (syncz.Instancia, error) {
	t.Helper()
	cli, err := syncz.Novo(o)
	if err != nil {
		t.Fatalf("Novo: %v", err)
	}
	defer func() { _ = cli.Fechar() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return cli.CriarInstancia(ctx, syncz.EntradaCriarInstancia{Nome: "tls", Telefone: "5511999999999"})
}

// T-A7b: com TLSGRPC (RootCAs custom), a chamada autenticada atravessa um
// servidor gRPC sob TLS e a chave chega no metadata authorization.
func TestNovoGRPCComTLSAutoassinado(t *testing.T) {
	cert, pool := certAutoassinado(t)
	endereco, mock := subirServidorGRPC(t, grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	})))

	inst, err := criarComNovo(t, syncz.Opcoes{
		EnderecoGRPC: endereco,
		ChaveAPI:     "chave-tls-123",
		TLSGRPC:      &tls.Config{RootCAs: pool},
	})
	if err != nil {
		t.Fatalf("CriarInstancia sob TLS falhou: %v", err)
	}
	if inst.ID != "inst-123" {
		t.Errorf("id inesperado: %s", inst.ID)
	}
	if mock.receivedToken != "Bearer chave-tls-123" {
		t.Errorf("authorization nao chegou sobre TLS: %q", mock.receivedToken)
	}
}

// Sem RootCAs a verificação usa as raízes do sistema: o certificado
// autoassinado é recusado e a chave nunca chega ao servidor.
func TestNovoGRPCComTLSVerificaCertificado(t *testing.T) {
	cert, _ := certAutoassinado(t)
	endereco, mock := subirServidorGRPC(t, grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
	})))

	_, err := criarComNovo(t, syncz.Opcoes{
		EnderecoGRPC: endereco,
		ChaveAPI:     "chave-nao-pode-vazar",
		TLSGRPC:      &tls.Config{},
	})
	if err == nil {
		t.Fatal("esperava falha de verificacao do certificado autoassinado")
	}
	if mock.receivedToken != "" {
		t.Errorf("a chave chegou ao servidor sem certificado verificado: %q", mock.receivedToken)
	}
}

// O caminho de sempre (v0.1.2): EnderecoGRPC sem opção de TLS fala texto puro
// com um servidor sem TLS, e a credencial por chamada continua indo.
func TestNovoGRPCSemTLSContinuaFuncionando(t *testing.T) {
	endereco, mock := subirServidorGRPC(t)

	for _, o := range []syncz.Opcoes{
		{EnderecoGRPC: endereco, ChaveAPI: "chave-h2c"},
		{EnderecoGRPC: endereco, ChaveAPI: "chave-h2c", GRPCSemTLS: true},
	} {
		mock.receivedToken = ""
		if _, err := criarComNovo(t, o); err != nil {
			t.Fatalf("CriarInstancia em texto puro falhou (GRPCSemTLS=%v): %v", o.GRPCSemTLS, err)
		}
		if mock.receivedToken != "Bearer chave-h2c" {
			t.Errorf("authorization inesperado: %q", mock.receivedToken)
		}
	}
}
