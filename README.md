# syncz-go — SDK Go do sync-zap

Cliente Go do gateway `sync-zap`. A mesma interface fala **REST** e **gRPC**: você
escolhe o transporte por opção, não por reescrever o código.

> **Conteúdo gerado.** A fonte deste repositório é `sdk/go/` do gateway, espelhado
> a cada tag. Edição feita aqui desaparece na próxima publicação — veja
> `PUBLICACAO.md`.

## Instalação

```bash
go get github.com/quanturisai-ai/syncz-go@v0.1.0
```

`v0.x` significa interface ainda em movimento: pode quebrar em uma minor. Fixe a
versão no seu `go.mod`.

## Primeira mensagem

```go
package main

import (
	"context"
	"log"
	"os"

	syncz "github.com/quanturisai-ai/syncz-go"
)

func main() {
	c, err := syncz.Novo(syncz.Opcoes{
		BaseURL:  os.Getenv("SYNCZ_BASE_URL"),
		ChaveAPI: os.Getenv("SYNCZ_API_KEY"),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Fechar() }()

	r, err := c.Enviar(context.Background(), syncz.EntradaEnvio{
		InstanciaID:       os.Getenv("SYNCZ_INSTANCIA"),
		Para:              "5511999999999",
		Texto:             "primeira mensagem pelo SDK",
		ChaveIdempotencia: "primeira-1",
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Println("enviada:", r.MensagemID)
}
```

O mesmo arquivo, executável, está em [`exemplos/primeira-mensagem`](exemplos/primeira-mensagem).

## gRPC com TLS (gateway público)

O gateway público publica gRPC na porta 443, com TLS. Endereço terminado em
`:443` liga TLS sozinho, com o certificado verificado contra as raízes do sistema
(a partir da `v0.1.3`):

```go
c, err := syncz.Novo(syncz.Opcoes{
	EnderecoGRPC: "gateway.exemplo.com:443", // :443 => TLS automático
	ChaveAPI:     os.Getenv("SYNCZ_API_KEY"),
})
```

| Situação | Opção |
|---|---|
| `host:443` | nada: TLS automático, raízes do sistema |
| TLS em outra porta, CA própria ou `ServerName` | `TLSGRPC: &tls.Config{RootCAs: pool}` |
| texto puro (h2c) mesmo na 443 | `GRPCSemTLS: true` |
| `localhost:50051`, `gateway:50051` (dev, rede interna) | nada: texto puro, como na `v0.1.2` |

`TLSGRPC` e `GRPCSemTLS` são mutuamente exclusivos. A chave vai no metadata
`authorization` de cada chamada nos dois transportes. Fora da sua máquina ou da
rede interna, use TLS.

## O que vem junto

| | |
|---|---|
| `syncz.Novo` | cliente REST (padrão) ou gRPC, mesma interface |
| `syncz.NovoSandbox` | gateway falso em memória — testa sua integração sem rede e sem instância |
| `syncz.VerificarWebhook` | valida a assinatura HMAC dos webhooks recebidos |
| `synczv1` | stubs gRPC gerados do `.proto` do gateway |

## Contrato e compatibilidade

O contrato é do gateway, não do SDK: `GET /api/v1/contract` devolve o hash do
contrato que a sua instância está servindo, a versão semântica dele
(`contract_version`) e as versões ainda atendidas (`supported_versions`).
Divergiu do que este SDK espera? Atualize o SDK — o gateway decide as regras.

`contract_version` não é o `v1` do caminho: aquele é a versão do transporte e
não muda. Campo ou rpc novo sobe a *minor*; remoção, renomeação ou troca de
tipo sobe a *major*. É por ela que dá para saber se a divergência que o hash
acusou exige trabalho seu ou não.

Bug, dúvida ou pedido de capacidade: abra no repositório do gateway. Aqui não há
código para consertar, só o resultado da publicação.
