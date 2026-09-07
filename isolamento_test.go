package syncz_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestSDKNaoImportaGateway prova a CA-01 (F13-01): nenhum arquivo do módulo
// sdk/go pode importar um pacote do gateway (internal/, cmd/ ou o módulo raiz
// github.com/quanturisai-ai/sync-zap). Se importasse, o SDK deixaria de ser
// publicável isoladamente — um tenant que fizesse `go get` do SDK arrastaria
// junto o código do gateway, que é justamente o que a Fase 13.1 fecha.
//
// A varredura roda dentro do `go test` isolado (job sdk-isolado / `make
// sdk-test`), então falha tanto no CI quanto na máquina de quem desenvolve.
func TestSDKNaoImportaGateway(t *testing.T) {
	const (
		moduloSDK     = "github.com/quanturisai-ai/syncz-go"
		moduloGateway = "github.com/quanturisai-ai/sync-zap"
	)

	raiz, err := filepath.Abs(".")
	if err != nil {
		t.Fatalf("resolver raiz do módulo: %v", err)
	}

	fset := token.NewFileSet()
	var violacoes []string

	err = filepath.Walk(raiz, func(caminho string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(caminho, ".go") {
			return nil
		}

		arq, err := parser.ParseFile(fset, caminho, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}

		rel, _ := filepath.Rel(raiz, caminho)
		for _, imp := range arq.Imports {
			via, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				continue
			}
			if via == moduloSDK || strings.HasPrefix(via, moduloSDK+"/") {
				continue // o próprio SDK
			}
			if via == moduloGateway || strings.HasPrefix(via, moduloGateway+"/") {
				violacoes = append(violacoes, rel+" → "+via)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("varrer sdk/go: %v", err)
	}

	if len(violacoes) > 0 {
		t.Fatalf("sdk/go importa pacote(s) do gateway — o SDK deixa de ser publicável isoladamente:\n  %s",
			strings.Join(violacoes, "\n  "))
	}
}
