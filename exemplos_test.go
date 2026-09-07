package syncz_test

import (
	"bufio"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestExemploPrimeiraMensagemCabeEm30Linhas(t *testing.T) {
	exemploPath := filepath.Join("exemplos", "primeira-mensagem", "main.go")
	file, err := os.Open(exemploPath)
	if err != nil {
		t.Fatalf("falha ao abrir exemplo: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	linhasUteis := 0
	inImport := false

	for scanner.Scan() {
		linha := strings.TrimSpace(scanner.Text())

		// Ignora linhas vazias e comentários
		if linha == "" || strings.HasPrefix(linha, "//") || strings.HasPrefix(linha, "/*") {
			continue
		}

		if strings.HasPrefix(linha, "import (") {
			inImport = true
			continue
		}
		if inImport {
			if strings.HasPrefix(linha, ")") {
				inImport = false
			}
			continue
		}
		if strings.HasPrefix(linha, "import ") {
			continue
		}

		linhasUteis++
	}

	t.Logf("Exemplo possui %d linhas uteis de codigo proprio", linhasUteis)
	if linhasUteis >= 30 {
		t.Fatalf("exemplo excede o limite maximo de 30 linhas uteis: possui %d linhas", linhasUteis)
	}

	// Verifica se compila corretamente
	cmd := exec.Command("go", "build", "-o", filepath.Join(t.TempDir(), "exemplo-bin"), "./exemplos/primeira-mensagem")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("exemplo nao compila: %v\nsaida:\n%s", err, string(out))
	}
}
