package syncz_test

import (
	"os"
	"regexp"
	"testing"
)

// CA-01 (T-21): O SDK não pode arrastar dependências pesadas do gateway (whatsmeow, river, pgx, goose).
func TestSDKNaoArrastaGateway(t *testing.T) {
	data, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatalf("falha ao ler go.mod do sdk/go: %v", err)
	}

	proibidos := []string{
		`whatsmeow`,
		`riverqueue`,
		`jackc/pgx`,
		`pressly/goose`,
	}

	for _, p := range proibidos {
		re := regexp.MustCompile(p)
		if re.Match(data) {
			t.Errorf("go.mod do sdk/go contém dependência proibida do gateway: %s", p)
		}
	}
}
