package syncz_test

import (
	"encoding/json"
	"testing"

	syncz "github.com/quanturisai-ai/syncz-go"
)

// envelope monta o JSON de um core.Event com o payload dado, no mesmo
// formato que trafega no outbox, no stream SSE/gRPC e no corpo do webhook.
func envelope(t *testing.T, tipo string, payload map[string]any) []byte {
	t.Helper()
	corpoPayload, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	ev := map[string]any{
		"id":          "evt-1",
		"type":        tipo,
		"instance_id": "11111111-1111-1111-1111-111111111111",
		"seq":         int64(1),
		"occurred_at": "2026-09-18T00:00:00Z",
		"payload":     json.RawMessage(corpoPayload),
		"trace_id":    "trace-1",
	}
	data, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return data
}

// decodificarCaso roda DecodificarEvento e confere o envelope basico (Tipo,
// PayloadTipado nao-nil) -- cada TestDecodificarEvento_* so' confere o campo
// especifico do seu payload por cima disso (task 589, CA-16: uma funcao
// pequena por tipo, em vez de uma tabela so' com closures, para nao estourar
// o teto de complexidade ciclomatica do gocyclo).
func decodificarCaso(t *testing.T, tipo string, payload map[string]any) any {
	t.Helper()
	ev, err := syncz.DecodificarEvento(envelope(t, tipo, payload))
	if err != nil {
		t.Fatalf("DecodificarEvento(%s): %v", tipo, err)
	}
	if ev.Tipo != tipo {
		t.Fatalf("esperava Tipo=%s, obteve %s", tipo, ev.Tipo)
	}
	if ev.PayloadTipado == nil {
		t.Fatalf("esperava PayloadTipado preenchido para %s", tipo)
	}
	return ev.PayloadTipado
}

func TestDecodificarEvento_StatusChanged(t *testing.T) {
	p := decodificarCaso(t, syncz.TipoInstanceStatusChanged, map[string]any{
		"from_state": "pairing", "to_state": "paired", "reason": "pair_success",
	})
	sc, ok := p.(*syncz.PayloadStatusChanged)
	if !ok || sc.DeEstado != "pairing" || sc.ParaEstado != "paired" || sc.Motivo != "pair_success" {
		t.Fatalf("PayloadStatusChanged incorreto: %+v (ok=%v)", p, ok)
	}
}

func TestDecodificarEvento_PairingQR(t *testing.T) {
	p := decodificarCaso(t, syncz.TipoInstancePairingQR, map[string]any{
		"qr_code": "2@abc", "seq": 3, "expires_at": "2026-09-18T00:05:00Z",
	})
	pq, ok := p.(*syncz.PayloadPairingQR)
	if !ok || pq.QRCode != "2@abc" || pq.Seq != 3 {
		t.Fatalf("PayloadPairingQR incorreto: %+v (ok=%v)", p, ok)
	}
}

func TestDecodificarEvento_Paired(t *testing.T) {
	p := decodificarCaso(t, syncz.TipoInstancePaired, map[string]any{"phone": "5511999999999"})
	pp, ok := p.(*syncz.PayloadPaired)
	if !ok || pp.Telefone != "5511999999999" {
		t.Fatalf("PayloadPaired incorreto: %+v (ok=%v)", p, ok)
	}
}

func TestDecodificarEvento_PairingFailed(t *testing.T) {
	p := decodificarCaso(t, syncz.TipoInstancePairingFailed, map[string]any{"error": "timeout"})
	pf, ok := p.(*syncz.PayloadPairingFailed)
	if !ok || pf.Erro != "timeout" {
		t.Fatalf("PayloadPairingFailed incorreto: %+v (ok=%v)", p, ok)
	}
}

func TestDecodificarEvento_ConsentRequested(t *testing.T) {
	p := decodificarCaso(t, syncz.TipoInstanceConsentRequested, map[string]any{
		"instance_id": "i1", "via": "whatsapp", "to": "self", "expires_at": "2026-09-19T00:00:00Z",
	})
	cr, ok := p.(*syncz.PayloadConsentRequested)
	if !ok || cr.Via != "whatsapp" || cr.Para != "self" {
		t.Fatalf("PayloadConsentRequested incorreto: %+v (ok=%v)", p, ok)
	}
}

func TestDecodificarEvento_ConsentGranted(t *testing.T) {
	p := decodificarCaso(t, syncz.TipoInstanceConsentGranted, map[string]any{
		"instance_id": "i1", "origin": "tenant", "granted_by": "api_key:abc",
		"contract_version": 2, "scopes": []string{"chat:read_dm"}, "consented_at": "2026-09-18T00:00:00Z",
	})
	cg, ok := p.(*syncz.PayloadConsentGranted)
	if !ok || cg.Origem != "tenant" || cg.VersaoContrato != 2 || len(cg.Escopos) != 1 {
		t.Fatalf("PayloadConsentGranted incorreto: %+v (ok=%v)", p, ok)
	}
}

func TestDecodificarEvento_ContractRevoked(t *testing.T) {
	p := decodificarCaso(t, syncz.TipoContractRevoked, map[string]any{
		"instance_id": "i1", "by": "tenant", "origin": "gateway_web",
	})
	rv, ok := p.(*syncz.PayloadContractRevoked)
	if !ok || rv.Por != "tenant" || rv.Origem != "gateway_web" {
		t.Fatalf("PayloadContractRevoked incorreto: %+v (ok=%v)", p, ok)
	}
}

func TestDecodificarEvento_PreConsentOperation(t *testing.T) {
	p := decodificarCaso(t, syncz.TipoInstancePreConsentOperation, map[string]any{
		"instance_id": "i1", "provisional": true, "direction": "send", "at": "2026-09-18T00:00:00Z",
	})
	pc, ok := p.(*syncz.PayloadPreConsentOperation)
	if !ok || !pc.Provisorio || pc.Direcao != "send" {
		t.Fatalf("PayloadPreConsentOperation incorreto: %+v (ok=%v)", p, ok)
	}
}

func TestDecodificarEvento_MessageReceived(t *testing.T) {
	p := decodificarCaso(t, syncz.TipoMessageReceived, map[string]any{
		"wa_message_id": "wa-1", "from": map[string]any{"phone_jid": "5511@s.whatsapp.net"},
		"contact": map[string]any{"phone_jid": "5511@s.whatsapp.net"}, "is_group": false,
		"type": "text", "text": "oi", "origin": "inbound", "order_key": "ok-1",
		"received_at": "2026-09-18T00:00:00Z",
	})
	mr, ok := p.(*syncz.PayloadMessageReceived)
	if !ok || mr.WAMensagemID != "wa-1" || mr.Texto != "oi" {
		t.Fatalf("PayloadMessageReceived incorreto: %+v (ok=%v)", p, ok)
	}
}

// TestDecodificarEvento_TipoDesconhecido prova que tipos nao mapeados (ex.:
// eventos de grupo) deixam PayloadTipado nil sem erro -- Payload continua
// disponivel para decodificacao manual.
func TestDecodificarEvento_TipoDesconhecido(t *testing.T) {
	ev, err := syncz.DecodificarEvento(envelope(t, "group.subject_changed", map[string]any{"subject": "novo nome"}))
	if err != nil {
		t.Fatalf("DecodificarEvento: %v", err)
	}
	if ev.PayloadTipado != nil {
		t.Fatalf("esperava PayloadTipado nil para tipo desconhecido, obteve %+v", ev.PayloadTipado)
	}
	if len(ev.Payload) == 0 {
		t.Fatal("esperava Payload (raw) preenchido mesmo sem PayloadTipado")
	}
}
