package syncz

import (
	"encoding/json"
	"fmt"
	"time"
)

// Constantes de Evento.Tipo reconhecidas por DecodificarEvento (task 589,
// CA-16) -- espelham internal/core/event_types.go e
// internal/core/instance/state.go (statusChangedPayload).
const (
	TipoInstanceStatusChanged       = "instance.status_changed"
	TipoInstancePairingQR           = "instance.pairing_qr"
	TipoInstancePaired              = "instance.paired"
	TipoInstancePairingFailed       = "instance.pairing_failed"
	TipoInstanceConsentRequested    = "instance.consent_requested"
	TipoInstanceConsentGranted      = "instance.consent_granted"
	TipoContractRevoked             = "contract.revoked"
	TipoInstancePreConsentOperation = "instance.pre_consent_operation"
	TipoMessageReceived             = "message.received"
)

// PayloadStatusChanged e' o corpo de instance.status_changed.
type PayloadStatusChanged struct {
	DeEstado   string `json:"from_state"`
	ParaEstado string `json:"to_state"`
	Motivo     string `json:"reason"`
}

// PayloadPairingQR e' o corpo de instance.pairing_qr.
type PayloadPairingQR struct {
	QRCode   string    `json:"qr_code"`
	PairCode string    `json:"pair_code,omitempty"`
	Seq      int       `json:"seq"`
	ExpiraEm time.Time `json:"expires_at"`
}

// PayloadPaired e' o corpo de instance.paired.
type PayloadPaired struct {
	Telefone string `json:"phone,omitempty"`
	LID      string `json:"lid,omitempty"`
}

// PayloadPairingFailed e' o corpo de instance.pairing_failed.
type PayloadPairingFailed struct {
	Erro string `json:"error"`
}

// PayloadConsentRequested e' o corpo de instance.consent_requested (CA-07).
type PayloadConsentRequested struct {
	InstanciaID string    `json:"instance_id"`
	Via         string    `json:"via"`
	Para        string    `json:"to"`
	ExpiraEm    time.Time `json:"expires_at"`
}

// PayloadConsentGranted e' o corpo de instance.consent_granted (CA-07).
type PayloadConsentGranted struct {
	InstanciaID    string    `json:"instance_id"`
	Origem         string    `json:"origin"`
	ConcedidoPor   string    `json:"granted_by"`
	VersaoContrato int       `json:"contract_version"`
	Escopos        []string  `json:"scopes"`
	ConsentidoEm   time.Time `json:"consented_at"`
}

// PayloadContractRevoked e' o corpo de contract.revoked (com Origem desde a
// task 582, CA-07).
type PayloadContractRevoked struct {
	InstanciaID string `json:"instance_id"`
	Por         string `json:"by"`
	Origem      string `json:"origin,omitempty"`
}

// PayloadPreConsentOperation e' o corpo de instance.pre_consent_operation
// (task 586, CA-12).
type PayloadPreConsentOperation struct {
	InstanciaID string    `json:"instance_id"`
	Provisorio  bool      `json:"provisional"`
	Direcao     string    `json:"direction"` // "send" | "receive"
	Em          time.Time `json:"at"`
}

// PayloadContato e' o remetente/contato de PayloadMessageReceived.
type PayloadContato struct {
	ID           string `json:"id,omitempty"`
	NomeExibicao string `json:"display_name,omitempty"`
	PhoneJID     string `json:"phone_jid,omitempty"`
	LIDJID       string `json:"lid_jid,omitempty"`
}

// PayloadMidia descreve o anexo de PayloadMessageReceived, quando houver.
type PayloadMidia struct {
	URL          string `json:"url,omitempty"`
	DirectPath   string `json:"direct_path,omitempty"`
	MimeType     string `json:"mimetype,omitempty"`
	TamanhoBytes uint64 `json:"size_bytes,omitempty"`
	Segundos     uint32 `json:"seconds,omitempty"`
}

// PayloadMessageReceived e' o corpo de message.received.
type PayloadMessageReceived struct {
	WAMensagemID string         `json:"wa_message_id"`
	De           PayloadContato `json:"from"`
	Contato      PayloadContato `json:"contact"`
	EhGrupo      bool           `json:"is_group"`
	Tipo         string         `json:"type"`
	Texto        string         `json:"text,omitempty"`
	Origem       string         `json:"origin"`
	EcoDe        string         `json:"echo_of,omitempty"`
	CitadoID     string         `json:"quoted_id,omitempty"`
	Midia        *PayloadMidia  `json:"media,omitempty"`
	OrderKey     string         `json:"order_key"`
	RecebidoEm   time.Time      `json:"received_at"`
	ContatoID    string         `json:"contact_id,omitempty"`
}

// payloadTipadoPara devolve um ponteiro para struct vazia do tipo certo, ou
// nil se Tipo nao for reconhecido (ex.: eventos de grupo/enquete, que ja tem
// seu proprio decodificador em EventoGrupo/EnqueteResultado).
func payloadTipadoPara(tipo string) any {
	switch tipo {
	case TipoInstanceStatusChanged:
		return &PayloadStatusChanged{}
	case TipoInstancePairingQR:
		return &PayloadPairingQR{}
	case TipoInstancePaired:
		return &PayloadPaired{}
	case TipoInstancePairingFailed:
		return &PayloadPairingFailed{}
	case TipoInstanceConsentRequested:
		return &PayloadConsentRequested{}
	case TipoInstanceConsentGranted:
		return &PayloadConsentGranted{}
	case TipoContractRevoked:
		return &PayloadContractRevoked{}
	case TipoInstancePreConsentOperation:
		return &PayloadPreConsentOperation{}
	case TipoMessageReceived:
		return &PayloadMessageReceived{}
	default:
		return nil
	}
}

// DecodificarEvento (task 589, CA-16) decodifica o envelope de um evento --
// mesmo JSON no outbox, no stream SSE/gRPC e no corpo do webhook do tenant
// (internal/core.Event) -- e, quando Tipo e' um dos reconhecidos (constantes
// Tipo* acima), preenche Evento.PayloadTipado com a struct correspondente.
// PayloadTipado fica nil para tipos nao mapeados aqui; Evento.Payload
// (json.RawMessage) continua disponivel para decodificacao manual em
// qualquer caso.
func DecodificarEvento(data []byte) (Evento, error) {
	var ev Evento
	if err := json.Unmarshal(data, &ev); err != nil {
		return Evento{}, fmt.Errorf("syncz: decodificar evento: %w", err)
	}

	alvo := payloadTipadoPara(ev.Tipo)
	if alvo == nil {
		return ev, nil
	}
	if len(ev.Payload) > 0 {
		if err := json.Unmarshal(ev.Payload, alvo); err != nil {
			return Evento{}, fmt.Errorf("syncz: decodificar payload de %q: %w", ev.Tipo, err)
		}
	}
	ev.PayloadTipado = alvo
	return ev, nil
}
