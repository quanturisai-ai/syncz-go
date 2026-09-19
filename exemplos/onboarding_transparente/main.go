// Command onboarding_transparente prova, contra o sandbox, as cinco chamadas
// do onboarding transparente descritas em
// docs/guia/do-zero-a-primeira-mensagem.md: criar instância, acompanhar o
// pareamento até `paired`, buscar o contrato + mostrar numa tela própria,
// e registrar o aceite com evidência (ou, alternativamente, pedir o
// consentimento por WhatsApp em vez de desenhar tela nenhuma).
//
// O sandbox (syncz.NovoSandbox) é um dublê em memória: ele não modela a
// máquina de estados completa da instância (pairing/paired/awaiting_consent/
// connected) como o gateway real faz -- Instancia.Status nele fica em
// "created" o tempo todo, exceto pelos efeitos colaterais explícitos que
// RevogarInstancia/Desparear/AtualizarEstadoDesejado já simulam. Os rótulos
// abaixo narram os MESMOS quatro passos que o gateway real percorre nesta
// sprint (tasks 578-589): pairing (instância recém-criada, aguardando
// escaneio), paired (telefone escaneou o QR), awaiting_consent (pareado, mas
// sem contrato vigente -- é quando SolicitarConsentimento ou a tela própria
// entram em cena) e connected (aceite registrado, contrato vigente).
package main

import (
	"context"
	"fmt"
	"log"

	syncz "github.com/quanturisai-ai/syncz-go"
)

func main() {
	ctx := context.Background()
	c := syncz.NovoSandbox()
	defer func() { _ = c.Fechar() }()

	var rotulos []string
	rotular := func(rotulo string) {
		rotulos = append(rotulos, rotulo)
		log.Println(rotulo)
	}

	// 1. Criar instância -- nasce esperando o escaneio do QR.
	inst, err := c.CriarInstancia(ctx, syncz.EntradaCriarInstancia{Nome: "onboarding-transparente"})
	if err != nil {
		log.Fatal(err)
	}
	rotular("pairing")

	// 2. Receber o QR (por webhook `instance.pairing_qr` ou por
	// AcompanharPareamento, que devolve o mesmo evento em stream -- aqui só
	// lemos o estado atual, que é o suficiente para o sandbox).
	pareamento, err := c.EstadoPareamento(ctx, inst.ID)
	if err != nil {
		log.Fatal(err)
	}

	// 3. Mostrar o QR (aqui, só confirmamos que ele existe) e simular o
	// titular escaneando com o telefone.
	if pareamento.QRCode == "" {
		log.Fatal("esperava um QR code do sandbox")
	}
	c.InjetarPareamento(inst.ID, "5511999999999")
	rotular("paired")

	// 4. Buscar o Contrato para desenhar a tela própria de aceite -- o
	// titular decide entre aceitar ali mesmo ou receber o pedido pelo
	// WhatsApp (SolicitarConsentimento, comentado abaixo como alternativa).
	contrato, err := c.Contrato(ctx, inst.ID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("contrato: %s v%d (%d escopo(s) obrigatorio(s))\n", contrato.Nome, contrato.Versao, len(contrato.EscoposObrigatorios))
	rotular("awaiting_consent")

	// Alternativa a RegistrarAceite: pedir por WhatsApp em vez de tela
	// própria.
	//
	//   pedido, err := c.SolicitarConsentimento(ctx, syncz.EntradaSolicitarConsentimento{
	//       InstanciaID: inst.ID,
	//       Via:         "whatsapp",
	//   })

	// 5. RegistrarAceite com Evidencia -- a prova de que o titular aceitou
	// pela tela própria (não pelo wizard hospedado nem pelo link de
	// WhatsApp).
	if err := c.RegistrarAceite(ctx, syncz.EntradaAceite{
		InstanciaID: inst.ID,
		Evidencia: map[string]any{
			"metodo":     "tela-propria",
			"referencia": "exemplo-onboarding-transparente",
		},
	}); err != nil {
		log.Fatal(err)
	}
	rotular("connected")

	final, err := c.ObterInstancia(ctx, inst.ID)
	if err != nil {
		log.Fatal(err)
	}
	if final.Consentimento.Status != "granted" {
		log.Fatalf("esperava consentimento granted, obtive %q", final.Consentimento.Status)
	}

	fmt.Println(joinComVirgula(rotulos))
}

func joinComVirgula(rotulos []string) string {
	out := ""
	for i, r := range rotulos {
		if i > 0 {
			out += ", "
		}
		out += r
	}
	return out
}
