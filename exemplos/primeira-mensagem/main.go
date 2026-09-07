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
