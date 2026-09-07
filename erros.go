package syncz

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// ErrNaoEncontrado indica recurso inexistente (HTTP 404 / gRPC NotFound).
	ErrNaoEncontrado = errors.New("syncz: recurso nao encontrado")
	// ErrInvalido indica argumentos inválidos na requisição (HTTP 400 / gRPC InvalidArgument).
	ErrInvalido = errors.New("syncz: argumento invalido")
	// ErrCredencial indica falha de autenticação (HTTP 401 / gRPC Unauthenticated).
	ErrCredencial = errors.New("syncz: credencial invalida ou ausente")
	// ErrSemPermissao indica acesso proibido (HTTP 403 / gRPC PermissionDenied).
	ErrSemPermissao = errors.New("syncz: sem permissao")
	// ErrConflito indica conflito de estado ou duplicidade incompatível (HTTP 409, 412 / gRPC AlreadyExists, FailedPrecondition).
	ErrConflito = errors.New("syncz: conflito")
	// ErrIndisponivel indica serviço indisponível temporariamente (HTTP 503 / gRPC Unavailable).
	ErrIndisponivel = errors.New("syncz: servico indisponivel")
	// ErrConfiguracao indica parâmetros de inicialização do SDK inválidos.
	ErrConfiguracao = errors.New("syncz: erro de configuracao")
)

// ErroAPI encapsula um erro do SDK preservando a mensagem original do servidor
// e permitindo correspondência com erros sentinela via errors.Is.
type ErroAPI struct {
	Base     error
	Mensagem string
}

func (e *ErroAPI) Error() string {
	if e.Mensagem != "" {
		return fmt.Sprintf("%v: %s", e.Base, e.Mensagem)
	}
	return e.Base.Error()
}

func (e *ErroAPI) Unwrap() error {
	return e.Base
}

func novoErroAPI(base error, mensagem string) error {
	return &ErroAPI{
		Base:     base,
		Mensagem: mensagem,
	}
}

// mapearErroGRPC converte códigos gRPC nos erros sentinela do SDK preservando a mensagem.
func mapearErroGRPC(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}

	msg := st.Message()
	switch st.Code() {
	case codes.NotFound:
		return novoErroAPI(ErrNaoEncontrado, msg)
	case codes.InvalidArgument:
		return novoErroAPI(ErrInvalido, msg)
	case codes.Unauthenticated:
		return novoErroAPI(ErrCredencial, msg)
	case codes.PermissionDenied:
		return novoErroAPI(ErrSemPermissao, msg)
	case codes.AlreadyExists, codes.FailedPrecondition:
		return novoErroAPI(ErrConflito, msg)
	case codes.Unavailable:
		return novoErroAPI(ErrIndisponivel, msg)
	default:
		return err
	}
}
