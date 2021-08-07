package sdk

import (
	"context"
)

type ApiClient interface {
	Create(context.Context, string) error
	Upload(context.Context, string) error
	Status(context.Context, string) (string, error)
}
