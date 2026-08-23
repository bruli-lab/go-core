package cqs

import (
	"context"
	"fmt"
)

//go:generate go tool moq -out query_handler_mock.go . QueryHandler

type InvalidQueryError struct {
	expected string
	had      string
}

func NewInvalidQueryError(expected, had string) InvalidQueryError {
	return InvalidQueryError{expected: expected, had: had}
}

func (e InvalidQueryError) Error() string {
	return fmt.Sprintf("invalid query, expected '%s' but found '%s'", e.expected, e.had)
}

type Query interface {
	Name() string
}

type QueryName string

func (qn QueryName) Name() string {
	return string(qn)
}

type QueryHandler interface {
	Handle(ctx context.Context, query Query) (any, error)
}

type QueryHandlerFunc func(ctx context.Context, query Query) (any, error)

func (f QueryHandlerFunc) Handle(ctx context.Context, query Query) (any, error) {
	return f(ctx, query)
}
