package util

import (
	"context"
)

type IO[T any] func(context.Context) (T, error)

type Void struct{}

var Empty Void = struct{}{}

func Of[T any](t T) IO[T] {
	return func(_ context.Context) (T, error) {
		return t, nil
	}
}

func Bind[T, U any](
	i IO[T],
	f func(T) IO[U],
) IO[U] {
	return func(ctx context.Context) (u U, e error) {
		t, e := i(ctx)
		if nil != e {
			return u, e
		}
		return f(t)(ctx)
	}
}

func (i IO[T]) OrElse(alt IO[T]) IO[T] {
	return func(ctx context.Context) (T, error) {
		t1, e := i(ctx)
		if nil == e {
			return t1, nil
		}
		return alt(ctx)
	}
}

func Lift[T, U any](
	pure func(T) (U, error),
) func(T) IO[U] {
	return func(t T) IO[U] {
		return func(_ context.Context) (U, error) {
			return pure(t)
		}
	}
}
