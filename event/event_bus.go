package event

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type UnknownEventToDispatchError struct {
	event string
}

func (u UnknownEventToDispatchError) Error() string {
	return fmt.Sprintf("event %q is not declared to dispatch", u.event)
}

type Listener interface {
	Listen(ctx context.Context, ev Event) error
}

type Bus map[string][]Listener

func NewBus() Bus {
	return make(map[string][]Listener)
}

func (e Bus) Subscribe(eventName string, listeners ...Listener) {
	e[eventName] = append(e[eventName], listeners...)
}

func (e Bus) Dispatch(ctx context.Context, ev Event) error {
	list, ok := e[ev.EventName()]
	if !ok {
		return UnknownEventToDispatchError{event: ev.EventName()}
	}

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs = make([]error, 0, len(list))
	)
	for _, l := range list {
		wg.Add(1)
		go func(l Listener) {
			defer wg.Done()
			if err := l.Listen(ctx, ev); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}(l)
	}
	wg.Wait()

	if err := errors.Join(errs...); err != nil {
		return DispatchError{msg: err.Error(), eventName: ev.EventName(), err: err}
	}
	return nil
}

type DispatchError struct {
	msg, eventName string
	err            error
}

func (d DispatchError) Error() string {
	return fmt.Sprintf("failed to dispatch event %s error: %s", d.eventName, d.msg)
}

func (d DispatchError) Unwrap() error {
	return d.err
}

func NewDispatchError(msg string, eventName string) DispatchError {
	return DispatchError{msg: msg, eventName: eventName}
}
