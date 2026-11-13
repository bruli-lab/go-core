package event_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bruli-lab/go-core/event"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestBus_Dispatch(t *testing.T) {
	type args struct {
		subscribedEventName string
		dispatchedEventName string
		listeners           []event.Listener
	}
	tests := []struct {
		name            string
		args            args
		expectsUnknown  bool
		expectsDispatch bool
		expectedErrMsg  string
		expectedErrMsgs []string
	}{
		{
			name: "with an unsubscribed event, then it returns an unknown event to dispatch error",
			args: args{
				subscribedEventName: "test",
				dispatchedEventName: "other",
			},
			expectsUnknown: true,
		},
		{
			name: "with all listeners returns an error, then it returns all messages in the error",
			args: args{
				listeners: []event.Listener{
					listener{msgError: new("error1")},
					listener{msgError: new("error2")},
				},
				subscribedEventName: "test",
				dispatchedEventName: "test",
			},
			expectsDispatch: true,
			expectedErrMsgs: []string{"error1", "error2"},
		},
		{
			name: "with one listener returns an error, and the other that returns nil, then it returns the first error",
			args: args{
				listeners: []event.Listener{
					listener{msgError: new("error1")},
					listener{},
				},
				subscribedEventName: "test",
				dispatchedEventName: "test",
			},
			expectsDispatch: true,
			expectedErrMsg:  "failed to dispatch event test error: error1",
		},
		{
			name: "with all listeners returning nil, then it returns nil",
			args: args{
				listeners: []event.Listener{
					listener{},
					listener{},
				},
				subscribedEventName: "test",
				dispatchedEventName: "test",
			},
		},
	}
	for _, tt := range tests {
		t.Run(`Given a event bus with subscribed listeners to event
		when Dispatch method is called `+tt.name, func(t *testing.T) {
			t.Parallel()
			bus := event.NewBus()
			ev := eventTest{name: tt.args.dispatchedEventName}
			bus.Subscribe(tt.args.subscribedEventName, tt.args.listeners...)
			err := bus.Dispatch(t.Context(), ev)
			switch {
			case tt.expectsUnknown:
				var unknown event.UnknownEventToDispatchError
				require.ErrorAs(t, err, &unknown)
			case tt.expectsDispatch:
				var dispatch event.DispatchError
				require.ErrorAs(t, err, &dispatch)
				if tt.expectedErrMsg != "" {
					require.Equal(t, tt.expectedErrMsg, err.Error())
				}
				for _, msg := range tt.expectedErrMsgs {
					require.Contains(t, err.Error(), msg)
				}
			default:
				require.NoError(t, err)
			}
		})
	}
}

func TestBus_Subscribe(t *testing.T) {
	tests := []struct {
		name          string
		subscriptions [][]int
		expectedCalls []int32
	}{
		{
			name:          "once with two listeners, then Dispatch calls both of them",
			subscriptions: [][]int{{0, 1}},
			expectedCalls: []int32{1, 1},
		},
		{
			name:          "twice for the same event, then Dispatch calls the listeners of both subscriptions",
			subscriptions: [][]int{{0}, {1}},
			expectedCalls: []int32{1, 1},
		},
	}
	for _, tt := range tests {
		t.Run(`Given a event bus
		when Subscribe method is called `+tt.name, func(t *testing.T) {
			t.Parallel()
			counters := []*atomic.Int32{{}, {}}
			bus := event.NewBus()
			for _, subscription := range tt.subscriptions {
				listeners := make([]event.Listener, len(subscription))
				for i, index := range subscription {
					listeners[i] = countingListener{calls: counters[index]}
				}
				bus.Subscribe("test", listeners...)
			}
			err := bus.Dispatch(t.Context(), eventTest{name: "test"})
			require.NoError(t, err)
			for i, expected := range tt.expectedCalls {
				require.Equal(t, expected, counters[i].Load())
			}
		})
	}
}

func TestDispatchError_Unwrap(t *testing.T) {
	t.Run(`Given a event bus with a listener that returns an error
	when Dispatch method is called, then the listener error can be reached with errors.Is`, func(t *testing.T) {
		t.Parallel()
		errListener := errors.New("listener failed")
		bus := event.NewBus()
		bus.Subscribe("test", failingListener{err: errListener})
		err := bus.Dispatch(t.Context(), eventTest{name: "test"})
		require.ErrorIs(t, err, errListener)
	})
}

type eventTest struct {
	name string
}

func (e eventTest) EventID() uuid.UUID {
	return uuid.New()
}

func (e eventTest) EventName() string {
	return e.name
}

func (e eventTest) EventAt() time.Time {
	return time.Now()
}

func (e eventTest) AggregateRootID() string {
	return uuid.NewString()
}

type listener struct {
	msgError *string
}

func (l listener) Listen(_ context.Context, _ event.Event) error {
	if l.msgError != nil {
		return errors.New(*l.msgError)
	}
	return nil
}

type countingListener struct {
	calls *atomic.Int32
}

func (c countingListener) Listen(_ context.Context, _ event.Event) error {
	c.calls.Add(1)
	return nil
}

type failingListener struct {
	err error
}

func (f failingListener) Listen(_ context.Context, _ event.Event) error {
	return f.err
}
