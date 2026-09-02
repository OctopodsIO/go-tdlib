package client

import (
	"context"
	"sync"
	"time"
)

type Client struct {
	jsonClient      *JsonClient
	extraGenerator  ExtraGenerator
	responses       chan *Response
	resultHandler   ResultHandler
	catchersStore   *sync.Map
	fallbackTimeout time.Duration
	isClosed        bool
}

type Option func(*Client)

func WithExtraGenerator(extraGenerator ExtraGenerator) Option {
	return func(client *Client) {
		client.extraGenerator = extraGenerator
	}
}

func WithFallbackTimeout(timeout time.Duration) Option {
	return func(client *Client) {
		client.fallbackTimeout = timeout
	}
}

func WithProxy(req *AddProxyRequest) Option {
	return func(client *Client) {
		client.AddProxy(context.Background(), req)
	}
}

func WithResultHandler(resultHandler ResultHandler) Option {
	return func(client *Client) {
		client.resultHandler = resultHandler
	}
}

type ResultHandler interface {
	OnResult(result Type)
}

type CallbackResultHandler struct {
	callback func(result Type)
}

func (handler *CallbackResultHandler) OnResult(result Type) {
	handler.callback(result)
}

func NewCallbackResultHandler(callback func(result Type)) *CallbackResultHandler {
	return &CallbackResultHandler{
		callback: callback,
	}
}

func NewClient(authorizationStateHandler AuthorizationStateHandler, options ...Option) (*Client, error) {
	client := &Client{
		jsonClient:    NewJsonClient(),
		responses:     make(chan *Response, 1000),
		catchersStore: &sync.Map{},
		isClosed:      false,
	}

	client.extraGenerator = UuidV4Generator()
	client.resultHandler = NewCallbackResultHandler(func(result Type) {})
	client.fallbackTimeout = 60 * time.Second

	// Octopods patch: options are applied before the receiver runs, and on this
	// goroutine. Upstream applied them in goroutines after starting it, so
	// WithResultHandler wrote client.resultHandler while receiver() was reading
	// it, with no synchronisation. A torn read of that field crashes the whole
	// process, taking every other client in it down.
	//
	// The cost is that WithProxy no longer works: it sends a request, and no
	// request can complete before the receiver starts.
	for _, option := range options {
		option(client)
	}

	tdlibInstance.addClient(client)
	go client.receiver()

	err := Authorize(client, authorizationStateHandler)
	if err != nil {
		// Octopods patch: the client is returned even though authorization
		// failed. The native TDLib client is alive and holding the database
		// open at this point, and nothing else can reach it, so returning nil
		// leaks it for the life of the process. Callers must still check the
		// error first; the client is only there to be closed.
		return client, err
	}

	return client, nil
}

func (client *Client) receiver() {
	for response := range client.responses {
		if response.MetaExtra != "" {
			value, ok := client.catchersStore.Load(response.MetaExtra)
			if ok {
				value.(chan *Response) <- response
			}
		}

		typ, err := UnmarshalType(response.Data)
		if err != nil {
			continue
		}

		client.resultHandler.OnResult(typ)

		if typ.GetConstructor() == ConstructorUpdateAuthorizationState &&
			typ.(*UpdateAuthorizationState).AuthorizationState.AuthorizationStateConstructor() == ConstructorAuthorizationStateClosed {
			client.isClosed = true
			close(client.responses)
		}
	}
}

func (client *Client) Send(ctx context.Context, req Request) (*Response, error) {
	req.SetExtra(client.extraGenerator())
	req.SetType(req.GetFunctionName())

	catcher := make(chan *Response)

	client.catchersStore.Store(req.GetExtra(), catcher)

	defer func() {
		client.catchersStore.Delete(req.GetExtra())
		close(catcher)
	}()

	err := client.jsonClient.Send(req)
	if err != nil {
		return nil, err
	}

	fallbackCtx, cancel := context.WithTimeout(context.Background(), client.fallbackTimeout)
	defer cancel()

	select {
	case response := <-catcher:
		return response, nil

	case <-ctx.Done():
		return nil, ctx.Err()

	case <-fallbackCtx.Done():
		return nil, fallbackCtx.Err()
	}
}

func (client *Client) Execute(req Request) (*Response, error) {
	req.SetExtra(client.extraGenerator())
	req.SetType(req.GetFunctionName())

	return client.jsonClient.Execute(req)
}
