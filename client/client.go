package client

import (
	"context"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Octopods patch: tag only sendMessage request IDs so a response remains
// recognizable if it arrives after the caller's deadline. No content is logged.
const sendTimingPrefix = "octopods-send:"

func sendExtra(req Request, extra string) string {
	if _, ok := req.(*SendMessageRequest); ok {
		return sendTimingPrefix + extra
	}
	return extra
}

func logSendTiming(stage string, clientID int, extra string) {
	if !strings.HasPrefix(extra, sendTimingPrefix) {
		return
	}
	log.Printf("tdlib_send_timing stage=%s at=%s client_id=%d extra=%s",
		stage, time.Now().UTC().Format(time.RFC3339Nano), clientID, extra)
}

type Client struct {
	jsonClient      *JsonClient
	extraGenerator  ExtraGenerator
	responses       chan *Response
	resultHandler   ResultHandler
	catchersStore   *sync.Map
	fallbackTimeout time.Duration

	// Octopods patch: atomic because receiver() writes it while the shared
	// tdlibInstance.receiver() reads it, on a different goroutine. Upstream
	// left it a plain bool.
	isClosed atomic.Bool
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
			logSendTiming("client_receiver", client.jsonClient.id, response.MetaExtra)
			value, ok := client.catchersStore.Load(response.MetaExtra)
			if ok {
				value.(chan *Response) <- response
				logSendTiming("handed_to_waiter", client.jsonClient.id, response.MetaExtra)
			}
		}

		typ, err := UnmarshalType(response.Data)
		if err != nil {
			continue
		}

		client.resultHandler.OnResult(typ)

		if typ.GetConstructor() == ConstructorUpdateAuthorizationState &&
			typ.(*UpdateAuthorizationState).AuthorizationState.AuthorizationStateConstructor() == ConstructorAuthorizationStateClosed {
			client.isClosed.Store(true)
			close(client.responses)
		}
	}
}

func (client *Client) Send(ctx context.Context, req Request) (*Response, error) {
	req.SetExtra(sendExtra(req, client.extraGenerator()))
	req.SetType(req.GetFunctionName())
	if send, ok := req.(*SendMessageRequest); ok {
		var sendingID int32
		if send.Options != nil {
			sendingID = send.Options.SendingId
		}
		log.Printf("tdlib_send_timing stage=request at=%s client_id=%d extra=%s chat_id=%d sending_id=%d",
			time.Now().UTC().Format(time.RFC3339Nano), client.jsonClient.id, req.GetExtra(), send.ChatId, sendingID)
	}

	catcher := make(chan *Response)

	client.catchersStore.Store(req.GetExtra(), catcher)

	defer func() {
		client.catchersStore.Delete(req.GetExtra())
		close(catcher)
	}()

	err := client.jsonClient.Send(req)
	if err != nil {
		logSendTiming("native_send_error", client.jsonClient.id, req.GetExtra())
		return nil, err
	}

	fallbackCtx, cancel := context.WithTimeout(context.Background(), client.fallbackTimeout)
	defer cancel()

	select {
	case response := <-catcher:
		logSendTiming("returned", client.jsonClient.id, req.GetExtra())
		return response, nil

	case <-ctx.Done():
		logSendTiming("deadline", client.jsonClient.id, req.GetExtra())
		return nil, ctx.Err()

	case <-fallbackCtx.Done():
		logSendTiming("fallback_deadline", client.jsonClient.id, req.GetExtra())
		return nil, fallbackCtx.Err()
	}
}

func (client *Client) Execute(req Request) (*Response, error) {
	req.SetExtra(client.extraGenerator())
	req.SetType(req.GetFunctionName())

	return client.jsonClient.Execute(req)
}
