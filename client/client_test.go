package client

import (
	"strings"
	"testing"
)

func TestSendExtraTagsOnlyMessageSends(t *testing.T) {
	tests := []struct {
		name string
		req  Request
		want string
	}{
		{name: "send message", req: &SendMessageRequest{}, want: sendTimingPrefix + "request-id"},
		{name: "other request", req: &GetMeRequest{}, want: "request-id"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := sendExtra(test.req, "request-id")
			if got != test.want {
				t.Errorf("sendExtra() = %q, want %q", got, test.want)
			}
			if strings.Count(got, "request-id") != 1 {
				t.Errorf("sendExtra() changed the unique request identifier: %q", got)
			}
		})
	}
}
