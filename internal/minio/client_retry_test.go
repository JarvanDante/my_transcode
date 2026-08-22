package minio

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestIsTransientPutErr(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{context.Canceled, false},
		{fmt.Errorf("wrap: %w", context.DeadlineExceeded), false},
		{errors.New("put my-media/a.ts: You did not provide the number of bytes specified by the Content-Length HTTP header."), true},
		{errors.New("connection reset by peer"), true},
		{errors.New("read tcp: unexpected EOF"), true},
		{errors.New("Access Denied"), false},
		{errors.New("The specified key does not exist."), false},
	}
	for _, c := range cases {
		if got := isTransientPutErr(c.err); got != c.want {
			t.Fatalf("isTransientPutErr(%v)=%v want %v", c.err, got, c.want)
		}
	}
}
