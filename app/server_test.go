package main

import (
	"errors"
	"fmt"
	"math"
	"os"
	"testing"
	"time"
)

// Stage nv3: a request with the correlation ID 7 is answered with a response
// that starts with a 4-byte message_size and a 4-byte correlation_id of 7.
func TestSendCorrelationID(t *testing.T) {
	addr := startTestServer(t)
	conn := dial(t, addr)

	send(t, conn, apiVersionsRequest(7))

	if got := readResponse(t, conn).correlationID; got != 7 {
		t.Errorf("correlation_id = %d, want 7", got)
	}

	expectNoMoreData(t, conn)
}

// Stage wa6: the broker echoes back the correlation ID found in the request
// header (v2).
func TestParseCorrelationID(t *testing.T) {
	t.Run("example from the stage instructions", func(t *testing.T) {
		addr := startTestServer(t)
		conn := dial(t, addr)

		send(t, conn, mustDecodeHex(t, exampleRequestHex))

		if got := readResponse(t, conn).correlationID; got != 1870644833 {
			t.Errorf("correlation_id = %d, want 1870644833", got)
		}

		expectNoMoreData(t, conn)
	})

	correlationIDs := []int32{0, 1, 7, 311908132, 1870644833, math.MaxInt32, -1, math.MinInt32}

	for _, correlationID := range correlationIDs {
		t.Run(fmt.Sprintf("correlation ID %d", correlationID), func(t *testing.T) {
			addr := startTestServer(t)
			conn := dial(t, addr)

			send(t, conn, apiVersionsRequest(correlationID))

			if got := readResponse(t, conn).correlationID; got != correlationID {
				t.Errorf("correlation_id = %d, want %d", got, correlationID)
			}

			expectNoMoreData(t, conn)
		})
	}
}

// The message_size of a response is the size of the header and body that
// follow it.
func TestResponseMessageSize(t *testing.T) {
	addr := startTestServer(t)
	conn := dial(t, addr)

	send(t, conn, apiVersionsRequest(7))

	response := readResponse(t, conn)
	if want := int32(4 + len(response.body)); response.messageSize != want {
		t.Errorf("message_size = %d, want %d", response.messageSize, want)
	}

	// readResponse consumed exactly message_size bytes; nothing may follow.
	expectNoMoreData(t, conn)
}

// TCP is a byte stream: a request may arrive in several pieces, and the broker
// must wait for all of it before answering.
func TestRequestSplitAcrossWrites(t *testing.T) {
	addr := startTestServer(t)
	conn := dial(t, addr)

	request := apiVersionsRequest(1234567890)

	// Cut inside message_size, inside the correlation ID and inside the body.
	for _, piece := range [][]byte{request[:2], request[2:10], request[10:20], request[20:]} {
		send(t, conn, piece)
		time.Sleep(20 * time.Millisecond)
	}

	if got := readResponse(t, conn).correlationID; got != 1234567890 {
		t.Errorf("correlation_id = %d, want 1234567890", got)
	}
}

// A request that can't be parsed gets no response: the broker just closes the
// connection, like Kafka does.
func TestMalformedRequestClosesConnection(t *testing.T) {
	tests := []struct {
		name    string
		request string
	}{
		{name: "header shorter than a request header v2", request: "00000004 00120004"},
		{name: "client_id longer than the message", request: "0000000c 0012 0004 00000007 0064 6b61"},
		{name: "negative message_size", request: "ffffffff 0012 0004 00000007 ffff 00"},
		{name: "oversized message_size", request: "7fffffff 0012 0004 00000007 ffff 00"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr := startTestServer(t)
			conn := dial(t, addr)

			send(t, conn, mustDecodeHex(t, tt.request))

			// The connection must be closed without a single byte being sent.
			// Depending on timing that shows up as EOF or as a reset.
			response := make([]byte, 64)
			n, err := conn.Read(response)
			if n > 0 {
				t.Fatalf("expected no response, got % x", response[:n])
			}
			if err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("expected the connection to be closed, got error: %v", err)
			}
		})
	}
}

// The broker keeps accepting clients after it has served one.
func TestServeAcceptsConsecutiveClients(t *testing.T) {
	addr := startTestServer(t)

	for i := int32(0); i < 3; i++ {
		conn := dial(t, addr)

		send(t, conn, apiVersionsRequest(100+i))

		if got := readResponse(t, conn).correlationID; got != 100+i {
			t.Errorf("client %d: correlation_id = %d, want %d", i, got, 100+i)
		}

		conn.Close()
	}
}
