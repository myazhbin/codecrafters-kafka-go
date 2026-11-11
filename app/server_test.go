package main

import (
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"sync"
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

// expectApiVersionsResponse reads one response from conn and checks it the way
// the CodeCrafters tester checks the response to a valid ApiVersions request.
func expectApiVersionsResponse(t *testing.T, conn net.Conn, correlationID int32) {
	t.Helper()

	response := readResponse(t, conn)

	// The message length is valid.
	if want := int32(4 + len(response.body)); response.messageSize != want {
		t.Errorf("message_size = %d, want %d", response.messageSize, want)
	}

	// The correlation ID matches the one in the request header.
	if response.correlationID != correlationID {
		t.Errorf("correlation_id = %d, want %d", response.correlationID, correlationID)
	}

	// The error code is 0 (No Error), and no bytes remain after the body
	// (checked by decodeApiVersionsResponse).
	body := decodeApiVersionsResponse(t, response.body)
	if body.errorCode != 0 {
		t.Errorf("error_code = %d, want 0", body.errorCode)
	}

	// The body has an entry for the API key 18 (ApiVersions) whose
	// MaxVersion is at least 4.
	if api := findAPI(t, body.apiKeys, 18); api.maxVersion < 4 {
		t.Errorf("max_version for ApiVersions = %d, want at least 4", api.maxVersion)
	}
}

// Stage nh4: the broker answers several requests sent one after the other over
// the same connection.
func TestSerialRequests(t *testing.T) {
	addr := startTestServer(t)
	conn := dial(t, addr)

	for _, correlationID := range []int32{1001, 7, 1870644833, -5, 1002} {
		send(t, conn, apiVersionsRequest(correlationID))
		expectApiVersionsResponse(t, conn, correlationID)
	}

	expectNoMoreData(t, conn)
}

// The broker waits for the next request for as long as the client keeps the
// connection open.
func TestSerialRequestsWithPauses(t *testing.T) {
	addr := startTestServer(t)
	conn := dial(t, addr)

	for correlationID := int32(1); correlationID <= 3; correlationID++ {
		send(t, conn, apiVersionsRequest(correlationID))
		expectApiVersionsResponse(t, conn, correlationID)

		time.Sleep(50 * time.Millisecond)
	}
}

// Requests may be sent without waiting for the previous response: the broker
// must answer all of them, in the order they were sent.
func TestPipelinedRequests(t *testing.T) {
	addr := startTestServer(t)
	conn := dial(t, addr)

	correlationIDs := []int32{11, 22, 33, 44}

	var requests []byte
	for _, correlationID := range correlationIDs {
		requests = append(requests, apiVersionsRequest(correlationID)...)
	}
	send(t, conn, requests)

	for _, correlationID := range correlationIDs {
		expectApiVersionsResponse(t, conn, correlationID)
	}

	expectNoMoreData(t, conn)
}

// A request for an unsupported version gets an error response, after which
// the connection can still be used.
func TestSerialRequestsAfterUnsupportedVersion(t *testing.T) {
	addr := startTestServer(t)
	conn := dial(t, addr)

	send(t, conn, apiVersionsRequestWithVersion(99, 1))

	response := readResponse(t, conn)
	if response.correlationID != 1 {
		t.Errorf("correlation_id = %d, want 1", response.correlationID)
	}
	if got := decodeApiVersionsResponse(t, response.body).errorCode; got != 35 {
		t.Errorf("error_code = %d, want 35", got)
	}

	send(t, conn, apiVersionsRequest(2))
	expectApiVersionsResponse(t, conn, 2)
}

// Once a client has disconnected, the broker serves the next one.
func TestServeAcceptsClientAfterSerialRequests(t *testing.T) {
	addr := startTestServer(t)

	for client := int32(0); client < 3; client++ {
		conn := dial(t, addr)

		for request := int32(0); request < 3; request++ {
			correlationID := client*10 + request

			send(t, conn, apiVersionsRequest(correlationID))
			expectApiVersionsResponse(t, conn, correlationID)
		}

		conn.Close()
	}
}

// Stage sk0: several clients are connected at the same time, and each of them
// sends multiple requests. The requests of the clients are interleaved, so a
// broker that serves one client at a time can't answer them.
func TestConcurrentClients(t *testing.T) {
	addr := startTestServer(t)

	conns := []net.Conn{dial(t, addr), dial(t, addr), dial(t, addr)}

	for round := int32(0); round < 3; round++ {
		for client, conn := range conns {
			correlationID := int32(client)*100 + round

			send(t, conn, apiVersionsRequest(correlationID))
			expectApiVersionsResponse(t, conn, correlationID)
		}
	}

	// The clients are also served when they take turns in the reverse order.
	for client := len(conns) - 1; client >= 0; client-- {
		correlationID := int32(client)*100 + 99

		send(t, conns[client], apiVersionsRequest(correlationID))
		expectApiVersionsResponse(t, conns[client], correlationID)
	}
}

// All the responses to requests that were sent before any response was read
// arrive on the connection that the request was sent on.
func TestConcurrentClientsReadAfterAllRequestsAreSent(t *testing.T) {
	addr := startTestServer(t)

	conns := []net.Conn{dial(t, addr), dial(t, addr), dial(t, addr)}

	for client, conn := range conns {
		send(t, conn, apiVersionsRequest(int32(client)+1))
	}
	for client, conn := range conns {
		expectApiVersionsResponse(t, conn, int32(client)+1)
		expectNoMoreData(t, conn)
	}
}

// A client that is connected but silent doesn't keep others from being served.
func TestIdleClientDoesNotBlockOthers(t *testing.T) {
	addr := startTestServer(t)

	idle := dial(t, addr)

	active := dial(t, addr)
	send(t, active, apiVersionsRequest(1))
	expectApiVersionsResponse(t, active, 1)

	// The idle client is still connected and gets served once it speaks up.
	send(t, idle, apiVersionsRequest(2))
	expectApiVersionsResponse(t, idle, 2)
}

// Many clients send requests at the very same time. Run with -race, this also
// checks that serving them concurrently is free of data races.
func TestConcurrentClientsInParallel(t *testing.T) {
	const (
		clients           = 10
		requestsPerClient = 20
	)

	addr := startTestServer(t)

	// Clients only start sending once all of them are connected.
	var connected sync.WaitGroup
	connected.Add(clients)
	allConnected := make(chan struct{})
	go func() {
		connected.Wait()
		close(allConnected)
	}()

	// The group only returns once all its parallel subtests are done, which
	// keeps the broker running until then.
	t.Run("group", func(t *testing.T) {
		for client := int32(0); client < clients; client++ {
			t.Run(fmt.Sprintf("client %d", client), func(t *testing.T) {
				t.Parallel()

				conn := dial(t, addr)
				connected.Done()

				select {
				case <-allConnected:
				case <-time.After(testTimeout):
					t.Fatal("timed out waiting for the other clients to connect")
				}

				for request := int32(0); request < requestsPerClient; request++ {
					correlationID := client*1000 + request

					send(t, conn, apiVersionsRequest(correlationID))
					expectApiVersionsResponse(t, conn, correlationID)
				}
			})
		}
	})
}
