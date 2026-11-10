package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"
)

// testTimeout bounds every network operation in the tests so that a broken
// server makes them fail instead of hang.
const testTimeout = 2 * time.Second

// startTestServer runs the broker on an ephemeral local port and returns the
// address it listens on. The listener is closed when the test finishes.
func startTestServer(t *testing.T) string {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	t.Cleanup(func() { l.Close() })

	go serve(l)

	return l.Addr().String()
}

// dial connects to the broker at addr. The connection is closed when the test
// finishes.
func dial(t *testing.T, addr string) net.Conn {
	t.Helper()

	conn, err := net.DialTimeout("tcp", addr, testTimeout)
	if err != nil {
		t.Fatalf("failed to connect to %s: %v", addr, err)
	}
	t.Cleanup(func() { conn.Close() })

	if err := conn.SetDeadline(time.Now().Add(testTimeout)); err != nil {
		t.Fatalf("failed to set deadline: %v", err)
	}

	return conn
}

// apiVersionsRequest builds the ApiVersions (v4) request that the CodeCrafters
// tester sends: message_size, request header v2 and the request body.
func apiVersionsRequest(correlationID int32) []byte {
	var message []byte

	// Request header v2
	message = binary.BigEndian.AppendUint16(message, 18) // request_api_key: ApiVersions
	message = binary.BigEndian.AppendUint16(message, 4)  // request_api_version
	message = binary.BigEndian.AppendUint32(message, uint32(correlationID))
	clientID := "kafka-cli"
	message = binary.BigEndian.AppendUint16(message, uint16(len(clientID))) // client_id length
	message = append(message, clientID...)
	message = append(message, 0) // tag buffer

	// ApiVersions request body v4
	message = appendCompactString(message, "kafka-cli") // client_software_name
	message = appendCompactString(message, "0.1")       // client_software_version
	message = append(message, 0)                        // tag buffer

	return append(binary.BigEndian.AppendUint32(nil, uint32(len(message))), message...)
}

// appendCompactString appends s as a Kafka COMPACT_STRING: the length + 1 as
// an unsigned varint, followed by the bytes of the string.
func appendCompactString(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)+1))
	return append(b, s...)
}

// send writes a whole request to conn.
func send(t *testing.T, conn net.Conn, request []byte) {
	t.Helper()

	if _, err := conn.Write(request); err != nil {
		t.Fatalf("failed to send request: %v", err)
	}
}

// readResponse reads one response from conn and returns its message_size
// along with the header and body bytes that follow it.
func readResponse(t *testing.T, conn net.Conn) (int32, []byte) {
	t.Helper()

	sizeBytes := make([]byte, 4)
	if _, err := io.ReadFull(conn, sizeBytes); err != nil {
		t.Fatalf("failed to read message_size: %v", err)
	}
	messageSize := int32(binary.BigEndian.Uint32(sizeBytes))
	if messageSize < 0 {
		t.Fatalf("message_size is negative: %d", messageSize)
	}

	payload := make([]byte, messageSize)
	if _, err := io.ReadFull(conn, payload); err != nil {
		t.Fatalf("failed to read the %d bytes announced by message_size: %v", messageSize, err)
	}

	return messageSize, payload
}

// expectNoMoreData fails the test if the broker sends anything else on conn.
// The broker may either close the connection or keep it open.
func expectNoMoreData(t *testing.T, conn net.Conn) {
	t.Helper()

	if err := conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatalf("failed to set read deadline: %v", err)
	}

	extra := make([]byte, 64)
	n, err := conn.Read(extra)
	if n > 0 {
		t.Fatalf("expected no more data, got %d extra bytes: % x", n, extra[:n])
	}
	if !errors.Is(err, io.EOF) && !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("unexpected error while checking for extra data: %v", err)
	}
}

func TestEncodeResponse(t *testing.T) {
	tests := []struct {
		name          string
		correlationID int32
		want          []byte
	}{
		{
			name:          "hard-coded correlation ID",
			correlationID: 7,
			want:          []byte{0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x07},
		},
		{
			name:          "big-endian byte order",
			correlationID: 0x01020304,
			want:          []byte{0x00, 0x00, 0x00, 0x04, 0x01, 0x02, 0x03, 0x04},
		},
		{
			name:          "negative correlation ID",
			correlationID: -1,
			want:          []byte{0x00, 0x00, 0x00, 0x04, 0xff, 0xff, 0xff, 0xff},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := encodeResponse(tt.correlationID)
			if !bytes.Equal(got, tt.want) {
				t.Errorf("encodeResponse(%d) = % x, want % x", tt.correlationID, got, tt.want)
			}
		})
	}
}

// Stage nv3: the broker answers a request with a response that is 8 bytes
// long: a 4-byte message_size and a 4-byte correlation_id of 7.
func TestSendCorrelationID(t *testing.T) {
	addr := startTestServer(t)
	conn := dial(t, addr)

	send(t, conn, apiVersionsRequest(7))

	messageSize, payload := readResponse(t, conn)
	if len(payload) != 4 {
		t.Fatalf("expected a 4-byte response header, got %d bytes: % x", len(payload), payload)
	}
	if total := 4 + int(messageSize); total != 8 {
		t.Errorf("expected the response to be 8 bytes long, got %d", total)
	}
	if got := int32(binary.BigEndian.Uint32(payload)); got != 7 {
		t.Errorf("correlation_id = %d, want 7", got)
	}

	expectNoMoreData(t, conn)
}

// Stage nv3: the request isn't parsed, so even the placeholder request from
// the stage instructions (`echo -n "Placeholder request" | nc localhost 9092`)
// gets the 8-byte response, after which the broker closes the connection.
func TestSendCorrelationIDForPlaceholderRequest(t *testing.T) {
	addr := startTestServer(t)
	conn := dial(t, addr)

	send(t, conn, []byte("Placeholder request"))

	response, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}
	if len(response) != 8 {
		t.Fatalf("expected an 8-byte response, got %d bytes: % x", len(response), response)
	}
	if got := int32(binary.BigEndian.Uint32(response[4:])); got != 7 {
		t.Errorf("correlation_id = %d, want 7", got)
	}
}

// The broker keeps accepting clients after it has served one.
func TestServeAcceptsConsecutiveClients(t *testing.T) {
	addr := startTestServer(t)

	for i := 0; i < 3; i++ {
		conn := dial(t, addr)

		send(t, conn, apiVersionsRequest(7))

		_, payload := readResponse(t, conn)
		if len(payload) != 4 {
			t.Fatalf("client %d: expected a 4-byte response header, got %d bytes: % x", i, len(payload), payload)
		}
		if got := int32(binary.BigEndian.Uint32(payload)); got != 7 {
			t.Errorf("client %d: correlation_id = %d, want 7", i, got)
		}

		conn.Close()
	}
}
