package main

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"os"
	"strings"
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

// send writes a whole request to conn.
func send(t *testing.T, conn net.Conn, request []byte) {
	t.Helper()

	if _, err := conn.Write(request); err != nil {
		t.Fatalf("failed to send request: %v", err)
	}
}

// encodeRequest builds a request message the way the CodeCrafters tester
// does: message_size, a request header v2 with the client ID "kafka-cli" and
// the given request body.
func encodeRequest(apiKey, apiVersion int16, correlationID int32, body []byte) []byte {
	const clientID = "kafka-cli"

	var message []byte
	message = binary.BigEndian.AppendUint16(message, uint16(apiKey))
	message = binary.BigEndian.AppendUint16(message, uint16(apiVersion))
	message = binary.BigEndian.AppendUint32(message, uint32(correlationID))
	message = binary.BigEndian.AppendUint16(message, uint16(len(clientID)))
	message = append(message, clientID...)
	message = append(message, 0) // tag buffer
	message = append(message, body...)

	return append(binary.BigEndian.AppendUint32(nil, uint32(len(message))), message...)
}

// newApiVersionsRequest builds the ApiVersions (v4) request that the CodeCrafters
// tester sends.
func newApiVersionsRequest(correlationID int32) []byte {
	return newApiVersionsRequestWithVersion(4, correlationID)
}

// newApiVersionsRequestWithVersion builds an ApiVersions request that claims to
// use the given API version.
func newApiVersionsRequestWithVersion(apiVersion int16, correlationID int32) []byte {
	var body []byte
	body = appendCompactString(body, "kafka-cli") // client_software_name
	body = appendCompactString(body, "0.1")       // client_software_version
	body = append(body, 0)                        // tag buffer

	return encodeRequest(18, apiVersion, correlationID, body)
}

// appendCompactString appends s as a COMPACT_STRING: the length + 1 as an
// unsigned varint, followed by the bytes of the string.
func appendCompactString(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)+1))
	return append(b, s...)
}

// response is a response message split into its parts.
type response struct {
	messageSize   int32
	correlationID int32
	body          []byte // everything after the correlation ID
}

// bodyAfterHeaderV1 returns the response body of a response that uses response
// header v1, in which a TAG_BUFFER follows the correlation ID. The test fails
// if that TAG_BUFFER is missing or not empty.
func (r response) bodyAfterHeaderV1(t *testing.T) []byte {
	t.Helper()

	if len(r.body) == 0 || r.body[0] != 0 {
		t.Fatalf("expected an empty TAG_BUFFER (00) after the correlation ID, got: % x", r.body)
	}
	return r.body[1:]
}

// readResponse reads one response from conn: a 4-byte message_size and the
// header and body it announces.
func readResponse(t *testing.T, conn net.Conn) response {
	t.Helper()

	sizeBytes := make([]byte, 4)
	if _, err := io.ReadFull(conn, sizeBytes); err != nil {
		t.Fatalf("failed to read message_size: %v", err)
	}
	messageSize := int32(binary.BigEndian.Uint32(sizeBytes))
	if messageSize < 4 {
		t.Fatalf("message_size = %d, too small to hold a response header", messageSize)
	}

	message := make([]byte, messageSize)
	if _, err := io.ReadFull(conn, message); err != nil {
		t.Fatalf("failed to read the %d bytes announced by message_size: %v", messageSize, err)
	}

	return response{
		messageSize:   messageSize,
		correlationID: int32(binary.BigEndian.Uint32(message)),
		body:          message[4:],
	}
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

// mustDecodeHex decodes a hex string, ignoring any whitespace in it.
func mustDecodeHex(t *testing.T, s string) []byte {
	t.Helper()

	b, err := hex.DecodeString(strings.Join(strings.Fields(s), ""))
	if err != nil {
		t.Fatalf("invalid hex string %q: %v", s, err)
	}
	return b
}
