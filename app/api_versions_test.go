package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"testing"
)

// unsupportedVersionRequestHex is the ApiVersions request with an unsupported
// version from the "Parse API Version" stage instructions.
const unsupportedVersionRequestHex = "00000023" + // message_size:        35
	"0012" + // request_api_key:     18
	"674a" + // request_api_version: 26442
	"4f74d28b" + // correlation_id:      1333056139
	"00096b61666b612d636c69" + // client_id:           "kafka-cli"
	"00" + // tag buffer
	"0a6b61666b612d636c69" + // client_software_name:    "kafka-cli"
	"04302e31" + // client_software_version: "0.1"
	"00" // tag buffer

// errorCodeOf returns the error_code that an ApiVersions response body starts
// with.
func errorCodeOf(t *testing.T, body []byte) int16 {
	t.Helper()

	if len(body) < 2 {
		t.Fatalf("response body is too short to hold an error_code: % x", body)
	}
	return int16(binary.BigEndian.Uint16(body))
}

func TestHandleApiVersionsErrorCode(t *testing.T) {
	tests := []struct {
		apiVersion int16
		want       int16
	}{
		// The broker supports versions 0 to 4 of ApiVersions.
		{apiVersion: 0, want: 0},
		{apiVersion: 1, want: 0},
		{apiVersion: 2, want: 0},
		{apiVersion: 3, want: 0},
		{apiVersion: 4, want: 0},

		// Everything else gets UNSUPPORTED_VERSION (35).
		{apiVersion: 5, want: 35},
		{apiVersion: 26442, want: 35},
		{apiVersion: math.MaxInt16, want: 35},
		{apiVersion: -1, want: 35},
		{apiVersion: math.MinInt16, want: 35},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("version %d", tt.apiVersion), func(t *testing.T) {
			body := handleApiVersions(requestHeader{apiKey: 18, apiVersion: tt.apiVersion})

			if got := errorCodeOf(t, body); got != tt.want {
				t.Errorf("error_code = %d, want %d", got, tt.want)
			}
		})
	}
}

// Stage nc5: an ApiVersions request with an unsupported version is answered
// with the error code 35 (UNSUPPORTED_VERSION).
func TestParseAPIVersion(t *testing.T) {
	t.Run("example from the stage instructions", func(t *testing.T) {
		addr := startTestServer(t)
		conn := dial(t, addr)

		send(t, conn, mustDecodeHex(t, unsupportedVersionRequestHex))

		response := readResponse(t, conn)
		if response.correlationID != 1333056139 {
			t.Errorf("correlation_id = %d, want 1333056139", response.correlationID)
		}
		if want := []byte{0x00, 0x23}; !bytes.HasPrefix(response.body, want) {
			t.Errorf("response body = % x, want it to start with % x (error_code 35)", response.body, want)
		}

		expectNoMoreData(t, conn)
	})

	tests := []struct {
		apiVersion int16
		want       int16
	}{
		{apiVersion: 0, want: 0},
		{apiVersion: 3, want: 0},
		{apiVersion: 4, want: 0},
		{apiVersion: 5, want: 35},
		{apiVersion: 1000, want: 35},
		{apiVersion: -1, want: 35},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("version %d", tt.apiVersion), func(t *testing.T) {
			addr := startTestServer(t)
			conn := dial(t, addr)

			send(t, conn, apiVersionsRequestWithVersion(tt.apiVersion, 42))

			response := readResponse(t, conn)
			if response.correlationID != 42 {
				t.Errorf("correlation_id = %d, want 42", response.correlationID)
			}
			if got := errorCodeOf(t, response.body); got != tt.want {
				t.Errorf("error_code = %d, want %d", got, tt.want)
			}

			expectNoMoreData(t, conn)
		})
	}
}
