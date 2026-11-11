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

// apiVersionsResponse is a decoded ApiVersions response body (v4).
type apiVersionsResponse struct {
	errorCode      int16
	apiKeys        []apiVersionRange
	throttleTimeMs int32
}

// decodeApiVersionsResponse decodes an ApiVersions response body (v4) and
// fails the test if the body is malformed or followed by extra bytes.
func decodeApiVersionsResponse(t *testing.T, body []byte) apiVersionsResponse {
	t.Helper()

	d := newDecoder(body)

	var response apiVersionsResponse
	response.errorCode = d.int16()

	arrayLength := d.uvarint() // COMPACT_ARRAY: number of elements + 1
	if d.err == nil && arrayLength == 0 {
		t.Fatalf("api_keys is a null array")
	}
	for i := uint32(1); i < arrayLength && d.err == nil; i++ {
		response.apiKeys = append(response.apiKeys, apiVersionRange{
			apiKey:     d.int16(),
			minVersion: d.int16(),
			maxVersion: d.int16(),
		})
		d.taggedFields()
	}

	response.throttleTimeMs = d.int32()
	d.taggedFields()

	if d.err != nil {
		t.Fatalf("failed to decode ApiVersions response body % x: %v", body, d.err)
	}
	if len(d.buf) != 0 {
		t.Fatalf("%d extra bytes after the ApiVersions response body: % x", len(d.buf), d.buf)
	}

	return response
}

// findAPI returns the entry for apiKey in an api_keys array.
func findAPI(t *testing.T, apiKeys []apiVersionRange, apiKey int16) apiVersionRange {
	t.Helper()

	var found []apiVersionRange
	for _, api := range apiKeys {
		if api.apiKey == apiKey {
			found = append(found, api)
		}
	}
	if len(found) != 1 {
		t.Fatalf("expected exactly one entry for API key %d in %+v, found %d", apiKey, apiKeys, len(found))
	}
	return found[0]
}

func TestEncodeApiVersionsResponse(t *testing.T) {
	tests := []struct {
		name      string
		errorCode int16
		apis      []apiVersionRange
		want      string
	}{
		{
			// The response body from the "Handle ApiVersions requests" stage
			// instructions.
			name:      "single API",
			errorCode: 0,
			apis:      []apiVersionRange{{apiKey: 18, minVersion: 0, maxVersion: 4}},
			want: "0000" + // error_code:        0 (no error)
				"02" + // api_keys array length: 1 element
				"0012" + // api_key:           18 (ApiVersions)
				"0000" + // min_version:       0
				"0004" + // max_version:       4
				"00" + // TAG_BUFFER:        empty
				"00000000" + // throttle_time_ms:  0
				"00", // TAG_BUFFER:        empty
		},
		{
			// The response body from the "Include DescribeTopicPartitions in
			// ApiVersions" stage instructions.
			name:      "ApiVersions and DescribeTopicPartitions",
			errorCode: 0,
			apis: []apiVersionRange{
				{apiKey: 18, minVersion: 0, maxVersion: 4},
				{apiKey: 75, minVersion: 0, maxVersion: 0},
			},
			want: "0000" + // error_code:        0 (no error)
				"03" + // api_keys array:    2 elements (compact array encoding)
				"0012" + // api_key:           18 (ApiVersions)
				"0000" + // min_version:       0
				"0004" + // max_version:       4
				"00" + // TAG_BUFFER:        empty
				"004b" + // api_key:           75 (DescribeTopicPartitions)
				"0000" + // min_version:       0
				"0000" + // max_version:       0
				"00" + // TAG_BUFFER:        empty
				"00000000" + // throttle_time_ms:  0
				"00", // TAG_BUFFER:        empty
		},
		{
			name:      "several APIs",
			errorCode: 0,
			apis: []apiVersionRange{
				{apiKey: 18, minVersion: 0, maxVersion: 4},
				{apiKey: 75, minVersion: 0, maxVersion: 0},
				{apiKey: 1, minVersion: 4, maxVersion: 16},
			},
			want: "0000" +
				"04" +
				"0012 0000 0004 00" +
				"004b 0000 0000 00" +
				"0001 0004 0010 00" +
				"00000000" +
				"00",
		},
		{
			name:      "no APIs",
			errorCode: 0,
			apis:      nil,
			want:      "0000 01 00000000 00",
		},
		{
			name:      "error code",
			errorCode: 35,
			apis:      []apiVersionRange{{apiKey: 18, minVersion: 0, maxVersion: 4}},
			want:      "0023 02 0012 0000 0004 00 00000000 00",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := encodeApiVersionsResponse(tt.errorCode, tt.apis)
			if want := mustDecodeHex(t, tt.want); !bytes.Equal(got, want) {
				t.Errorf("body = % x, want % x", got, want)
			}

			// The body must also survive a round trip through the decoder.
			decoded := decodeApiVersionsResponse(t, got)
			if decoded.errorCode != tt.errorCode {
				t.Errorf("decoded error_code = %d, want %d", decoded.errorCode, tt.errorCode)
			}
			if len(decoded.apiKeys) != len(tt.apis) {
				t.Fatalf("decoded %d api_keys, want %d", len(decoded.apiKeys), len(tt.apis))
			}
			for i, api := range tt.apis {
				if decoded.apiKeys[i] != api {
					t.Errorf("decoded api_keys[%d] = %+v, want %+v", i, decoded.apiKeys[i], api)
				}
			}
		})
	}
}

func TestSupportedAPIs(t *testing.T) {
	seen := map[int16]bool{}
	for _, api := range supportedAPIs {
		if seen[api.apiKey] {
			t.Errorf("API key %d is listed more than once", api.apiKey)
		}
		seen[api.apiKey] = true

		if api.minVersion < 0 || api.minVersion > api.maxVersion {
			t.Errorf("API key %d has an invalid version range: %d to %d", api.apiKey, api.minVersion, api.maxVersion)
		}
	}

	// The broker supports versions 0 to 4 of ApiVersions.
	if got, want := findAPI(t, supportedAPIs, 18), (apiVersionRange{apiKey: 18, minVersion: 0, maxVersion: 4}); got != want {
		t.Errorf("ApiVersions entry = %+v, want %+v", got, want)
	}

	// The broker supports version 0 of DescribeTopicPartitions.
	if got, want := findAPI(t, supportedAPIs, 75), (apiVersionRange{apiKey: 75, minVersion: 0, maxVersion: 0}); got != want {
		t.Errorf("DescribeTopicPartitions entry = %+v, want %+v", got, want)
	}
}

func TestSupportsVersion(t *testing.T) {
	tests := []struct {
		apiKey     int16
		apiVersion int16
		want       bool
	}{
		{apiKey: 18, apiVersion: -1, want: false},
		{apiKey: 18, apiVersion: 0, want: true},
		{apiKey: 18, apiVersion: 4, want: true},
		{apiKey: 18, apiVersion: 5, want: false},

		{apiKey: 75, apiVersion: -1, want: false},
		{apiKey: 75, apiVersion: 0, want: true},
		{apiKey: 75, apiVersion: 1, want: false},

		// An API that the broker doesn't know supports no version at all.
		{apiKey: 9999, apiVersion: 0, want: false},
		{apiKey: -1, apiVersion: 0, want: false},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("API %d version %d", tt.apiKey, tt.apiVersion), func(t *testing.T) {
			if got := supportsVersion(tt.apiKey, tt.apiVersion); got != tt.want {
				t.Errorf("supportsVersion(%d, %d) = %v, want %v", tt.apiKey, tt.apiVersion, got, tt.want)
			}
		})
	}
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

			if got := decodeApiVersionsResponse(t, body).errorCode; got != tt.want {
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

			send(t, conn, newApiVersionsRequestWithVersion(tt.apiVersion, 42))

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

// Stage pv1: a valid ApiVersions (v4) request is answered with a complete
// ApiVersions response body.
func TestHandleApiVersionsRequest(t *testing.T) {
	addr := startTestServer(t)
	conn := dial(t, addr)

	// The request from the stage instructions (correlation ID 0x67890abc).
	send(t, conn, newApiVersionsRequest(1737034428))

	response := readResponse(t, conn)

	// The message_size field correctly represents the size of the header and
	// body.
	if want := int32(4 + len(response.body)); response.messageSize != want {
		t.Errorf("message_size = %d, want %d", response.messageSize, want)
	}

	// The correlation ID in the response header matches the one in the
	// request header.
	if response.correlationID != 1737034428 {
		t.Errorf("correlation_id = %d, want 1737034428", response.correlationID)
	}

	// No extra bytes remain after decoding all response fields (checked by
	// decodeApiVersionsResponse), and the error code is 0.
	body := decodeApiVersionsResponse(t, response.body)
	if body.errorCode != 0 {
		t.Errorf("error_code = %d, want 0", body.errorCode)
	}
	if body.throttleTimeMs != 0 {
		t.Errorf("throttle_time_ms = %d, want 0", body.throttleTimeMs)
	}

	// The response includes an entry for API key 18 (ApiVersions), where the
	// min_version is 0 and the max_version is 4.
	if got, want := findAPI(t, body.apiKeys, 18), (apiVersionRange{apiKey: 18, minVersion: 0, maxVersion: 4}); got != want {
		t.Errorf("ApiVersions entry = %+v, want %+v", got, want)
	}

	expectNoMoreData(t, conn)
}

// Stage yk1: the ApiVersions response advertises the DescribeTopicPartitions
// API next to the ApiVersions API.
func TestApiVersionsIncludesDescribeTopicPartitions(t *testing.T) {
	addr := startTestServer(t)
	conn := dial(t, addr)

	send(t, conn, newApiVersionsRequest(1737034428))

	response := readResponse(t, conn)
	if want := int32(4 + len(response.body)); response.messageSize != want {
		t.Errorf("message_size = %d, want %d", response.messageSize, want)
	}
	if response.correlationID != 1737034428 {
		t.Errorf("correlation_id = %d, want 1737034428", response.correlationID)
	}

	body := decodeApiVersionsResponse(t, response.body)
	if body.errorCode != 0 {
		t.Errorf("error_code = %d, want 0", body.errorCode)
	}

	// API key 18 (ApiVersions): min_version 0, max_version at least 4.
	if api := findAPI(t, body.apiKeys, 18); api.minVersion != 0 || api.maxVersion < 4 {
		t.Errorf("ApiVersions entry = %+v, want min_version 0 and max_version >= 4", api)
	}

	// API key 75 (DescribeTopicPartitions): min_version 0, max_version at
	// least 0.
	if api := findAPI(t, body.apiKeys, 75); api.minVersion != 0 || api.maxVersion < 0 {
		t.Errorf("DescribeTopicPartitions entry = %+v, want min_version 0 and max_version >= 0", api)
	}

	expectNoMoreData(t, conn)
}
