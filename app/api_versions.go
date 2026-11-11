package main

// API keys of the Kafka protocol.
const (
	apiKeyFetch                   int16 = 1
	apiKeyApiVersions             int16 = 18
	apiKeyDescribeTopicPartitions int16 = 75
)

// Error codes of the Kafka protocol.
const (
	errorCodeNone                    int16 = 0
	errorCodeUnknownTopicOrPartition int16 = 3
	errorCodeUnsupportedVersion      int16 = 35
	errorCodeUnknownTopicID          int16 = 100
)

// apiVersionRange is the range of versions of an API that the broker supports.
type apiVersionRange struct {
	apiKey     int16
	minVersion int16
	maxVersion int16
}

// supportedAPIs lists the APIs that the broker supports. It is what the broker
// advertises in its ApiVersions responses.
var supportedAPIs = []apiVersionRange{
	{apiKey: apiKeyFetch, minVersion: 16, maxVersion: 16},
	{apiKey: apiKeyApiVersions, minVersion: 0, maxVersion: 4},
	{apiKey: apiKeyDescribeTopicPartitions, minVersion: 0, maxVersion: 0},
}

// supportsVersion reports whether the broker supports the given version of an
// API.
func supportsVersion(apiKey, apiVersion int16) bool {
	for _, api := range supportedAPIs {
		if api.apiKey == apiKey {
			return api.minVersion <= apiVersion && apiVersion <= api.maxVersion
		}
	}
	return false
}

// handleApiVersions builds the body of the response to an ApiVersions request,
// which tells the client the APIs and versions that the broker supports.
//
// The error code is UNSUPPORTED_VERSION when the client asked for a version of
// ApiVersions that the broker doesn't support.
func handleApiVersions(header requestHeader) []byte {
	errorCode := errorCodeNone
	if !supportsVersion(apiKeyApiVersions, header.apiVersion) {
		errorCode = errorCodeUnsupportedVersion
	}

	return encodeApiVersionsResponse(errorCode, supportedAPIs)
}

// encodeApiVersionsResponse encodes the body of an ApiVersions response v4.
func encodeApiVersionsResponse(errorCode int16, apis []apiVersionRange) []byte {
	var e encoder
	e.int16(errorCode)

	e.compactArrayLength(len(apis)) // api_keys
	for _, api := range apis {
		e.int16(api.apiKey)
		e.int16(api.minVersion)
		e.int16(api.maxVersion)
		e.emptyTaggedFields()
	}

	e.int32(0) // throttle_time_ms
	e.emptyTaggedFields()

	return e.buf
}
