package main

// Error codes of the Kafka protocol.
const (
	errorCodeNone               int16 = 0
	errorCodeUnsupportedVersion int16 = 35
)

// The versions of the ApiVersions API that the broker supports.
const (
	apiVersionsMinVersion int16 = 0
	apiVersionsMaxVersion int16 = 4
)

// handleApiVersions builds the body of the response to an ApiVersions request.
//
// The body starts with an error code, which is UNSUPPORTED_VERSION when the
// client asked for a version of ApiVersions that the broker doesn't support.
func handleApiVersions(header requestHeader) []byte {
	errorCode := errorCodeNone
	if header.apiVersion < apiVersionsMinVersion || header.apiVersion > apiVersionsMaxVersion {
		errorCode = errorCodeUnsupportedVersion
	}

	var e encoder
	e.int16(errorCode)

	return e.buf
}
