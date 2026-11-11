package main

import (
	"errors"
	"fmt"
	"io"
	"net"
)

// broker is a Kafka broker.
//
// It keeps no state in memory: what it knows about the cluster is read from
// the files in its log directory whenever a request needs it.
type broker struct {
	// logDir is the directory that holds the logs of the partitions,
	// including the cluster metadata log.
	logDir string
}

// serve accepts connections on l until the listener fails or is closed. Every
// client is served by its own goroutine, so clients don't have to wait for
// each other.
func (b *broker) serve(l net.Listener) error {
	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}
		go b.handleConnection(conn)
	}
}

// handleConnection answers the requests that a client sends over conn, one
// after the other, until the client disconnects.
//
// Like Kafka, the broker closes the connection when it gets a request that it
// can't parse or that it doesn't support.
func (b *broker) handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		message, err := readMessage(conn)
		if err != nil {
			// A client that disconnects between two requests is not an error.
			if !errors.Is(err, io.EOF) {
				fmt.Println("Error reading request: ", err.Error())
			}
			return
		}

		response, err := b.handleRequest(message)
		if err != nil {
			fmt.Println("Error handling request: ", err.Error())
			return
		}

		if _, err := conn.Write(response); err != nil {
			fmt.Println("Error writing response: ", err.Error())
			return
		}
	}
}

// handleRequest builds the response message for a request message (the header
// and body of a request, without the message_size that precedes them).
func (b *broker) handleRequest(message []byte) ([]byte, error) {
	d := newDecoder(message)

	header, err := decodeRequestHeader(d)
	if err != nil {
		return nil, fmt.Errorf("parsing request header: %w", err)
	}

	// ApiVersions is how clients find out what the broker supports, so it is
	// the one API that answers requests for versions it doesn't support. Its
	// responses always use response header v0, whatever the request version.
	if header.apiKey == apiKeyApiVersions {
		return encodeResponse(responseHeaderV0, header.correlationID, handleApiVersions(header)), nil
	}

	if !supportsVersion(header.apiKey, header.apiVersion) {
		return nil, fmt.Errorf("unsupported request: API key %d, version %d", header.apiKey, header.apiVersion)
	}

	switch header.apiKey {
	case apiKeyDescribeTopicPartitions:
		body, err := b.handleDescribeTopicPartitions(d)
		if err != nil {
			return nil, err
		}
		return encodeResponse(responseHeaderV1, header.correlationID, body), nil
	default:
		return nil, fmt.Errorf("unsupported request: API key %d", header.apiKey)
	}
}
