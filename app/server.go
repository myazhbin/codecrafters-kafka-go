package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

// serve accepts connections on l and handles them until the listener fails or
// is closed.
func serve(l net.Listener) error {
	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}
		handleConnection(conn)
	}
}

// handleConnection reads a request from conn and answers it with a response
// that echoes the correlation ID of the request.
func handleConnection(conn net.Conn) {
	defer conn.Close()

	message, err := readMessage(conn)
	if err != nil {
		// A client that disconnects without sending anything is not an error.
		if !errors.Is(err, io.EOF) {
			fmt.Println("Error reading request: ", err.Error())
		}
		return
	}

	header, err := decodeRequestHeader(newDecoder(message))
	if err != nil {
		fmt.Println("Error parsing request header: ", err.Error())
		return
	}

	if _, err := conn.Write(encodeResponse(header.correlationID)); err != nil {
		fmt.Println("Error writing response: ", err.Error())
	}
}

// encodeResponse builds a Kafka response message: a 4-byte message_size
// followed by a response header v0, which only holds the correlation ID.
// All integers are big-endian.
func encodeResponse(correlationID int32) []byte {
	header := binary.BigEndian.AppendUint32(nil, uint32(correlationID))

	message := binary.BigEndian.AppendUint32(nil, uint32(len(header)))
	return append(message, header...)
}
