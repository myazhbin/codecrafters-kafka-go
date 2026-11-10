package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

// hardcodedCorrelationID is the correlation ID sent back to every client.
const hardcodedCorrelationID int32 = 7

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
// that carries a response header v0.
func handleConnection(conn net.Conn) {
	defer conn.Close()

	// The request isn't parsed yet, but it has to be read before responding:
	// closing a connection that still has unread data makes the kernel reset
	// it, and the client would then lose the response.
	request := make([]byte, 1024)
	if _, err := conn.Read(request); err != nil {
		// A client that disconnects without sending anything is not an error.
		if !errors.Is(err, io.EOF) {
			fmt.Println("Error reading request: ", err.Error())
		}
		return
	}

	if _, err := conn.Write(encodeResponse(hardcodedCorrelationID)); err != nil {
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
