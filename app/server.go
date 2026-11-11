package main

import (
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

// handleConnection reads a request from conn and answers it.
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

	// Every request is treated as an ApiVersions request for now.
	body := handleApiVersions(header)

	if _, err := conn.Write(encodeResponse(header.correlationID, body)); err != nil {
		fmt.Println("Error writing response: ", err.Error())
	}
}
