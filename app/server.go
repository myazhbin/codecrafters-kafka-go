package main

import (
	"errors"
	"fmt"
	"io"
	"net"
)

// serve accepts connections on l until the listener fails or is closed. Every
// client is served by its own goroutine, so clients don't have to wait for
// each other.
func serve(l net.Listener) error {
	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}
		go handleConnection(conn)
	}
}

// handleConnection answers the requests that a client sends over conn, one
// after the other, until the client disconnects.
func handleConnection(conn net.Conn) {
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

		header, err := decodeRequestHeader(newDecoder(message))
		if err != nil {
			fmt.Println("Error parsing request header: ", err.Error())
			return
		}

		// Every request is treated as an ApiVersions request for now.
		body := handleApiVersions(header)

		if _, err := conn.Write(encodeResponse(header.correlationID, body)); err != nil {
			fmt.Println("Error writing response: ", err.Error())
			return
		}
	}
}
