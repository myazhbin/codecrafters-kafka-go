package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// The broker is started like Kafka, with the path of a properties file as
	// its argument: ./your_program.sh /tmp/server.properties
	cfg := defaultConfig()
	if len(os.Args) > 1 {
		loaded, err := loadConfig(os.Args[1])
		if err != nil {
			fmt.Println("Error reading configuration, using the defaults: ", err.Error())
		} else {
			cfg = loaded
		}
	}

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	defer l.Close()

	b := &broker{logDir: cfg.logDir}
	if err := b.serve(l); err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
}
