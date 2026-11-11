package main

import (
	"os"
	"strings"
)

// defaultLogDir is where the broker looks for its logs when its configuration
// doesn't name a directory.
const defaultLogDir = "/tmp/kraft-combined-logs"

// config is the configuration of the broker.
type config struct {
	// logDir is the directory that holds the logs of the partitions.
	logDir string
}

// defaultConfig returns the configuration of a broker that is started without
// a properties file.
func defaultConfig() config {
	return config{logDir: defaultLogDir}
}

// loadConfig reads the configuration of the broker from a properties file like
// Kafka's server.properties.
func loadConfig(path string) (config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return config{}, err
	}

	return configFromProperties(parseProperties(string(data))), nil
}

// configFromProperties builds the configuration of the broker out of the
// properties that it understands.
func configFromProperties(properties map[string]string) config {
	cfg := defaultConfig()

	// Like in Kafka, log.dirs is a comma-separated list of directories that
	// takes precedence over log.dir. The broker only uses its first entry.
	for _, key := range []string{"log.dirs", "log.dir"} {
		for _, dir := range strings.Split(properties[key], ",") {
			if dir = strings.TrimSpace(dir); dir != "" {
				cfg.logDir = dir
				return cfg
			}
		}
	}

	return cfg
}

// parseProperties parses the text of a Java properties file: lines of the form
// "key=value" (or "key:value"), where blank lines and lines that start with
// "#" or "!" are ignored. Escape sequences and values that continue on the
// next line are not supported.
func parseProperties(text string) map[string]string {
	properties := make(map[string]string)

	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || line[0] == '#' || line[0] == '!' {
			continue
		}

		separator := strings.IndexAny(line, "=:")
		if separator < 0 {
			properties[line] = ""
			continue
		}

		key := strings.TrimSpace(line[:separator])
		value := strings.TrimSpace(line[separator+1:])
		properties[key] = value
	}

	return properties
}
