package main

import (
	"path/filepath"
	"reflect"
	"testing"
)

// serverProperties is a server.properties file like the one that the
// CodeCrafters tester passes to the broker.
const serverProperties = `# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.

############################# Server Basics #############################

process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093

############################# Socket Server Settings #############################

listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://localhost:9092
socket.request.max.bytes=104857600

############################# Log Basics #############################

# A comma separated list of directories under which to store log files
log.dirs=/tmp/kraft-combined-logs
num.partitions=1
`

func TestParseProperties(t *testing.T) {
	tests := []struct {
		name string
		text string
		want map[string]string
	}{
		{
			name: "empty file",
			text: "",
			want: map[string]string{},
		},
		{
			name: "server.properties",
			text: serverProperties,
			want: map[string]string{
				"process.roles":            "broker,controller",
				"node.id":                  "1",
				"controller.quorum.voters": "1@localhost:9093",
				"listeners":                "PLAINTEXT://:9092,CONTROLLER://:9093",
				"advertised.listeners":     "PLAINTEXT://localhost:9092",
				"socket.request.max.bytes": "104857600",
				"log.dirs":                 "/tmp/kraft-combined-logs",
				"num.partitions":           "1",
			},
		},
		{
			name: "comments and blank lines",
			text: "# comment\n! another comment\n\n   \n  # indented comment\nkey=value\n",
			want: map[string]string{"key": "value"},
		},
		{
			name: "whitespace around keys and values",
			text: "  key1 = value 1  \n\tkey2\t=\tvalue2\t\n",
			want: map[string]string{"key1": "value 1", "key2": "value2"},
		},
		{
			name: "colon as separator",
			text: "key1: value1\nkey2:value2\n",
			want: map[string]string{"key1": "value1", "key2": "value2"},
		},
		{
			name: "value that contains separators",
			text: "url=http://host:80/path?a=b\n",
			want: map[string]string{"url": "http://host:80/path?a=b"},
		},
		{
			name: "empty value and key without value",
			text: "empty=\nalone\n",
			want: map[string]string{"empty": "", "alone": ""},
		},
		{
			name: "Windows line endings",
			text: "key1=value1\r\nkey2=value2\r\n",
			want: map[string]string{"key1": "value1", "key2": "value2"},
		},
		{
			name: "no trailing newline",
			text: "key=value",
			want: map[string]string{"key": "value"},
		},
		{
			name: "later value wins",
			text: "key=first\nkey=second\n",
			want: map[string]string{"key": "second"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseProperties(tt.text); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseProperties() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfigFromProperties(t *testing.T) {
	tests := []struct {
		name       string
		properties map[string]string
		want       config
	}{
		{
			name:       "defaults",
			properties: map[string]string{},
			want:       config{logDir: "/tmp/kraft-combined-logs"},
		},
		{
			name:       "log.dirs",
			properties: map[string]string{"log.dirs": "/var/lib/kafka"},
			want:       config{logDir: "/var/lib/kafka"},
		},
		{
			name:       "first of several log.dirs",
			properties: map[string]string{"log.dirs": " /data/one , /data/two"},
			want:       config{logDir: "/data/one"},
		},
		{
			name:       "log.dir",
			properties: map[string]string{"log.dir": "/var/lib/kafka"},
			want:       config{logDir: "/var/lib/kafka"},
		},
		{
			name:       "log.dirs takes precedence over log.dir",
			properties: map[string]string{"log.dir": "/data/one", "log.dirs": "/data/two"},
			want:       config{logDir: "/data/two"},
		},
		{
			name:       "empty log.dirs falls back to log.dir",
			properties: map[string]string{"log.dir": "/data/one", "log.dirs": " , "},
			want:       config{logDir: "/data/one"},
		},
		{
			name:       "empty values fall back to the default",
			properties: map[string]string{"log.dir": "", "log.dirs": ""},
			want:       config{logDir: "/tmp/kraft-combined-logs"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := configFromProperties(tt.properties); got != tt.want {
				t.Errorf("configFromProperties() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestLoadConfig(t *testing.T) {
	t.Run("server.properties", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "server.properties")
		writeFile(t, path, []byte(serverProperties))

		cfg, err := loadConfig(path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if want := (config{logDir: "/tmp/kraft-combined-logs"}); cfg != want {
			t.Errorf("config = %+v, want %+v", cfg, want)
		}
	})

	t.Run("custom log directory", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "server.properties")
		writeFile(t, path, []byte("node.id=1\nlog.dirs=/custom/logs\n"))

		cfg, err := loadConfig(path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.logDir != "/custom/logs" {
			t.Errorf("logDir = %q, want %q", cfg.logDir, "/custom/logs")
		}
	})

	t.Run("missing file", func(t *testing.T) {
		if _, err := loadConfig(filepath.Join(t.TempDir(), "missing.properties")); err == nil {
			t.Error("expected an error, got nil")
		}
	})
}

func TestDefaultConfig(t *testing.T) {
	if got, want := defaultConfig(), (config{logDir: "/tmp/kraft-combined-logs"}); got != want {
		t.Errorf("defaultConfig() = %+v, want %+v", got, want)
	}
}
