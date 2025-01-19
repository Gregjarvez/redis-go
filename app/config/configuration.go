package config

import "flag"

type Configuration struct {
	Dir        *string
	DbFilename *string
	Port       *int
	Host       *string
	Replica    *bool
}

// Config not the brightest idea 💡
var Config = Configuration{
	Dir:        flag.String("dir", "", "Directory to store the database"),
	DbFilename: flag.String("dbfilename", "db.json", "Database filename"),
	Port:       flag.Int("port", 6379, "Port to listen on"),
	Host:       flag.String("host", "0.0.0.0", "Host to listen on"),
	Replica:    flag.Bool("replicaof", false, "Replica mode"),
}
