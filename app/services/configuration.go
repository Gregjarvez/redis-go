package services

import "flag"

type Configuration struct {
	Dir        *string
	DbFilename *string
	Port       *int
	Host       *string
	ReplicaOf  *string
}

// Config not the brightest idea 💡
var Config = Configuration{
	Dir:        flag.String("dir", "", "Directory to store the database"),
	DbFilename: flag.String("dbfilename", "db.json", "Database filename"),
	Port:       flag.Int("port", 6379, "Port to listen on"),
	Host:       flag.String("host", "0.0.0.0", "Host to listen on"),
	ReplicaOf:  flag.String("replicaof", "", "ReplicaOf mode"),
}
