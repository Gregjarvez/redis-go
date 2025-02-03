test:
	go test ./...

submit:
	codecrafters test

run:
	./your_program.sh

run-slave:
	./your_program.sh --port 6380 --replicaof "localhost 6379"