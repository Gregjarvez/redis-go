submit:
	codecrafters submit
test:
	codecrafters test
test-local:
	go test ./...
run:
	./your_program.sh
run-slave:
	./your_program.sh --port 6380 --replicaof "localhost 6379"