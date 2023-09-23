run:
	go run main.go

purge:
	go run cmd/purge/main.go

update:
	go get -u
	go mod tidy