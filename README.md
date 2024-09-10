go tool pprof -http=":8080" profile
go run main.go -p cpu|mem
