.PHONY: install
install:
	rm -rf bin
	GOBIN=$(PWD)/bin go install ./...

.PHONY: run-both
run-both:
	# go run cmd/SurfstoreServerExec/main.go -s both -p 8080 -l -d localhost:8081
	go run cmd/SurfstoreServerExec/main.go -s both -p 8080 -l -d localhost:8080

.PHONY: run-blockstore
run-blockstore:
	# go run cmd/SurfstoreServerExec/main.go -s block -p 8081 -l -d
	go run cmd/SurfstoreServerExec/main.go -s block -p 8081 -d

.PHONY: run-metastore
run-metastore:
	# go run cmd/SurfstoreServerExec/main.go -s meta -l -d localhost:8081
	go run cmd/SurfstoreServerExec/main.go -s meta -p 8080 -d localhost:8081


# client 1 creates and syncs. client 2 syncs, deletes and syncs. client 1 modifies and syncs. client 2 syncs
syncTwoClientsDeleteConflict:
	rm -rf test/syncTwoClientsDeleteConflict
	mkdir test/syncTwoClientsDeleteConflict test/syncTwoClientsDeleteConflict/client1 test/syncTwoClientsDeleteConflict/client2
	cp statics/article.txt test/syncTwoClientsDeleteConflict/client1/article.txt
	go run cmd/SurfstoreClientExec/main.go -d localhost:8080 test/syncTwoClientsDeleteConflict/client1 10240
	go run cmd/SurfstoreClientExec/main.go -d localhost:8080 test/syncTwoClientsDeleteConflict/client2 10240
	rm test/syncTwoClientsDeleteConflict/client2/article.txt
	go run cmd/SurfstoreClientExec/main.go -d localhost:8080 test/syncTwoClientsDeleteConflict/client2 10240
	cp statics/nothing test/syncTwoClientsDeleteConflict/client1/article.txt
	go run cmd/SurfstoreClientExec/main.go -d localhost:8080 test/syncTwoClientsDeleteConflict/client1 10240
	go run cmd/SurfstoreClientExec/main.go -d localhost:8080 test/syncTwoClientsDeleteConflict/client2 10240

syncTwoClient1:
	rm -rf test/syncTwoClient1
	mkdir test/syncTwoClient1 test/syncTwoClient1/client1 test/syncTwoClient1/client2
	cp statics/article.txt test/syncTwoClient1/client1/article.txt
	cp statics/nothing test/syncTwoClient1/client1/nothing
	go run cmd/SurfstoreClientExec/main.go -d localhost:8080 test/syncTwoClient1/client1 10240 &
	go run cmd/SurfstoreClientExec/main.go -d localhost:8080 test/syncTwoClient1/client2 10240
