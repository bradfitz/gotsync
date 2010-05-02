gotsync: main.8 gotsync.8
	8l -o gotsync main.8

gotsync.8: gotsync.go
	8g -o gotsync.8 gotsync.go

main.8: main.go
	8g -o main.8 main.go

