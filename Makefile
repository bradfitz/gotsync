gotsync: main.6 gotsync.6
	6l -o gotsync main.6

gotsync.6: gotsync.go
	6g -o gotsync.6 gotsync.go

main.6: main.go
	6g -o main.6 main.go

