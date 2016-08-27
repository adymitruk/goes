package main

import (
	actions "./actions"
	serializer "./serializer"
	server "./server"
	storage "./storage"
	"flag"
	"fmt"
	"os"
	"path"
)

var addr = flag.String("addr", "tcp://127.0.0.1:12345", "zeromq address to listen to")
var db = flag.String("db", fmt.Sprintf(".%cevents", os.PathSeparator), "path for storage")

func PathIsAbsolute(s string) bool {
	if len(s) > 1 && s[1] == ':' {
		return true
	}
	return path.IsAbs(s)
}

func main() {
	fmt.Println("GoES - Go Event Store")
	fmt.Println("Released under the MIT license. See LICENSE file.")
	fmt.Println()

	flag.Parse()

	storagePath := *db
	if !PathIsAbsolute(storagePath) {
		wd, _ := os.Getwd()
		storagePath = path.Join(wd, storagePath)
	}

	fmt.Println("Listening on:", *addr)
	fmt.Println("Storage path:", storagePath)

	var handler = actions.NewActionsHandler(storage.NewDailyDiskStorage(storagePath), serializer.NewPassthruSerializer())
	server.Bind(*addr)
	server.Listen(handler)
	server.Destroy()
}
