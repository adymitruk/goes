package main

import (
	"fmt"
	actions "./actions"
	storage "./storage"
	serializer "./serializer"
	data "./data"
	"github.com/satori/go.uuid"
	"github.com/pebbe/zmq4"
	"flag"
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
	fmt.Println("Simple ZeroMQ server for goes.")

	flag.Parse()

	storagePath := *db
	if !PathIsAbsolute(storagePath) {
		wd, _ := os.Getwd()
		storagePath = path.Join(wd, storagePath)
	}

	fmt.Println("Listening on:", *addr)
	fmt.Println("Storage path:", storagePath)

	var handler = actions.NewActionsHandler(storage.NewDailyDiskStorage(storagePath), serializer.NewPassthruSerializer())

	context, err := zmq4.NewContext()
	if err != nil {
		panic(err)
	}
	defer context.Term()

	replySocket, err := context.NewSocket(zmq4.REP)
	if err != nil {
		panic(err)
	}
	defer replySocket.Close()

	err = replySocket.Bind(*addr)
	if err != nil {
		panic(err)
	}

	for {
		message, err := replySocket.RecvMessageBytes(0)
		if err != nil {
			fmt.Println("Error receiving command from client", err)
			continue
		}

		command := string(message[0])
		switch command {
		case "AddEvent":
			aggregateId, err := uuid.FromBytes(message[1])
			if err != nil {
				fmt.Println("Wrong format for AggregateId", err)
				break
			}
			fmt.Println("->", command, aggregateId.String())
			payload := message[2]
			err = handler.AddEvent(data.Event{AggregateId: aggregateId, Payload: payload})
			if err != nil {
				replySocket.Send(fmt.Sprintf("Error: %v", err), 0)
				fmt.Println(err)
				break
			}
			replySocket.Send("Ok", 0)
		case "ReadStream":
			aggregateId, err := uuid.FromBytes(message[1])
			if err != nil {
				fmt.Println("Wrong format for AggregateId", err)
				break
			}
			fmt.Println("->", command, aggregateId.String())
			events, err := handler.RetrieveFor(aggregateId)
			if err != nil {
				replySocket.Send(fmt.Sprintf("Error: %v", err), 0)
				fmt.Println(err)
				break
			}
			sendEvents(replySocket, events)
		case "ReadAll":
			fmt.Println("->", command)
			events, err := handler.RetrieveAll()
			if err != nil {
				replySocket.Send(fmt.Sprintf("Error: %v", err), 0)
				fmt.Println(err)
				break
			}
			sendEvents(replySocket, events)
		case "Shutdown":
			fmt.Println("->", command)
			return
		}
	}
}

func sendEvents(socket *zmq4.Socket, events []*data.Event) {
	len := len(events)
	socket.Send(fmt.Sprintf("%v", len), zmq4.SNDMORE)

	i := 0
	for ; i < len-1; i++ {
		socket.SendBytes(events[i].Payload.([]byte), zmq4.SNDMORE)
	}
	socket.SendBytes(events[i].Payload.([]byte), 0)
	fmt.Println("<-", len, "events")
}
