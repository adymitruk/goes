package main

import (
	"fmt"
	"github.com/pebbe/zmq4"
	"github.com/satori/go.uuid"
	"bitbucket.org/nicdex/adaptech-goes"
	"os"
	"path"
	"encoding/binary"
)

func main() {
	fmt.Println("Simple ZeroMQ server for goes.")

	//TODO: config/flag
	storagePath := path.Join(os.TempDir(), uuid.NewV4().String())

	goes.SetStorage(goes.NewDiskStorage(storagePath))
	goes.SetSerializer(goes.NewPassthruSerializer())

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

	//TODO: config/flag
	listenAddr := "tcp://*:12345"
	err = replySocket.Bind(listenAddr)
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
			fmt.Println(command, aggregateId.String())
			data := message[2]
			err = goes.AddEvent(goes.Event{aggregateId, data})
			if err != nil {
				panic(err)
			}
			replySocket.Send("Ok", 0)
		case "ReadStream":
			aggregateId, err := uuid.FromBytes(message[1])
			if err != nil {
				fmt.Println("Wrong format for AggregateId", err)
				break
			}
			fmt.Println(command, aggregateId.String())
			events, err := goes.RetrieveFor(aggregateId)
			if err != nil {
				panic(err)
			}
			sendEvents(replySocket, events)
		case "ReadAll":
			fmt.Println(command)
			events, err := goes.RetrieveAll()
			if err != nil {
				panic(err)
			}
			sendEvents(replySocket, events)
		}
	}
}

func sendEvents(socket *zmq4.Socket, events []*goes.Event) {
	len := len(events)
	lenBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(lenBytes, uint64(len))
	socket.SendBytes(lenBytes, zmq4.SNDMORE)

	i := 0
	for ; i < len-1; i++ {
		socket.SendBytes(events[i].Payload.([]byte), zmq4.SNDMORE)
	}
	socket.SendBytes(events[i].Payload.([]byte), 0)
}