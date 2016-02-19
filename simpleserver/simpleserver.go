package main

import (
	"fmt"
	"github.com/pebbe/zmq4"
	"github.com/satori/go.uuid"
	"bitbucket.org/nicdex/adaptech-goes"
	"bytes"
	"errors"
	"flag"
	"os"
	"path"
)

var port = flag.Int("port", 12345, "port to listen to")
var db = flag.String("db", fmt.Sprintf(".%cevents", os.PathSeparator), "path for storage")

type Serializer struct {}

func NewSerializer() goes.Serializer {
	return &Serializer{}
}

func (me Serializer) Serialize(input interface{}) (output []byte, typeId string, err error) {
	content, ok := input.([]byte)
	if !ok {
		err = errors.New("input should be []byte")
		return
	}

	sep := bytes.IndexByte(content, ' ')
	if sep == -1 {
		err = errors.New("missing split char.")
		return
	}

	output = content[sep+1:]
	typeId = string(content[0:sep])
	return
}

func (me Serializer) Deserialize(input []byte, typeId string) (interface{}, error) {
	output := []byte(typeId)
	output = append(output, ' ')
	output = append(output, input...)

	return output, nil
}

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

	fmt.Println("Listening on port:", *port)
	fmt.Println("Storage path:", storagePath)
	goes.SetStorage(goes.NewReadableDiskStorage(storagePath))
	goes.SetSerializer(NewSerializer())

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

	listenAddr := fmt.Sprintf("tcp://*:%d", *port)
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
			fmt.Println("->", command, aggregateId.String())
			data := message[2]
			err = goes.AddEvent(goes.Event{aggregateId, data})
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
			events, err := goes.RetrieveFor(aggregateId)
			if err != nil {
				replySocket.Send(fmt.Sprintf("Error: %v", err), 0)
				fmt.Println(err)
				break
			}
			sendEvents(replySocket, events)
		case "ReadAll":
			fmt.Println("->", command)
			events, err := goes.RetrieveAll()
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

func sendEvents(socket *zmq4.Socket, events []*goes.Event) {
	len := len(events)
	socket.Send(fmt.Sprintf("%v", len), zmq4.SNDMORE)

	i := 0
	for ; i < len-1; i++ {
		socket.SendBytes(events[i].Payload.([]byte), zmq4.SNDMORE)
	}
	socket.SendBytes(events[i].Payload.([]byte), 0)
	fmt.Println("<-", len, "events")
}