package server

import (
	"fmt"
	data "../data"
	actions "../actions"
	"github.com/satori/go.uuid"
	"github.com/pebbe/zmq4"
	"encoding/binary"
)

var _context *zmq4.Context
var _replySocket *zmq4.Socket
var _addr string

func Bind(addr string) {
	var err error;
	_addr = addr

	_context, err = zmq4.NewContext()
	if err != nil {
		panic(err)
	}

	_replySocket, err = _context.NewSocket(zmq4.REP)
	if err != nil {
		panic(err)
	}

	err = _replySocket.Bind(addr)
	if err != nil {
		panic(err)
	}
}

func Destroy() {
	_replySocket.Close()
	_context.Term()
}

const NO_FLAGS = zmq4.Flag(0)
const UUID_SIZE = 16
const COMMAND_FRAME = 0
const ARGS_FRAME = 1
const PAYLOAD_FRAME = 2
const METADATA_FRAME = 3

func Listen(handler actions.Handler) {
	fmt.Println("Listening for incoming commands on:", _addr)

	for {
		message, err := _replySocket.RecvMessageBytes(NO_FLAGS)
		if err != nil {
			fmt.Println("Error receiving command from client", err)
			continue
		}

		command := string(message[COMMAND_FRAME])
		switch command {
		case "AddEvent":
			// v1 - "AddEvent" [AggregateId] {payload}
			aggregateId, err := uuid.FromBytes(message[ARGS_FRAME])
			if err != nil {
				fmt.Println("Wrong format for AggregateId", err)
				break
			}
			fmt.Println("->", command, aggregateId.String())
			payload := message[PAYLOAD_FRAME]
			err = handler.AddEvent(data.Event{AggregateId: aggregateId, Payload: payload, Metadata: nil}, actions.NO_EXPECTEDVERSION)
			if err != nil {
				_replySocket.Send(fmt.Sprintf("Error: %v", err), NO_FLAGS)
				fmt.Println(err)
				break
			}
			_replySocket.Send("Ok", NO_FLAGS)
		case "AddEvent_v2":
			// v2 - "AddEvent" 16:AggregateId,4:expectedVersion {payload} {metadata}
			aggregateId, err := uuid.FromBytes(message[ARGS_FRAME][0:UUID_SIZE])
			if err != nil {
				fmt.Println("Wrong format for AggregateId", err)
				break
			}
			expectedVersion := binary.LittleEndian.Uint32(message[ARGS_FRAME][UUID_SIZE:])
			fmt.Println("->", command, aggregateId.String(), expectedVersion)
			payload := message[PAYLOAD_FRAME]
			metadata := message[METADATA_FRAME]
			err = handler.AddEvent(data.Event{AggregateId: aggregateId, Payload: payload, Metadata: metadata}, expectedVersion)
			if err != nil {
				_replySocket.Send(fmt.Sprintf("Error: %v", err), NO_FLAGS)
				fmt.Println(err)
				break
			}
			_replySocket.Send("Ok", NO_FLAGS)
		case "ReadStream", "ReadStream_v2":
			aggregateId, err := uuid.FromBytes(message[ARGS_FRAME])
			if err != nil {
				fmt.Println("Wrong format for AggregateId", err)
				break
			}
			fmt.Println("->", command, aggregateId.String())
			events, err := handler.RetrieveFor(aggregateId)
			if err != nil {
				_replySocket.Send(fmt.Sprintf("Error: %v", err), NO_FLAGS)
				fmt.Println(err)
				break
			}
			if command == "ReadStream_v2" {
				sendEvents_v2(_replySocket, events)
				break;
			}
			sendEvents_v1(_replySocket, events)
		case "ReadAll", "ReadAll_v2":
			fmt.Println("->", command)
			events, err := handler.RetrieveAll()
			if err != nil {
				_replySocket.Send(fmt.Sprintf("Error: %v", err), NO_FLAGS)
				fmt.Println(err)
				break
			}
			if command == "ReadAll_v2" {
				sendEvents_v2(_replySocket, events)
				break;
			}
			sendEvents_v1(_replySocket, events)
		case "Shutdown":
			fmt.Println("->", command)
			return
		}
	}
}

func sendEvent_v1(socket *zmq4.Socket, event *data.Event, isLast bool) {
	lastFlag := zmq4.SNDMORE
	if (isLast) {
		lastFlag = NO_FLAGS
	}
	socket.SendBytes(event.Payload.([]byte), lastFlag)
}

func sendEvents_v1(socket *zmq4.Socket, events []*data.Event) {
	len := len(events)
	if (len == 0) {
		socket.Send("0", NO_FLAGS)
		return
	}

	socket.Send(fmt.Sprintf("%v", len), zmq4.SNDMORE)

	i := 0
	for ; i < len; i++ {
		sendEvent_v1(socket, events[i], i == len - 1)
	}
	fmt.Println("<-", len, "events")
}

func sendEvent_v2(socket *zmq4.Socket, event *data.Event, isLast bool) {
	socket.SendBytes(event.Payload.([]byte), zmq4.SNDMORE)
	lastFlag := zmq4.SNDMORE
	if (isLast) {
		lastFlag = NO_FLAGS
	}
	socket.SendBytes(event.Metadata.([]byte), lastFlag)
}

func sendEvents_v2(socket *zmq4.Socket, events []*data.Event) {
	len := len(events)
	if (len == 0) {
		socket.Send("0", NO_FLAGS)
		return
	}

	socket.Send(fmt.Sprintf("%v", len), zmq4.SNDMORE)

	i := 0
	for ; i < len; i++ {
		sendEvent_v2(socket, events[i], i == len - 1)
	}
	fmt.Println("<-", len, "events")
}