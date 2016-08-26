package actions

import (
	"time"
	"github.com/satori/go.uuid"
	storage "../storage"
	serializer "../serializer"
	data "../data"
)

var mapLock chan int = make(chan int, 1)
var streamsLock map[string]chan int = make(map[string]chan int)

type ActionsHandler struct {
	storage storage.Storage
	serializer serializer.Serializer
}

func NewActionsHandler(storage storage.Storage, serializer serializer.Serializer) *ActionsHandler {
	return &ActionsHandler{storage, serializer}
}

func lockStream(streamName string) {
	mapLock <- 1
	defer func(){
		<-mapLock
	}()

	streamLock := streamsLock[streamName]
	if streamLock == nil {
		streamLock = make(chan int, 1)
		streamsLock[streamName] = streamLock
	}

	streamLock <- 1
}

func unlockStream(streamName string) {
	<-streamsLock[streamName]
}

func (me ActionsHandler) AddEvent(event data.Event) error {
	streamName := event.AggregateId.String()

	lockStream(streamName)
	defer unlockStream(streamName)

	serializedPayload, typeId, err := me.serializer.Serialize(event.Payload)
	if err != nil {
		return err
	}

	return me.storage.Write(&storage.StoredEvent{StreamId: event.AggregateId, CreationTime: time.Now(), TypeId: typeId, Data: serializedPayload})
}

func (me ActionsHandler) RetrieveFor(aggregateId uuid.UUID) ([]*data.Event, error) {
	results, err := me.storage.ReadStream(aggregateId)
	if err != nil {
		return nil, err
	}

	events := make([]*data.Event, 0)
	for _, storedEvent := range results {
		event, err := me.serializer.Deserialize(storedEvent.Data, storedEvent.TypeId)
		if err != nil {
			return nil, err
		}
		events = append(events, &data.Event{AggregateId: storedEvent.StreamId, Payload: event})
	}

	return events, nil
}

func (me ActionsHandler) RetrieveAll() ([]*data.Event, error) {
	results, err := me.storage.ReadAll()
	if err != nil {
		return nil, err
	}

	events := make([]*data.Event, 0)
	for _, storedEvent := range results {
		event, err := me.serializer.Deserialize(storedEvent.Data, storedEvent.TypeId)
		if err != nil {
			return nil, err
		}
		events = append(events, &data.Event{AggregateId: storedEvent.StreamId, Payload: event})
	}

	return events, nil
}

