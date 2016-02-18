package goes

import (
	"github.com/satori/go.uuid"
	"time"
)

var serializer Serializer
var storage Storage

type Event struct {
	AggregateId uuid.UUID
	Payload     interface{}
}

func SetStorage(newStorage Storage) {
	storage = newStorage
}

func SetSerializer(newSerializer Serializer) {
	serializer = newSerializer
}

var mapLock chan int = make(chan int, 1)
var streamsLock map[string]chan int = make(map[string]chan int)

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

func AddEvent(event Event) error {
	streamName := event.AggregateId.String()

	lockStream(streamName)
	defer unlockStream(streamName)

	serializedPayload, typeId, err := serializer.Serialize(event.Payload)
	if err != nil {
		return err
	}

	return storage.Write(&StoredEvent{event.AggregateId, time.Now(), typeId, serializedPayload})
}

func RetrieveFor(aggregateId uuid.UUID) ([]*Event, error) {
	results, err := storage.ReadStream(aggregateId)
	if err != nil {
		return nil, err
	}

	events := make([]*Event, 0)
	for _, storedEvent := range results {
		event, err := serializer.Deserialize(storedEvent.Data, storedEvent.TypeId)
		if err != nil {
			return nil, err
		}
		events = append(events, &Event{storedEvent.StreamId, event})
	}

	return events, nil
}

func RetrieveAll() ([]*Event, error) {
	results, err := storage.ReadAll()
	if err != nil {
		return nil, err
	}

	events := make([]*Event, 0)
	for _, storedEvent := range results {
		event, err := serializer.Deserialize(storedEvent.Data, storedEvent.TypeId)
		if err != nil {
			return nil, err
		}
		events = append(events, &Event{storedEvent.StreamId, event})
	}

	return events, nil
}