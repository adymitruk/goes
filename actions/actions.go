package actions

import (
	data "../data"
	serializer "../serializer"
	storage "../storage"
	"errors"
	"fmt"
	"github.com/satori/go.uuid"
	"time"
)

const NO_EXPECTEDVERSION = uint32(0xFFFFFFFF)

var mapLock chan int = make(chan int, 1)
var streamsLock map[string]chan int = make(map[string]chan int)

type Handler interface {
	AddEvent(data.Event, uint32) error
	RetrieveFor(uuid.UUID) ([]*data.Event, error)
	RetrieveAll() ([]*data.Event, error)
}

type ActionsHandler struct {
	storage    storage.Storage
	serializer serializer.Serializer
}

func NewActionsHandler(storage storage.Storage, serializer serializer.Serializer) *ActionsHandler {
	return &ActionsHandler{storage, serializer}
}

func lockStream(streamName string) {
	mapLock <- 1
	defer func() {
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

func (me ActionsHandler) AddEvent(event data.Event, expectedVersion uint32) error {
	streamName := event.AggregateId.String()

	lockStream(streamName)
	defer unlockStream(streamName)

	serializedPayload, typeId, err := me.serializer.Serialize(event.Payload)
	if err != nil {
		return err
	}

	serializedMetadata, _, err := me.serializer.Serialize(event.Metadata)
	if err != nil {
		return err
	}

	if expectedVersion != NO_EXPECTEDVERSION {
		ver, err := me.storage.StreamVersion(event.AggregateId)
		if err != nil && err.Error()[0:9] != "NOT_FOUND" {
			return err
		}
		if ver != expectedVersion {
			return errors.New(fmt.Sprint("WrongExpectedVersion: expected ", expectedVersion, " got ", ver))
		}
	}

	return me.storage.Write(&storage.StoredEvent{
		StreamId: event.AggregateId,
		CreationTime: time.Now(),
		TypeId: typeId,
		Data: serializedPayload,
		Metadata: serializedMetadata})
}

func (me ActionsHandler) RetrieveFor(aggregateId uuid.UUID) ([]*data.Event, error) {
	results, err := me.storage.ReadStream(aggregateId)
	if err != nil && err.Error()[0:9] == "NOT_FOUND" {
		return make([]*data.Event, 0), nil
	}
	if err != nil {
		return nil, err
	}

	events := make([]*data.Event, 0)
	for _, storedEvent := range results {
		event, err := me.serializer.Deserialize(storedEvent.Data, storedEvent.TypeId)
		if err != nil {
			return nil, err
		}
		metadata, err := me.serializer.Deserialize(storedEvent.Metadata, storedEvent.MetadataTypeId)
		if err != nil {
			return nil, err
		}
		events = append(events, &data.Event{
			AggregateId: storedEvent.StreamId,
			CreationTime: storedEvent.CreationTime,
			Payload: event,
			Metadata: metadata})
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
		metadata, err := me.serializer.Deserialize(storedEvent.Metadata, storedEvent.MetadataTypeId)
		if err != nil {
			return nil, err
		}
		events = append(events, &data.Event{
			AggregateId: storedEvent.StreamId,
			CreationTime: storedEvent.CreationTime,
			Payload: event,
			Metadata: metadata})
	}

	return events, nil
}
