package storage

import (
	"github.com/satori/go.uuid"
	"time"
)

const IntegerSizeInBytes = 8
const StreamStartingCapacity = 512

type StoredEvent struct {
	StreamId uuid.UUID
	CreationTime time.Time
	TypeId string
	Data []byte
}

//TODO: performance - change reads array for some kind of iterator
type Storage interface {
	Write(event *StoredEvent) error
	ReadStream(streamId uuid.UUID) ([]*StoredEvent, error)
	ReadAll() ([]*StoredEvent, error)
}