package goes

import (
	"github.com/satori/go.uuid"
	"os"
	"fmt"
	"encoding/binary"
	"path"
	"errors"
)

var storagePath string
var serializer Serializer

const IntegerSizeInBytes = 8
const StreamStartingCapacity = 512

type Event struct {
	AggregateId uuid.UUID
	Payload     interface{}
}

func SetStoragePath(newStoragePath string) {
	storagePath = newStoragePath
}

func SetSerializer(newSerializer Serializer) {
	serializer = newSerializer
}

func getFilename(stream, extension string) string {
	return fmt.Sprintf("%v%v", path.Join(storagePath, stream[0:2], stream[2:]), extension)
}

func getFilenameForEvents(stream string) string {
	return getFilename(stream, ".history")
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

	filename := getFilenameForEvents(streamName)
	os.MkdirAll(path.Dir(filename), os.ModeDir)

	eventIndexPath := path.Join(storagePath, "eventindex")
	indexFile, err := os.OpenFile(eventIndexPath, os.O_APPEND | os.O_WRONLY | os.O_CREATE, 0)
	if err != nil {
		return err
	}
	defer indexFile.Close()

	eventsFile, err := os.OpenFile(filename, os.O_APPEND | os.O_WRONLY | os.O_CREATE, 0)
	if err != nil {
		return err
	}
	defer eventsFile.Close()

	stat, err := eventsFile.Stat()
	if err != nil {
		return err
	}
	position := stat.Size()

	serializedPayload, err := serializer.Serialize(event.Payload)
	if err != nil {
		return err
	}

	lengthBytes := make([]byte, IntegerSizeInBytes)
	binary.BigEndian.PutUint64(lengthBytes, uint64(len(serializedPayload)))
	eventsFile.Write(lengthBytes)
	eventsFile.Write(serializedPayload)

	indexFile.Write(event.AggregateId.Bytes())
	positionBytes := make([]byte, IntegerSizeInBytes)
	binary.BigEndian.PutUint64(positionBytes, uint64(position))
	indexFile.Write(positionBytes)

	return nil
}

func RetrieveFor(aggregateId uuid.UUID) ([]Event, error) {
	streamName := aggregateId.String()
	offset := getStartingIndexFor(streamName)
	filename := getFilenameForEvents(streamName)

	eventsFile, err := os.OpenFile(filename, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer eventsFile.Close()

	eventsFile.Seek(offset, 0)

	contentLengthBytes := make([]byte, IntegerSizeInBytes)
	events := make([]Event, 0)
	for {
		read, err := eventsFile.Read(contentLengthBytes)
		if err != nil {
			break
		}
		if read < 8 {
			return nil, errors.New("event index integrity error")
		}
		event, err := getStoredEvent(eventsFile, contentLengthBytes)
		if err != nil {
			return nil, err
		}
		events = append(events, Event{aggregateId, event})
	}
	return events, nil
}

func getStartingIndexFor(streamName string) int64 {
	//TODO: snapshots
	return int64(0)
}

func getStoredEvent(eventsFile *os.File, contentLengthBytes []byte) (interface{}, error) {
	contentLength := binary.BigEndian.Uint64(contentLengthBytes)
	content := make([]byte, contentLength)
	read, err := eventsFile.Read(content)
	if err != nil {
		return nil, err
	}
	if uint64(read) < contentLength {
		return nil, errors.New("incomplete event information retrieved")
	}
	return serializer.Deserialize(content)
}

func retrieveEvent(aggregateId uuid.UUID, offset int64) (*Event, error) {
	filename := getFilenameForEvents(aggregateId.String())

	eventsFile, err := os.OpenFile(filename, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer eventsFile.Close()

	eventsFile.Seek(offset, 0)
	contentLengthBytes := make([]byte, IntegerSizeInBytes)
	read, err := eventsFile.Read(contentLengthBytes)
	if err != nil {
		return nil, err
	}
	if read < IntegerSizeInBytes {
		return nil, errors.New("event integrity problem")
	}
	content, err := getStoredEvent(eventsFile, contentLengthBytes)
	if err != nil {
		return nil, err
	}
	return &Event{aggregateId, content}, nil
}

func RetrieveAll() ([]*Event, error) {
	indexFile, err := os.OpenFile(path.Join(storagePath, "eventindex"), os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer indexFile.Close()

	events := make([]*Event, 0)
	guidBytes := make([]byte, 16)
	offsetBytes := make([]byte, IntegerSizeInBytes)
	for {
		read, err := indexFile.Read(guidBytes)
		if err != nil {
			break
		}
		if read != 16 {
			return nil, errors.New("index integrity error")
		}
		read, err = indexFile.Read(offsetBytes)
		if err != nil {
			return nil, err
		}
		if read != IntegerSizeInBytes {
			return nil, errors.New("index integrity error")
		}
		aggregateId, err := uuid.FromBytes(guidBytes)
		if err != nil {
			return nil, err
		}
		offset := binary.BigEndian.Uint64(offsetBytes)

		event, err := retrieveEvent(aggregateId, int64(offset))
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	return events, nil
}