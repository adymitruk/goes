package goes

import (
	"os"
	"path"
	"encoding/binary"
	"github.com/satori/go.uuid"
	"fmt"
	"errors"
)

const IntegerSizeInBytes = 8
const StreamStartingCapacity = 512

type StoredEvent struct {
	StreamId uuid.UUID
	Data []byte
}

//TODO: performance - change reads array for some kind of iterator
type Storage interface {
	Write(streamId uuid.UUID, data []byte) error
	ReadStream(streamId uuid.UUID) ([]*StoredEvent, error)
	ReadAll() ([]*StoredEvent, error)
}

func NewDiskStorage(storagePath string) Storage {
	return &DiskStorage{storagePath, path.Join(storagePath, "eventindex")}
}

type DiskStorage struct {
	storagePath string
	indexPath string
}

func (me DiskStorage) getFilename(stream, extension string) string {
	return fmt.Sprintf("%v%v", path.Join(me.storagePath, stream[0:2], stream[2:]), extension)
}

func (me DiskStorage) getFilenameForEvents(stream string) string {
	return me.getFilename(stream, ".history")
}

func (me DiskStorage) Write(streamId uuid.UUID, data []byte) error {
	filename := me.getFilenameForEvents(streamId.String())
	os.MkdirAll(path.Dir(filename), os.ModeDir)

	indexFile, err := os.OpenFile(me.indexPath, os.O_APPEND | os.O_WRONLY | os.O_CREATE, 0)
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

	lengthBytes := make([]byte, IntegerSizeInBytes)
	binary.BigEndian.PutUint64(lengthBytes, uint64(len(data)))
	eventsFile.Write(lengthBytes)
	eventsFile.Write(data)

	indexFile.Write(streamId.Bytes())
	positionBytes := make([]byte, IntegerSizeInBytes)
	binary.BigEndian.PutUint64(positionBytes, uint64(position))
	indexFile.Write(positionBytes)

	return nil
}

func (me DiskStorage) ReadStream(streamId uuid.UUID) ([]*StoredEvent, error) {
	streamName := streamId.String()
	offset := int64(0) //TODO snapshots
	filename := me.getFilenameForEvents(streamName)

	eventsFile, err := os.OpenFile(filename, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer eventsFile.Close()

	eventsFile.Seek(offset, 0)

	contentLengthBytes := make([]byte, IntegerSizeInBytes)
	results := make([]*StoredEvent, 0)
	for {
		read, err := eventsFile.Read(contentLengthBytes)
		if err != nil {
			break
		}
		if read != IntegerSizeInBytes {
			return nil, errors.New("event index integrity error")
		}
		data, err := getStoredData(eventsFile, contentLengthBytes)
		if err != nil {
			return nil, err
		}
		results = append(results, &StoredEvent{streamId, data})
	}
	return results, nil
}

func (me DiskStorage) ReadAll() ([]*StoredEvent, error) {
	indexFile, err := os.OpenFile(me.indexPath, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer indexFile.Close()

	results := make([]*StoredEvent, 0)
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

		data, err := me.retrieveData(aggregateId, int64(offset))
		if err != nil {
			return nil, err
		}
		results = append(results, &StoredEvent{aggregateId, data})
	}

	return results, nil
}

func (me DiskStorage) retrieveData(aggregateId uuid.UUID, offset int64) ([]byte, error) {
	filename := me.getFilenameForEvents(aggregateId.String())

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
	return getStoredData(eventsFile, contentLengthBytes)
}

func getStoredData(eventsFile *os.File, contentLengthBytes []byte) ([]byte, error) {
	contentLength := binary.BigEndian.Uint64(contentLengthBytes)
	data := make([]byte, contentLength)
	read, err := eventsFile.Read(data)
	if err != nil {
		return nil, err
	}
	if uint64(read) < contentLength {
		return nil, errors.New("incomplete event information retrieved")
	}
	return data, nil
}