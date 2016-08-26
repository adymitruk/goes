package storage

import (
	"encoding/binary"
	"github.com/satori/go.uuid"
	"os"
	"time"
	"path"
	"fmt"
	"errors"
)

func NewSimpleDiskStorage(storagePath string) Storage {
	return &SimpleDiskStorage{storagePath, path.Join(storagePath, "eventindex")}
}

type SimpleDiskStorage struct {
	storagePath string
	indexPath string
}

func (me SimpleDiskStorage) getFilename(stream, extension string) string {
	return fmt.Sprintf("%v%v", path.Join(me.storagePath, stream[0:2], stream[2:]), extension)
}

func (me SimpleDiskStorage) GetFilenameForEvents(stream string) string {
	return me.getFilename(stream, ".history")
}

func writeSizeAndBytes(f *os.File, data []byte) (error) {
	sizeBytes := make([]byte, IntegerSizeInBytes)
	size := len(data)
	binary.BigEndian.PutUint64(sizeBytes, uint64(size))

	written, err := f.Write(sizeBytes)
	if err != nil {
		return err
	}
	if written != IntegerSizeInBytes {
		return errors.New(fmt.Sprintf("Write error. Expected to write %v bytes, wrote only %v.", IntegerSizeInBytes, written))
	}

	written, err = f.Write(data)
	if err != nil {
		return err
	}
	if written != size {
		return errors.New(fmt.Sprintf("Write error. Expected to write %v bytes, wrote only %v.", size, written))
	}

	return nil
}

func readSizedBytes(f *os.File) ([]byte, error) {
	sizeBytes := make([]byte, IntegerSizeInBytes)
	read, err := f.Read(sizeBytes)
	if err != nil {
		return nil, err
	}
	if read != IntegerSizeInBytes {
		return nil, errors.New(fmt.Sprintf("Integrity error. Expected to read %d bytes, got %d bytes.", IntegerSizeInBytes, read))
	}
	size := binary.BigEndian.Uint64(sizeBytes)
	data := make([]byte, size)
	read, err = f.Read(data)
	if err != nil {
		return nil, err
	}
	if uint64(read) != size {
		return nil, errors.New(fmt.Sprintf("Integrity error. Expected to ready %d bytes, got %d bytes.", IntegerSizeInBytes, read))
	}

	return data, nil
}

func (me SimpleDiskStorage) Write(event *StoredEvent) error {
	filename := me.GetFilenameForEvents(event.StreamId.String())
	os.MkdirAll(path.Dir(filename), os.ModeDir)

	indexFile, err := os.OpenFile(me.indexPath, os.O_APPEND | os.O_WRONLY | os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer indexFile.Close()

	eventsFile, err := os.OpenFile(filename, os.O_APPEND | os.O_WRONLY | os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer eventsFile.Close()

	stat, err := eventsFile.Stat()
	if err != nil {
		return err
	}
	position := stat.Size()

	creationTimeBytes, err := event.CreationTime.MarshalBinary()
	if err != nil {
		return err
	}
	writeSizeAndBytes(eventsFile, creationTimeBytes)
	writeSizeAndBytes(eventsFile, []byte(event.TypeId))
	writeSizeAndBytes(eventsFile, event.Data)

	indexFile.Write(event.StreamId.Bytes())
	positionBytes := make([]byte, IntegerSizeInBytes)
	binary.BigEndian.PutUint64(positionBytes, uint64(position))
	indexFile.Write(positionBytes)

	return nil
}

func (me SimpleDiskStorage) ReadStream(streamId uuid.UUID) ([]*StoredEvent, error) {
	streamName := streamId.String()
	offset := int64(0) //TODO snapshots
	filename := me.GetFilenameForEvents(streamName)

	eventsFile, err := os.OpenFile(filename, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer eventsFile.Close()

	eventsFile.Seek(offset, 0)

	results := make([]*StoredEvent, 0)
	for {
		creationTime, typeId, data, err := getStoredData(eventsFile)
		if err != nil && err.Error() == "EOF" {
			break
		}
		if err != nil {
			return nil, err
		}

		event := &StoredEvent{streamId, creationTime, typeId, data}
		results = append(results, event)
	}
	return results, nil
}

func (me SimpleDiskStorage) ReadAll() ([]*StoredEvent, error) {
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

		storedEvent, err := me.retrieveStoredEvent(aggregateId, int64(offset))
		if err != nil {
			return nil, err
		}
		results = append(results, storedEvent)
	}

	return results, nil
}

func (me SimpleDiskStorage) retrieveStoredEvent(streamId uuid.UUID, offset int64) (*StoredEvent, error) {
	filename := me.GetFilenameForEvents(streamId.String())

	eventsFile, err := os.OpenFile(filename, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer eventsFile.Close()

	eventsFile.Seek(offset, 0)

	creationTime, typeId, data, err := getStoredData(eventsFile)
	if err != nil {
		return nil, err
	}

	event := &StoredEvent{streamId, creationTime, typeId, data}
	return event, nil
}

func getStoredData(eventsFile *os.File) (creationTime time.Time, typeId string, data []byte, err error) {
	creationTimeBytes, err := readSizedBytes(eventsFile)
	if err != nil {
		return
	}
	err = creationTime.UnmarshalBinary(creationTimeBytes)
	if err != nil {
		return
	}

	typeIdBytes, err := readSizedBytes(eventsFile)
	if err != nil {
		return
	}
	typeId = string(typeIdBytes)

	data, err = readSizedBytes(eventsFile)

	return
}

