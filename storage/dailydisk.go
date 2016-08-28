package storage

import (
	"path"
	"os"
	"time"
	"github.com/satori/go.uuid"
	"fmt"
	"errors"
	"io/ioutil"
	"bytes"
)

const EMPTY_STREAM = uint32(0)
var CRLF = []byte("\r\n")

type DailyDiskStorage struct {
	storagePath string
	indexesPath string
	typesIndexesPath string
	globalIndexFilename string
}

func NewDailyDiskStorage(storagePath string) Storage {
	fmt.Println("Using DailyDiskStorage path:", storagePath)
	indexesPath := path.Join(storagePath, "indexes")
	globalIndexPath := path.Join(indexesPath, "global")
	typesIndexesPath := path.Join(indexesPath, "types")
	if err := os.MkdirAll(typesIndexesPath, 0777); err != nil {
		panic(err)
	}
	return &DailyDiskStorage{storagePath, indexesPath, typesIndexesPath, globalIndexPath};
}

func (me DailyDiskStorage) getStreamIndexFilename(streamId uuid.UUID) string {
	return path.Join(me.indexesPath, streamId.String())
}

func (me DailyDiskStorage) getEventFilename(creationTime time.Time, typeId string) string {
	yearMonth := fmt.Sprintf("%04d%02d", creationTime.Year(), creationTime.Month())
	day := fmt.Sprintf("%02d", creationTime.Day())
	eventFilename := fmt.Sprintf("%02d%02d%02d%09d_%s", creationTime.Hour(), creationTime.Minute(), creationTime.Second(), creationTime.Nanosecond(), typeId)
	return path.Join(me.storagePath, yearMonth, day, eventFilename)
}

type IndexEntry struct {
	streamId uuid.UUID
	creationTime time.Time
	typeId string
}

func appendIndex(filename string, entry *IndexEntry) error {
	indexFile, err := os.OpenFile(filename, os.O_APPEND | os.O_WRONLY | os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer indexFile.Close()

	written, err := indexFile.Write(entry.streamId.Bytes())
	if err != nil {
		return err
	}
	if written != 16 {
		return errors.New(fmt.Sprintf("Write error. Expected to write %v bytes, wrote only %v.", 16, written))
	}

	creationTimeBytes, err := entry.creationTime.MarshalBinary()
	if err != nil {
		return err
	}
	writeSizeAndBytes(indexFile, creationTimeBytes)
	writeSizeAndBytes(indexFile, []byte(entry.typeId))

	return nil
}

func (me DailyDiskStorage) appendTypeIndex(entry *IndexEntry) error {
	filename := path.Join(me.typesIndexesPath, entry.typeId)
	indexFile, err := os.OpenFile(filename, os.O_APPEND | os.O_WRONLY | os.O_CREATE, 0644 )
	if err != nil {
		return err
	}
	defer indexFile.Close()

	value := me.getEventFilename(entry.creationTime, entry.typeId)
	start := len(me.storagePath) + 1
	_, err = indexFile.WriteString(value[start:] + "\r\n")

	return err
}

func readIndexNextEntry(f *os.File) (*IndexEntry, error) {
	index := IndexEntry{}

	uuidBytes := make([]byte, 16)
	read, err := f.Read(uuidBytes)
	if err != nil {
		return nil, err
	}
	if read != 16 {
		return nil, errors.New(fmt.Sprintf("Integrity error. Expected to read %v bytes, got only %v bytes.", 16, read))
	}
	index.streamId = uuid.FromBytesOrNil(uuidBytes)

	creationTimeBytes, err := readSizedBytes(f)
	if err != nil {
		return nil, err
	}
	if err = index.creationTime.UnmarshalBinary(creationTimeBytes); err != nil {
		return nil, err
	}

	typeIdBytes, err := readSizedBytes(f)
	index.typeId = string(typeIdBytes)

	return &index, nil;
}

func writeEvent(filename string, data []byte, metadata []byte) error {
	eventFile, err := os.OpenFile(filename, os.O_APPEND | os.O_WRONLY | os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer eventFile.Close()

	eventFile.Write(data)
	eventFile.Write(CRLF)
	eventFile.Write(metadata)

	return nil
}

func readEvent(filename string) (data []byte, metadata []byte, err error) {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return
	}
	sep := bytes.Index(content, CRLF)
	if sep == -1 {
		data = content
		metadata = make([]byte, 0)
		return
	}
	data = content[:sep]
	metadata = content[sep+2:]
	return
}

func (me DailyDiskStorage) Write(event *StoredEvent) error {

	eventFilename := me.getEventFilename(event.CreationTime, event.TypeId)
	os.MkdirAll(path.Dir(eventFilename), 0777)

	err := writeEvent(eventFilename, event.Data, event.Metadata)
	if err != nil {
		return err
	}

	index := &IndexEntry{event.StreamId, event.CreationTime, event.TypeId}

	err = appendIndex(me.globalIndexFilename, index)
	if err != nil {
		return err
	}

	err = appendIndex(me.getStreamIndexFilename(event.StreamId), index)
	if err != nil {
		return err
	}

	return me.appendTypeIndex(index)
}

func (me DailyDiskStorage) StreamVersion(streamId uuid.UUID) (uint32, error) {
	indexFile, err := os.OpenFile(me.getStreamIndexFilename(streamId), os.O_RDONLY, 0)
	if err != nil {
		return EMPTY_STREAM, errors.New("NOT_FOUND: " + err.Error())
	}
	defer indexFile.Close()

	ver := EMPTY_STREAM
	for {
		_, err := readIndexNextEntry(indexFile)
		if err != nil && err.Error() == "EOF" {
			break
		}
		if err != nil {
			return EMPTY_STREAM, err
		}
		ver++
	}

	return ver, nil
}

func (me DailyDiskStorage) ReadStream(streamId uuid.UUID) ([]*StoredEvent, error) {

	indexFile, err := os.OpenFile(me.getStreamIndexFilename(streamId), os.O_RDONLY, 0)
	if err != nil {
		return nil, errors.New("NOT_FOUND: " + err.Error())
	}
	defer indexFile.Close()

	events := make([]*StoredEvent, 0)
	for {
		indexEntry, err := readIndexNextEntry(indexFile)
		if err != nil && err.Error() == "EOF" {
			break
		}
		if err != nil {
			return nil, err
		}
		data, metadata, err := readEvent(me.getEventFilename(indexEntry.creationTime, indexEntry.typeId))
		if err != nil {
			return nil, err
		}
		event := &StoredEvent{streamId, indexEntry.creationTime, indexEntry.typeId, data, "Metadata", metadata}
		events = append(events, event)
	}

	return events, nil
}

func (me DailyDiskStorage) ReadAll() ([]*StoredEvent, error) {
	indexFile, err := os.OpenFile(me.globalIndexFilename, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer indexFile.Close()

	events := make([]*StoredEvent, 0)
	for {
		indexEntry, err := readIndexNextEntry(indexFile)
		if err != nil && err.Error() == "EOF" {
			break
		}
		if err != nil {
			return nil, err
		}
		data, metadata, err := readEvent(me.getEventFilename(indexEntry.creationTime, indexEntry.typeId))
		if err != nil {
			return nil, err
		}
		event := &StoredEvent{indexEntry.streamId, indexEntry.creationTime, indexEntry.typeId, data, "Metadata", metadata}
		events = append(events, event)
	}

	return events, nil
}

func (me DailyDiskStorage) RebuildTypeIndexes() {
	fmt.Print("Rebuilding type indexes... ")

	err := os.RemoveAll(me.typesIndexesPath)
	if err != nil {
		panic(err)
	}
	err = os.MkdirAll(me.typesIndexesPath, 0644)
	if err != nil {
		panic(err)
	}

	globalIndexFile, err := os.OpenFile(me.globalIndexFilename, os.O_RDONLY, 0)
	if err != nil {
		panic(err)
	}

	for {
		indexEntry, err := readIndexNextEntry(globalIndexFile)
		if err != nil && err.Error() == "EOF" {
			break
		}
		if err != nil {
			panic(err)
		}
		me.appendTypeIndex(indexEntry)
	}

	fmt.Println("Done.")
}