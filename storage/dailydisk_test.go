package storage

import (
	"testing"
	"os"
	"path"
	"github.com/satori/go.uuid"
	"time"
	"reflect"
)

func TestAddEvent(t *testing.T) {
	//Arrange
	storagePath := path.Join(os.TempDir(), uuid.NewV4().String())
	defer os.RemoveAll(storagePath)
	storage := NewDailyDiskStorage(storagePath)

	aLocation, _ := time.LoadLocation("")
	aTime := time.Date(2016,2,11,9,53,32,1234567, aLocation)
	aggregateId := uuid.NewV4()
	aType := "myType"
	data := []byte("{}")

	//Act
	err := storage.Write(&StoredEvent{aggregateId, aTime, aType, data})

	//Assert
	if err != nil {
		t.Errorf("Write failed. Error: %v", err)
	}

	readableDiskStorage := storage.(*DailyDiskStorage)

	globalIndexFi, _ := os.Stat(readableDiskStorage.globalIndexFilename)
	if globalIndexFi == nil {
		t.Error("Write failed. Expected global index file, none exists.")
	}
	aggregateIndexFi, _ := os.Stat(readableDiskStorage.getStreamIndexFilename(aggregateId))
	if aggregateIndexFi == nil {
		t.Errorf("Write failed. Expected index for aggregate %v, none exists.", aggregateId.String())
	}
	eventFi, _ := os.Stat(readableDiskStorage.getEventFilename(aTime, aType))
	if eventFi == nil {
		t.Errorf("Write failed. Expected file for event %v, none exists.", aggregateId.String())
	}

	//TODO: check indexes/event content
}

func TestReadStream(t *testing.T) {
	//Arrange
	storagePath := path.Join(os.TempDir(), uuid.NewV4().String())
	defer os.RemoveAll(storagePath)
	storage := NewDailyDiskStorage(storagePath)

	streamId := uuid.NewV4()
	ev1 := &StoredEvent{streamId, time.Now(), "1stType", []byte("1stEvent")}
	storage.Write(ev1)
	ev2 := &StoredEvent{streamId, time.Now(), "2ndType", []byte("2ndEvent")}
	storage.Write(ev2)

	//Act
	storedEvents, err := storage.ReadStream(streamId)

	//Assert
	if err != nil {
		t.Errorf("ReadStream failed. Error: %v", err)
		return
	}
	if len(storedEvents) != 2 {
		t.Errorf("ReadStream failed. Got %v stored events, expected %v", len(storedEvents), 2)
		return
	}
	if !reflect.DeepEqual(storedEvents[0], ev1) {
		t.Errorf("ReadStream failed. First event doesn't match. %+v != %+v", storedEvents[0], ev1)
		return
	}
	if !reflect.DeepEqual(storedEvents[1], ev2) {
		t.Errorf("ReadStream failed. Second event doesn't match. %+v != %+v", storedEvents[1], ev2)
		return
	}
}

func TestReadAll(t *testing.T) {
	//Arrange
	storagePath := path.Join(os.TempDir(), uuid.NewV4().String())
	defer os.RemoveAll(storagePath)
	storage := NewDailyDiskStorage(storagePath)

	stream1Id := uuid.NewV4()
	stream2Id := uuid.NewV4()
	ev1 := &StoredEvent{stream1Id, time.Now(), "1stType", []byte("1stEvent")}
	storage.Write(ev1)
	ev2 := &StoredEvent{stream2Id, time.Now(), "2ndType", []byte("2ndEvent")}
	storage.Write(ev2)
	ev3 := &StoredEvent{stream1Id, time.Now(), "3rdType", []byte("3rdEvent")}
	storage.Write(ev3)

	//Act
	storedEvents, err := storage.ReadAll()

	//Assert
	if err != nil {
		t.Errorf("ReadAll failed. Error: %v", err)
		return
	}
	if len(storedEvents) != 3 {
		t.Errorf("ReadAll failed. Got %v stored events, expected %v", len(storedEvents), 3)
		return
	}
	if !reflect.DeepEqual(storedEvents[0], ev1) {
		t.Errorf("ReadAll failed. First event doesn't match. %+v != %+v", storedEvents[0], ev1)
		return
	}
	if !reflect.DeepEqual(storedEvents[1], ev2) {
		t.Errorf("ReadAll failed. Second event doesn't match. %+v != %+v", storedEvents[1], ev2)
		return
	}
	if !reflect.DeepEqual(storedEvents[2], ev3) {
		t.Errorf("ReadAll failed. Third event doesn't match. %+v != %+v", storedEvents[2], ev2)
		return
	}
}
