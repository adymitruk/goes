package goes

import
(
	"testing"
	"github.com/satori/go.uuid"
	"os"
	"path"
	"reflect"
	"io/ioutil"
	"bytes"
)

var tempDir string

type AnEvent struct {
	A int64
	B string
}

type AnotherEvent struct {
	W int64
	T string
	F float64
}

func setUp() {
	tempDir := path.Join(os.TempDir(), uuid.NewV4().String())
	storage := NewDiskStorage(tempDir)
	SetStorage(storage)
	serializer := NewJsonSerializer((*AnEvent)(nil), (*AnotherEvent)(nil))
	SetSerializer(serializer)
}

func tearDown() {
	err := os.RemoveAll(tempDir)
	if err != nil {
		panic(err)
	}
}

func wrapEvent(aggregateId uuid.UUID, event interface{}) Event {
	return Event{aggregateId, event}
}

func TestSerializeEventToJson(t *testing.T) {
	setUp()
	defer tearDown()

	ev := wrapEvent(uuid.NewV4(), AnEvent{int64(1024), "Tests"})
	err := AddEvent(ev)
	if err != nil {
		t.Errorf("AddEvent failed with %q", err)
		return
	}

	filename := (storage.(*DiskStorage)).getFilenameForEvents(ev.AggregateId.String());
	if fi, _ := os.Stat(filename); fi == nil {
		t.Errorf("AddEvent failed to create file %q", filename)
		return
	}
	content, _ := ioutil.ReadFile(filename)
	if !bytes.Contains(content, []byte("{\"A\":1024,\"B\":\"Tests\"}")) {
		t.Errorf("AddEvent failed. File doesn't contain event json.")
		return
	}
}

func TestSerializeEventsForSameAggregateInSameFile(t *testing.T) {
	setUp()
	defer tearDown()

	aggregateId := uuid.NewV4()
	ev1 := wrapEvent(aggregateId, AnEvent{int64(12345), "Hello"})
	err := AddEvent(ev1)
	if err != nil {
		t.Errorf("AddEvent failed with %q", err)
		return
	}
	ev2 := wrapEvent(aggregateId, AnotherEvent{int64(23456), "Bob", 123.45})
	err = AddEvent(ev2)
	if err != nil {
		t.Errorf("AddEvent failed with %q", err)
		return
	}

	filename := (storage.(*DiskStorage)).getFilenameForEvents(aggregateId.String())
	content, _ := ioutil.ReadFile(filename)
	if !bytes.Contains(content, []byte("Hello")) || !bytes.Contains(content, []byte("Bob")) {
		t.Error("AddEvent failed. Both events are not serialized in same file.")
		return
	}
}

func TestTypeInformationIsProvided(t *testing.T) {
	setUp()
	defer tearDown()

	ev := wrapEvent(uuid.NewV4(), AnEvent{int64(1024), "Tests"})
	err := AddEvent(ev)
	if err != nil {
		t.Errorf("AddEvent failed with %q", err)
		return
	}

	filename := (storage.(*DiskStorage)).getFilenameForEvents(ev.AggregateId.String());
	if fi, _ := os.Stat(filename); fi == nil {
		t.Errorf("AddEvent failed to create file %q", filename)
		return
	}
	content, _ := ioutil.ReadFile(filename)
	if !bytes.Contains(content, []byte("AnEvent")) {
		t.Errorf("AddEvent failed. File doesn't contain event type.")
		return
	}
}

func (me *Event) Equals(other *Event) bool {
	return me.AggregateId == other.AggregateId && reflect.DeepEqual(me.Payload, other.Payload)
}

func TestEventsCanBeRetrieved(t *testing.T) {
	setUp()
	defer tearDown()

	aggregateId := uuid.NewV4()
	ev1 := wrapEvent(aggregateId, AnEvent{int64(12345), "Hello"})
	err := AddEvent(ev1)
	if err != nil {
		t.Errorf("AddEvent failed with %q", err)
		return
	}
	ev2 := wrapEvent(aggregateId, AnotherEvent{int64(23456), "Bob", 123.45})
	err = AddEvent(ev2)
	if err != nil {
		t.Errorf("AddEvent failed with %q", err)
		return
	}

	events, err := RetrieveFor(aggregateId)
	switch {
	case err != nil:
		t.Errorf("RetrieveFor(%q) failed with %q", aggregateId.String(), err)
	case len(events) != 2:
		t.Errorf("RetrieveFor(%q) returned %v events, expected %v", aggregateId.String(), len(events), 2)
	case !ev1.Equals(events[0]):
		t.Errorf("RetrieveFor(%q) first event doesn't match %+v != %+v", aggregateId.String(), events[0], ev1)
	case !ev2.Equals(events[1]):
		t.Errorf("RetrieveFor(%q) second event doesn't match %+v != %+v", aggregateId.String(), events[1], ev2)
	}
}

func TestEventsCanBeReplayedInOrder(t *testing.T) {
	setUp()
	defer tearDown()

	aggregateId1 := uuid.NewV4()
	aggregateId2 := uuid.NewV4()
	testEvent1 := wrapEvent(aggregateId1, AnEvent{int64(123), "Hello 1"})
	testEvent2 := wrapEvent(aggregateId2, AnEvent{int64(456), "Hello 2"})
	testEvent3 := wrapEvent(aggregateId1, AnEvent{int64(789), "Hello 3"})
	AddEvent(testEvent1)
	AddEvent(testEvent2)
	AddEvent(testEvent3)

	events, err := RetrieveAll()
	switch {
	case err != nil:
		t.Errorf("RetrieveAll failed with %q", err)
	case len(events) != 3:
		t.Errorf("RetrieveAll returned %v events, expected %v", len(events), 3)
	case !testEvent1.Equals(events[0]) || !testEvent2.Equals(events[1]) || !testEvent3.Equals(events[2]):
		t.Error("RetrieveAll returned events in wrong order.")
	}
}

/*
	Missing tests from https://gist.github.com/adymitruk/b4627b74617a37b6d949
	- GUID reversal for distribution
	- Created date stored with event
 */