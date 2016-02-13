package goes

import (
	"testing"
	"github.com/satori/go.uuid"
	"crypto/rand"
	"os"
	_ "path"
	"reflect"
	"fmt"
"math/big"
	"path"
)

var tempDir string

type MyEvent struct {
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
	SetStoragePath(tempDir)
	serializer := NewJsonSerializer((*MyEvent)(nil), (*AnotherEvent)(nil))
	SetSerializer(serializer)
}

func tearDown() {
	err := os.RemoveAll(tempDir)
	if err != nil {
		panic(err)
	}
}

func createRandomEvent() *Event {
	id := uuid.NewV4()
	return createRandomEventFor(id)
}

func createRandomEventFor(id uuid.UUID) *Event {
	a, _ := rand.Int(rand.Reader, big.NewInt(100000))
	b, _ := rand.Int(rand.Reader, big.NewInt(1000000))
	payload := MyEvent{a.Int64(), fmt.Sprintf("abc-%v", b.Int64())}
	return &Event{id, payload}
}

func TestAddEvent(t *testing.T) {
	setUp()
	defer tearDown()

	ev := createRandomEvent()
	err := AddEvent(*ev)
	if err != nil {
		t.Errorf("AddEvent failed with %q", err)
	}
}

func TestAddEventsToSameAggregate(t *testing.T) {
	setUp()
	defer tearDown()

	id := uuid.NewV4()
	ev1 := createRandomEventFor(id)
	err := AddEvent(*ev1)
	if err != nil {
		t.Errorf("AddEvent() failed with %q", err)
		return
	}
	ev2 := createRandomEventFor(id)
	err = AddEvent(*ev2)
	if err != nil {
		t.Errorf("AddEvent() failed with %q", err)
		return
	}
}

func (me *Event) Equals(other *Event) bool {
	return me.AggregateId == other.AggregateId && reflect.DeepEqual(me.Payload, other.Payload)
}

func TestRetrieveFor(t *testing.T) {
	setUp()
	defer tearDown()

	id := uuid.NewV4()
	ev1 := createRandomEventFor(id)
	ev2 := createRandomEventFor(id)
	AddEvent(*ev1)
	AddEvent(*ev2)
	AddEvent(*createRandomEvent())

	events, err := RetrieveFor(id)
	switch {
	case err != nil:
		t.Errorf("RetrieveFor(%q) failed with %q", id.String(), err)
	case len(events) != 2:
		t.Errorf("RetrieveFor(%q) returned %v events, expected %v", id.String(), len(events), 2)
	case !events[0].Equals(ev1):
		t.Errorf("RetrieveFor(%q) first event doesn't match %+v != %+v", id.String(), events[0], ev1)
	case !events[1].Equals(ev2):
		t.Errorf("RetrieveFor(%q) second event doesn't match %+v != %+v", id.String(), events[1], ev2)
	}
}

func TestRetrieveAll(t *testing.T) {
	setUp()
	defer tearDown()

	AddEvent(*createRandomEvent())
	AddEvent(*createRandomEvent())
	AddEvent(*createRandomEvent())

	events, err := RetrieveAll()
	switch {
	case err != nil:
		t.Errorf("RetrieveAll() failed with %q", err)
	case len(events) != 3:
		t.Errorf("RetrieveAll() returned %v events, expected %v", len(events), 3)
	}
}