package data

import (
	"github.com/satori/go.uuid"
	"reflect"
)

type Event struct {
	AggregateId uuid.UUID
	Payload     interface{}
}

func (me *Event) Equals(other *Event) bool {
	return me.AggregateId == other.AggregateId && reflect.DeepEqual(me.Payload, other.Payload)
}
