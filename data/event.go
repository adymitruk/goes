package data

import (
	"github.com/satori/go.uuid"
	"reflect"
	"time"
)

type Event struct {
	AggregateId 	uuid.UUID
	CreationTime 	time.Time
	Payload     	interface{}
	Metadata    	interface{}
}

func (me *Event) Equals(other *Event) bool {
	return me.AggregateId == other.AggregateId && reflect.DeepEqual(me.Payload, other.Payload)
}
