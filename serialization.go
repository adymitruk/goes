package goes

import (
	"reflect"
	"encoding/json"
	"errors"
	"bytes"
)

type Serializer interface {
	Serialize(interface{}) ([]byte, error)
	Deserialize([]byte) (interface{}, error)
}

//TODO: any serializer will require a type registry maybe this should be abstracted
type JsonSerializer struct {
	types map[string]reflect.Type
}

func NewJsonSerializer(types ...interface{}) *JsonSerializer {
	s := &JsonSerializer{make(map[string]reflect.Type)}
	for _, t := range types {
		s.RegisterType(t)
	}
	return s
}

func (me *JsonSerializer) RegisterType(t interface{}) {
	type_ := reflect.TypeOf(t)
	if type_.Kind() == reflect.Ptr || type_.Kind() == reflect.Interface {
		type_ = type_.Elem()
	}
	me.types[type_.String()] = type_
}

func (me *JsonSerializer) Serialize(obj interface{}) ([]byte, error) {
	type_ := reflect.TypeOf(obj)
	if (type_.Kind() == reflect.Interface || type_.Kind() == reflect.Ptr) {
		return nil, errors.New("Trying to serialize a Ptr type.")
	}
	typeId := type_.String()
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	return []byte(typeId + " " + string(data)), nil
}

func (me *JsonSerializer) Deserialize(serialized []byte) (interface{}, error) {
	separatorIndex := bytes.Index(serialized, []byte{' '})
	if separatorIndex < 0 {
		return nil, errors.New("invalid serialized data")
	}
	typeId := string(serialized[0:separatorIndex])
	type_ := me.types[typeId]
	if type_ == nil {
		return nil, errors.New("type not registered in serializer")
	}
	objPtr := reflect.New(type_).Interface()
	err := json.Unmarshal(serialized[separatorIndex:], objPtr)
	if err != nil {
		return nil, err
	}
	obj := reflect.Indirect(reflect.ValueOf(objPtr)).Interface()
	return obj, nil
}

type PassthruSerializer struct {}

func NewPassthruSerializer() Serializer {
	return &PassthruSerializer{}
}

func (me PassthruSerializer) Serialize(obj interface{}) ([]byte, error) {
	serialized, ok := obj.([]byte)
	if !ok {
		return nil, errors.New("Object is not a slice of bytes")
	}
	return serialized, nil
}

func (me PassthruSerializer) Deserialize(serialized []byte) (interface{}, error) {
	return serialized, nil
}
