package serializer

import (
	"reflect"
	"encoding/json"
	"errors"
	"fmt"
)

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

func (me *JsonSerializer) Serialize(obj interface{}) ([]byte, string, error) {
	if obj == nil {
		return []byte(""), "", nil
	}
	type_ := reflect.TypeOf(obj)
	if (type_.Kind() == reflect.Interface || type_.Kind() == reflect.Ptr) {
		return nil, "", errors.New("Trying to serialize a Ptr type.")
	}
	typeId := type_.String()
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, "", err
	}
	return data, typeId, nil
}

func (me *JsonSerializer) Deserialize(serialized []byte, typeId string) (interface{}, error) {
	if (typeId == "") {
		return nil, nil
	}
	type_ := me.types[typeId]
	if type_ == nil {
		return nil, errors.New(fmt.Sprintf("type %q not registered in serializer", typeId))
	}
	objPtr := reflect.New(type_).Interface()
	err := json.Unmarshal(serialized, objPtr)
	if err != nil {
		return nil, err
	}
	obj := reflect.Indirect(reflect.ValueOf(objPtr)).Interface()
	return obj, nil
}
