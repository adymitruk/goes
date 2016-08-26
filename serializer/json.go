package serializer

import (
	"reflect"
	"encoding/json"
	"errors"
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
	type_ := me.types[typeId]
	if type_ == nil {
		return nil, errors.New("type not registered in serializer")
	}
	objPtr := reflect.New(type_).Interface()
	err := json.Unmarshal(serialized, objPtr)
	if err != nil {
		return nil, err
	}
	obj := reflect.Indirect(reflect.ValueOf(objPtr)).Interface()
	return obj, nil
}
