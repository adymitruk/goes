package serializer

import (
	"bytes"
	"errors"
)

type PassthruSerializer struct {}

func NewPassthruSerializer() *PassthruSerializer {
	return &PassthruSerializer{}
}

func (me PassthruSerializer) Serialize(input interface{}) (output []byte, typeId string, err error) {
	content, ok := input.([]byte)
	if !ok {
		err = errors.New("input should be []byte")
		return
	}

	sep := bytes.IndexByte(content, ' ')
	if sep == -1 {
		err = errors.New("missing split char.")
		return
	}

	output = content[sep+1:]
	typeId = string(content[0:sep])
	return
}

func (me PassthruSerializer) Deserialize(input []byte, typeId string) (interface{}, error) {
	output := []byte(typeId)
	output = append(output, ' ')
	output = append(output, input...)

	return output, nil
}
