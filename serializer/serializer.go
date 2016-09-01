package serializer

type Serializer interface {
	Serialize(interface{}) ([]byte, string, error)
	Deserialize([]byte, string) (interface{}, error)
}