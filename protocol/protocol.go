package protocol

var (
	Aux       DataType = "Aux" // 辅助数据
	SelectDB  DataType = "SelectDB"
	Key       DataType = "Key"
	String    DataType = "String"
	Hash      DataType = "Hash"
	Set       DataType = "Set"
	SortedSet DataType = "SortedSet"
	List      DataType = "List"
	Stream    DataType = "Stream"
)

type DataType string

type Parser interface {
	Parse() error
}

type TypeObject interface {
	String() string       // Print string
	Type() DataType       // Redis data type
	ConcreteSize() uint64 // Data bytes size, except metadata
}
