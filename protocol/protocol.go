package protocol

var (
	Aux       = "AuxField" // 辅助数据
	SelectDB  = "SelectDB"
	Key       = "Key"
	String    = "String"
	Hash      = "Hash"
	Set       = "Set"
	SortedSet = "SortedSet"
	List      = "List"
	Stream    = "Stream"
)

type Parser interface {
	Parse() error
}

type TypeObject interface {
	Value() string
	Key() string          // Key
	String() string       // Print string
	Type() string         // Redis data type
	ConcreteSize() uint64 // Data bytes size, except metadata
}
