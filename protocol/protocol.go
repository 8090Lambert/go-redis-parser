package protocol

var (
	Aux       = "AuxField" // 辅助数据
	SelectDB  = "SelectDB"
	ResizeDB  = "ResizeDB"
	Key       = "Key"
	String    = "String"
	Hash      = "Hash"
	Set       = "Set"
	SortedSet = "SortedSet"
	List      = "List"
	Stream    = "Stream"
)

type Parser interface {
	Parse()
}

type TypeObject interface {
	Value() string        // value
	ValueLen() uint64     // value length
	Key() string          // Key
	String() string       // Print string
	Type() string         // Redis data type
	ConcreteSize() uint64 // Data bytes size, except metadata
}

type Factory func(file string) Parser
