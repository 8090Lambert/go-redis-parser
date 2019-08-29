package generator

type Gen interface {
	Begin(version string)
	AuxFiled(key, value []byte)
	SelectDb(index int)
	Set(key, value []byte, expire int)
	HSet(key, field, value []byte)
	SAdd(key, member []byte)
	List(key []byte, value ...[]byte)
	ZSet(key, value []byte, score float64)
}
