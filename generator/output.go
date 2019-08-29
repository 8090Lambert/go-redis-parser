package generator

type Output struct {
}

func (f *Output) Begin(version string) {

}

func (f *Output) AuxField(key, value []byte) {

}

func (f *Output) SelectDb(index int) {

}

func (f *Output) Set(key, value []byte, expire int) {

}

func (f *Output) HSet(key, field, value []byte) {

}

func (f *Output) SAdd(key, member []byte) {

}

func (f *Output) List(key []byte, value ...[]byte) {

}

func (f *Output) ZSet(key, value []byte, score float64) {

}
