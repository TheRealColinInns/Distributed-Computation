package job

type Executer interface {
	Execute(line_num int, line_txt string) ([]byte, []byte)
	Reduce(key []byte, values [][]byte) ([]byte, []byte)
	Results(key []byte, value []byte) string
}
