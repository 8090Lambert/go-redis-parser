<h1 align="center"><a href="https://github.com/8090lambert/go-redis-parser">go-redis-parser</a></h1>
:blue_book: A Faster parser for Redis's Data Persistence.

```
 _____ ____        ____  _____ ____  _  ____        ____  ____  ____  ____  _____ ____
/  __//  _ \      /  __\/  __//  _ \/ \/ ___\      /  __\/  _ \/  __\/ ___\/  __//  __\
| |  _| / \|_____ |  \/||  \  | | \|| ||    \_____ |  \/|| / \||  \/||    \|  \  |  \/|
| |_//| \_/|\____\|    /|  /_ | |_/|| |\___ |\____\|  __/| |-|||    /\___ ||  /_ |    /
\____\\____/      \_/\_\\____\\____/\_/\____/      \_/   \_/ \|\_/\_\\____/\____\\_/\_\

go-redis-parser version: alpha 2019-08-24 15:24:34

Usage of:
  -o string
    	set the output directory for gen-file. (default: current directory. parser.(json|csv) will be created)

  -rdb string
    	<rdb-file-name>. For example: ./dump.rdb

  -type string
    	set the gen-file's type, support type: json、csv. (default: csv)
    	 (default "csv")
```

### Feature

#### DataType
Supports Redis from 2.8 to 5.0, all data types except module. Including:
- String
- Hash
- List
- Set
- SortedSed
- Stream

#### Others
In addition to exporting all key/values, it also looks for all types of `Bigkeys` (like `redis-cli --bigkeys`).

### Installation
`go-redis-parser` is a standard package，you can use `git` or `go get` to install.

#### via git
```
$ git clone https://github.com/8090Lambert/go-redis-parser.git
```

#### via go
```
$ go get github.com/8090Lambert/go-redis-parser
```

### Using
```
$ go run main.go -rdb <dump.rdb> -o <gen-file folder> -type <gen-file type, json or csv, default csv>
```

### Generate File
| DataType | Key | Value | Size(bytes) |
| :-----: | :-----: | :-----: | :-----: | 
| AuxField | redis-ver | 5.0.5 | 0 |
| AuxField | redis-bits | 5.0.5 | 0 |
| AuxField | ctime | 5.0.5 | 0 |
| AuxField | used-mem | 5.0.5 | 0 |
| AuxField | aof-preamble | 5.0.5 | 0 |
| SelectDB | select | 0 | 0 |
| ResizeDB | resize db | {dbSize: 6, expireSize: 0} | 0 |
| String | s | a | 1 |
| List | li | a,b | 2 |
| Set | set | b,a | 2 |
| Stream | stream | {"Entries":{"1569553992318-0"... | 20 |
| Sortedset | zset | [{"Field":"a","Score":1},{"Field":"b","Score":2}] | 10 |
| Hash | h | [{"field":"a","value":"a"}] | 2 |

