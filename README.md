<h1 align="left">go-redis-parser</h1>
<img align="right" width="130px" src="https://raw.githubusercontent.com/8090Lambert/material/master/go-redis-parser.jpg">  

This is a parser in Golang. Its characteristics are: make full use of   
the coroutine language's coroutine, write at the same time as the file 
is written, more efficient analysis.  
<br>
   
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
Supports Redis from 2.8 to 5.0, all data types except module. Including:
- String
- Hash
- List
- Set
- SortedSed
- Stream

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
| Stream | stream | {"Entries":{"1569553992318-0"... | 41 |
| Sortedset | zset | [{"Field":"a","Score":1},{"Field":"b","Score":2}] | 2 |
| Hash | h | [{"field":"a","value":"a"}] | 2 |

### BigKeys outputs
```
# Scanning the rdb file to find biggest keys

-------- summary -------

Sampled 6 keys in the keyspace!
Total key length in bytes is 17

Biggest string found 's' has 1 bytes
Biggest   hash found 'h' has 1 fields
Biggest   list found 'li' has 2 items
Biggest sortedset found 'zset' has 2 members
Biggest    set found 'set' has 2 members
Biggest stream found 'stream' has 3 entries

1 string with 1 bytes
1 hash with 1 fields
1 list with 2 items
1 sortedset with 2 members
1 set with 2 members
1 stream with 3 entries
```

