package rdb

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/8090Lambert/go-redis-parser/protocol"
	"io"
	"strconv"
)

type RedisStream struct {
	Field   KeyObject
	Entries map[string]interface{} `json:"entries"`
	Length  uint64                 `json:"length"`
	LastId  StreamId               `json:"last_id"`
	Groups  []StreamGroup          `json:"groups"`
}

type StreamEntries struct {
	Length uint64                 `json:"length"`
	Data   map[string]interface{} `json:"data"`
}

type StreamId struct {
	Ms       uint64 `json:"ms"`
	Sequence uint64 `json:"sequence"`
}

type StreamGroup struct {
	Name             string                 `json:"group_name"`
	LastId           string                 `json:"last_id"`
	PendingEntryList map[string]interface{} `json:"pending"`
	Consumers        []StreamConsumer       `json:"consumers"`
}

type StreamConsumer struct {
	SeenTime         uint64                 `json:"seen_time"`
	Name             string                 `json:"consumer_name"`
	PendingEntryList map[string]interface{} `json:"pending"`
}

type StreamNACK struct {
	Consumer      StreamConsumer `json:"consumer"`
	DeliveryTime  uint64         `json:"delivery_time"`
	DeliveryCount uint64         `json:"delivery_count"`
}

func (r *ParseRdb) loadStreamListPack(key KeyObject) error {
	// Stream entry
	entries, err := r.loadStreamEntry()
	if err != nil {
		return nil
	}

	length, _, _ := r.loadLen()
	ms, _, _ := r.loadLen()
	seq, _, _ := r.loadLen()
	lastId := StreamId{Ms: ms, Sequence: seq}
	stream := RedisStream{Field: key, LastId: lastId, Length: length, Entries: nil, Groups: nil}
	if len(entries) > 0 {
		stream.Entries = entries
	}
	//Stream group
	groups, err := r.loadStreamGroup()
	if len(groups) > 0 {
		stream.Groups = groups
	}
	//r.d1 = append(r.d1, stream.String())
	r.d1 = append(r.d1, stream)

	return nil
}

func (r *ParseRdb) loadStreamEntry() (map[string]interface{}, error) {
	entryLength, _, err := r.loadLen()
	if err != nil {
		return nil, err
	}

	entries := make(map[string]interface{}, entryLength)
	for i := uint64(0); i < entryLength; i++ {
		streamAuxBytes, err := r.loadString()
		if err != nil {
			return nil, err
		}
		header := newInput(streamAuxBytes)
		msBytes, err := header.Slice(8) // ms
		if err != nil {
			return nil, err
		}
		seqBytes, err := header.Slice(8) // seq
		if err != nil {
			return nil, err
		}
		streamId := StreamId{Ms: binary.BigEndian.Uint64(msBytes), Sequence: binary.BigEndian.Uint64(seqBytes)}
		messageId := streamId.String()

		headerBytes, err := r.loadString()
		lp := newInput(headerBytes)
		// Skip the header.
		// 4b total-bytes + 2b num-elements
		lp.Seek(6, 1)

		entry, err := loadStreamEntryItem(lp, streamId)
		if err != nil {
			return nil, err
		}
		entries[messageId] = entry
	}

	return entries, nil
}

func (r *ParseRdb) loadStreamGroup() ([]StreamGroup, error) {
	/*Redis group, struct is this
	typedef struct streamCG {
	 	streamID last_id
		rax *pel
		rax *consumers;
	}*/
	groupCount, _, err := r.loadLen()
	if err != nil {
		return nil, err
	}

	groups := make([]StreamGroup, 0, groupCount)
	for i := uint64(0); i < groupCount; i++ {
		gName, err := r.loadString()
		if err != nil {
			return nil, err
		}
		ms, _, _ := r.loadLen()
		seq, _, _ := r.loadLen()
		// GroupLastId
		lastId := StreamId{Ms: ms, Sequence: seq}
		group := StreamGroup{Name: string(gName), LastId: lastId.String()}

		// Global PendingEntryList
		pel, _, _ := r.loadLen()
		groupPendingEntries := make(map[string]interface{}, pel)
		for i := uint64(0); i < pel; i++ {
			io.ReadFull(r.handler, buff)
			msBytes := buff
			rawIdObj := StreamId{Ms: binary.BigEndian.Uint64(msBytes)}
			io.ReadFull(r.handler, buff)
			seqBytes := buff
			rawIdObj.Sequence = binary.BigEndian.Uint64(seqBytes)
			rawId := rawIdObj.String()

			io.ReadFull(r.handler, buff)
			deliveryTime := uint64(binary.LittleEndian.Uint64(buff))
			deliveryCount, _, _ := r.loadLen()
			// This pending message not acknowledged, it will in consumer group
			groupPendingEntries[rawId] = StreamNACK{DeliveryTime: deliveryTime, DeliveryCount: deliveryCount}
		}

		// Consumer
		consumerCount, _, _ := r.loadLen()
		consumers := make([]StreamConsumer, 0, consumerCount)
		for i := uint64(0); i < consumerCount; i++ {
			cName, err := r.loadString()
			if err != nil {
				return nil, err
			}
			io.ReadFull(r.handler, buff)
			seenTime := uint64(binary.LittleEndian.Uint64(buff))
			consumer := StreamConsumer{Name: string(cName), SeenTime: seenTime}

			// Consumer PendingEntryList
			pel, _, _ := r.loadLen()
			consumersPendingEntries := make(map[string]interface{}, pel)
			for i := uint64(0); i < pel; i++ {
				io.ReadFull(r.handler, buff)
				msBytes := buff
				rawIdObj := StreamId{Ms: binary.BigEndian.Uint64(msBytes)}
				io.ReadFull(r.handler, buff)
				seqBytes := buff
				rawIdObj.Sequence = binary.BigEndian.Uint64(seqBytes)
				rawId := rawIdObj.String()

				// NoAck pending message
				if _, ok := groupPendingEntries[rawId].(StreamNACK); !ok {
					return nil, errors.New("NoACK pending message type unknown")
				}
				streamNoAckEntry := groupPendingEntries[rawId].(StreamNACK)
				streamNoAckEntry.Consumer = consumer
				consumersPendingEntries[rawId] = streamNoAckEntry
			}
			if len(consumersPendingEntries) > 0 {
				consumer.PendingEntryList = consumersPendingEntries
			}
			consumers = append(consumers, consumer)
		}
		if len(groupPendingEntries) > 0 {
			group.PendingEntryList = groupPendingEntries
		}
		if len(consumers) > 0 {
			group.Consumers = consumers
		}
		groups = append(groups, group)
	}

	return groups, nil
}

func loadStreamEntryItem(lp *input, stId StreamId) (entries map[string]interface{}, err error) {
	// Entry format:
	// | count | deleted | num-fields | field_1 | field_2 | ... | field_N |0|
	countBytes, err := loadStreamListPackEntry(lp)
	if err != nil {
		return nil, err
	}

	count, _ := strconv.ParseUint(string(countBytes), 10, 64)
	deletedBytes, err := loadStreamListPackEntry(lp)
	if err != nil {
		return nil, err
	}
	deleted, _ := strconv.ParseInt(string(deletedBytes), 10, 64)

	fieldsNumBytes, err := loadStreamListPackEntry(lp)
	if err != nil {
		return nil, err
	}
	fieldsNum, _ := strconv.ParseUint(string(fieldsNumBytes), 10, 64)
	fieldCollect := make([][]byte, 0, fieldsNum)
	for i := uint64(0); i < fieldsNum; i++ {
		tmp, err := loadStreamListPackEntry(lp)
		if err != nil {
			return nil, err
		}
		fieldCollect = append(fieldCollect, tmp)
	}
	loadStreamListPackEntry(lp)

	total := uint64(count) + uint64(deleted)
	entries = make(map[string]interface{}, total)
	for i := uint64(0); i < total; i++ {
		flagBytes, err := loadStreamListPackEntry(lp)
		if err != nil {
			return nil, err
		}
		flag, _ := strconv.Atoi(string(flagBytes))
		msBytes, err := loadStreamListPackEntry(lp) // ms
		if err != nil {
			return nil, err
		}
		seqBytes, err := loadStreamListPackEntry(lp) // seq
		if err != nil {
			return nil, err
		}

		ms, _ := strconv.ParseUint(string(msBytes), 10, 64)
		seq, _ := strconv.ParseUint(string(seqBytes), 10, 64)
		messageId := stId.BuildOn(ms, seq).String()

		hasDelete := "false"
		if flag&StreamItemFlagDeleted != 0 {
			hasDelete = "true"
		}
		if flag&StreamItemFlagSameFields == 0 {
			fieldsNumBytes, err := loadStreamListPackEntry(lp)
			if err != nil {
				return nil, err
			}
			fieldsNum, _ = strconv.ParseUint(string(fieldsNumBytes), 10, 64)
		}
		fields := make(map[string]interface{}, fieldsNum)
		for i := uint64(0); i < fieldsNum; i++ {
			fieldBytes := fieldCollect[i]
			if flag&StreamItemFlagSameFields == 0 {
				fieldBytes, err = loadStreamListPackEntry(lp)
				if err != nil {
					return nil, err
				}
			} else {
				fieldBytes = fieldCollect[i]
			}
			vBytes, err := loadStreamListPackEntry(lp)
			if err != nil {
				return nil, err
			}
			fields[string(fieldBytes)] = string(vBytes)
		}
		entries[messageId] = map[string]interface{}{"has_deleted": hasDelete, "fields": fields}
		loadStreamListPackEntry(lp)
	}

	if endBytes, _ := lp.ReadByte(); endBytes != 255 {
		return nil, errors.New("ListPack expect 255 with end")
	}

	return entries, nil
}

func loadStreamListPackEntry(buf *input) ([]byte, error) {
	special, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}

	res := make([]byte, 0)
	skip := 0
	if special&0x80 == 0 {
		skip = 1
		res = []byte(strconv.FormatInt(int64(special&0x7F), 10))
	} else if special&0xC0 == 0x80 {
		length := special & 0x3F
		skip = 1 + int(length)
		res, err = buf.Slice(int(length))
		if err != nil {
			return nil, err
		}
	} else if special&0xE0 == 0xC0 {
		skip = 2
		next, err := buf.ReadByte()
		if err != nil {
			return nil, err
		}
		//int64(int32(uint32(special&0x1F)<<8|uint32(next)) << 19 >> 19)
		res = []byte(strconv.FormatInt(int64(int32(uint32(special&0x1F)<<8|uint32(next))<<19>>19), 10))
	} else if special&0xFF == 0xF1 {
		skip = 3
		b, err := buf.Slice(2)
		if err != nil {
			return nil, err
		}
		res = []byte(strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(b))), 10))
	} else if special&0xFF == 0xF2 {
		skip = 4
		intBytes := make([]byte, 4)
		_, err := buf.Read(intBytes[1:])
		if err != nil {
			return nil, err
		}
		res = []byte(strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))>>8), 10))
	} else if special&0xFF == 0xF3 {
		skip = 5
		intBytes, err := buf.Slice(4)
		if err != nil {
			return nil, err
		}
		res = []byte(strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))), 10))
	} else if special&0xFF == 0xF4 {
		skip = 9
		intBytes, err := buf.Slice(8)
		if err != nil {
			return nil, err
		}
		res = []byte(strconv.FormatInt(int64(binary.LittleEndian.Uint64(intBytes)), 10))
	} else if special&0xF0 == 0xE0 {
		b, err := buf.ReadByte()
		if err != nil {
			return nil, err
		}
		length := ((special & 0x0F) << 8) | b
		skip = 2 + int(length)
		res, err = buf.Slice(int(length))
		if err != nil {
			return nil, err
		}
	} else if special&0xFF == 0xf0 {
		lenBytes, err := buf.Slice(4)
		if err != nil {
			return nil, err
		}
		length := uint64(binary.BigEndian.Uint32(lenBytes))
		skip = 5 + int(length)
		res, err = buf.Slice(int(length))
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errors.New("Unknown encoding type! ")
	}

	// element-total-len
	if skip <= 127 {
		buf.Seek(1, 1)
	} else if skip < 16383 {
		buf.Seek(2, 1)
	} else if skip < 2097151 {
		buf.Seek(3, 1)
	} else if skip < 268435455 {
		buf.Seek(4, 1)
	} else {
		buf.Seek(5, 1)
	}
	return res, err
}

func (sd StreamId) String() string {
	return strconv.FormatUint(sd.Ms, 10) + "-" + strconv.FormatUint(sd.Sequence, 10)
}

func (sd StreamId) BuildOn(ms, seq uint64) StreamId {
	newMs := sd.Ms + ms
	newSequence := sd.Sequence + seq
	return StreamId{Ms: newMs, Sequence: newSequence}
}

func (rs RedisStream) Type() string {
	return protocol.Stream
}

func (rs RedisStream) String() string {
	return fmt.Sprintf("{Stream: {Field: %s, Value:%s}}", rs.Key(), rs.Value())
}

func (rs RedisStream) Key() string {
	return ToString(rs.Field)
}

func (rs RedisStream) Value() string {
	format := map[string]interface{}{"LastId": rs.LastId, "Length": rs.Length}
	if len(rs.Entries) > 0 {
		format["Entries"] = rs.Entries
	}
	if len(rs.Groups) > 0 {
		format["groups"] = rs.Groups
	}
	output, err := json.Marshal(format)
	if err != nil {
		return ""
	}

	return string(output)
}

func (rs RedisStream) ValueLen() uint64 {
	return uint64(len(rs.Entries))
}

func (rs RedisStream) ConcreteSize() uint64 {
	if len(rs.Entries) > 0 {
		var size uint64
		for _, item := range rs.Entries {
			if entry, ok := item.(map[string]interface{}); ok {
				for _, fields := range entry {
					collect := fields.(map[string]interface{})["fields"]
					for _, value := range collect.(map[string]interface{}) { // entry fields and values
						size += uint64(len([]byte(ToString(value))))
					}
				}
			}
		}
		return size
	}

	return 0
}
