package mvcc

import (
	"bytes"
	"encoding/binary"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// Your Code Here (4A).
	// 将Write对象编码后存储在write列族中
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Key:   EncodeKey(key, ts),
			Value: write.ToBytes(),
			Cf:    engine_util.CfWrite,
		},
	})
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	// 从lock列族中读取锁信息
	lockVal, err := txn.Reader.GetCF(engine_util.CfLock, key)

	if err != nil || lockVal == nil {
		return nil, err
	}

	// 读取的value转化为lock对象
	lockObj, err := ParseLock(lockVal)

	if err != nil {
		return nil, err
	}

	return lockObj, nil
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	// 将Lock对象编码后存储在lock列族中
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Key:   key,
			Value: lock.ToBytes(),
			Cf:    engine_util.CfLock,
		},
	})
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).
	// 从lock列族中删除锁
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Delete{
			Key: key,
			Cf:  engine_util.CfLock,
		},
	})
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// Your Code Here (4A).
	// 从write列族中查找最接近但小于等于startTs的记录
	iterator := txn.Reader.IterCF(engine_util.CfWrite)
	defer iterator.Close()
	// 将迭代器定位到大于或等于目标键 target 的第一个键位置。
	// 这里doc给出提示，首先按用户键（升序），然后按时间戳（降序），确保了遍历编码键将首先给出最新版本
	iterator.Seek(EncodeKey(key, txn.StartTS))

	// iterator无效
	if !iterator.Valid() {
		return nil, nil
	}

	item := iterator.Item()
	writeKey := item.Key()
	writeTs := decodeTimestamp(writeKey)

	// 提交时间大于事务开始时间，直接返回
	if writeTs > txn.StartTS {
		return nil, nil
	}

	// 如果key值不等，直接返回
	if !bytes.Equal(key, DecodeUserKey(writeKey)) {
		return nil, nil
	}

	// 获取value
	writeVal, err := item.Value()
	if err != nil {
		return nil, err
	}

	// 转化为Object
	write, err := ParseWrite(writeVal)

	// 出错或是Delete类型而不是Put类型直接返回
	if err != nil || write.Kind == WriteKindDelete {
		return nil, err
	}

	// 从data中取出数据
	data, err := txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
	if err != nil {
		return nil, err
	}

	return data, nil
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).
	// 将数据存储在default列族中
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Key:   EncodeKey(key, txn.StartTS),
			Value: value,
			Cf:    engine_util.CfDefault,
		},
	})
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).
	// 从default列族中删除数据
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Delete{
			Key: EncodeKey(key, txn.StartTS),
			Cf:  engine_util.CfDefault,
		},
	})
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	iterator := txn.Reader.IterCF(engine_util.CfWrite)
	defer iterator.Close()

	for iterator.Seek(EncodeKey(key, TsMax)); iterator.Valid(); iterator.Next() {
		item := iterator.Item()
		writeKey := item.Key()
		writeTs := decodeTimestamp(writeKey)

		// 如果key值不等，直接返回
		if !bytes.Equal(key, DecodeUserKey(writeKey)) {
			return nil, 0, nil
		}

		// 获取value
		writeVal, err := item.Value()
		if err != nil {
			return nil, 0, err
		}

		// 转化为Object
		write, err := ParseWrite(writeVal)
		if err != nil {
			return nil, 0, err
		}

		// write.StartTS和txn.StartTS大小比较
		if write.StartTS == txn.StartTS {
			// 写入时间等于事务开始，返回正确结果
			return write, writeTs, nil
		} else if write.StartTS < txn.StartTS {
			// 如果write.StartTS开始小于事务的StartTS，没必要进行下去了
			break
		}
	}

	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	iterator := txn.Reader.IterCF(engine_util.CfWrite)
	defer iterator.Close()
	iterator.Seek(EncodeKey(key, TsMax))

	// iterator无效
	if !iterator.Valid() {
		return nil, 0, nil
	}

	item := iterator.Item()
	writeKey := item.Key()
	writeTs := decodeTimestamp(writeKey)

	// 如果key值不等，直接返回
	if !bytes.Equal(key, DecodeUserKey(writeKey)) {
		return nil, 0, nil
	}

	// 获取value
	writeVal, err := item.Value()
	if err != nil {
		return nil, 0, err
	}

	// 转化为Object
	write, err := ParseWrite(writeVal)
	if err != nil {
		return nil, 0, err
	}

	return write, writeTs, nil
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
