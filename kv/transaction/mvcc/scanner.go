package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type Scanner struct {
	// Your Code Here (4C).
	Txn      *MvccTxn               // 事务对象，用于在读取过程中管理多版本并发控制
	Iter     engine_util.DBIterator // 数据库迭代器，用于顺序扫描数据库中的键值对
	StartKey []byte                 // 起始键，从该键开始扫描
	Valid    bool                   // 迭代器的有效状态，表示当前迭代器是否还有更多的键值对可供读取
	cache    map[string][]byte      // 存放不同的userKey，便于判断
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	return &Scanner{
		Txn:      txn,
		Iter:     iter,
		StartKey: startKey,
		Valid:    iter.Valid(),
		cache:    make(map[string][]byte),
	}
}

// Close closes the scanner and releases any resources held by it.
func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.Iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	scan.Valid = scan.Iter.Valid()
	if !scan.Valid {
		return nil, nil, nil
	}

	// 找到大于等于的第一个目标
	scan.Iter.Seek(EncodeKey(scan.StartKey, scan.Txn.StartTS))
	scan.Valid = scan.Iter.Valid()

	if !scan.Valid {
		return nil, nil, nil
	}

	item := scan.Iter.Item()
	key := item.KeyCopy(nil)
	userKey := DecodeUserKey(key)

	// 当前userKey没有符合条件的，继续调用Next
	if !bytes.Equal(userKey, scan.StartKey) {
		scan.StartKey = userKey
		return scan.Next()
	}

	// 当前userKey有符合条件的，但先找到下一个不同的userKey，跳过相同的
	for {
		scan.Iter.Next()
		scan.Valid = scan.Iter.Valid()
		if !scan.Valid {
			break
		}
		item1 := scan.Iter.Item()
		key1 := item1.KeyCopy(nil)
		userKey1 := DecodeUserKey(key1)
		if !bytes.Equal(userKey1, scan.StartKey) {
			scan.StartKey = userKey1
			break
		}
	}

	// 获取 value
	writeVal, err := item.ValueCopy(nil)
	if err != nil {
		return nil, nil, err
	}

	write, err := ParseWrite(writeVal)
	if err != nil || write == nil {
		return userKey, nil, err
	}

	value, err := scan.Txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(userKey, write.StartTS))
	if err != nil {
		return userKey, nil, err
	}

	// 返回 userKey 和 value
	return userKey, value, nil
}
