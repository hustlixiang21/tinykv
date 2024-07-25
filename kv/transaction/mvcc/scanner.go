package mvcc

import (
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type Scanner struct {
	// Your Code Here (4C).
	Txn      *MvccTxn               // 事务对象，用于在读取过程中管理多版本并发控制
	Iter     engine_util.DBIterator // 数据库迭代器，用于顺序扫描数据库中的键值对
	StartKey []byte                 // 起始键，从该键开始扫描
	Valid    bool                   // 迭代器的有效状态，表示当前迭代器是否还有更多的键值对可供读取
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
	return nil, nil, nil
}
