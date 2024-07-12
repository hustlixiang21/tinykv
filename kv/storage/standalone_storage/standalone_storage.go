package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"log"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB // 使用 Badger 作为底层的存储引擎
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath
	db, err := badger.Open(opts) // 打开 Badger 数据库

	if err != nil {
		// 打开数据库失败
		log.Fatal("Failed to open Badger database: ", err)
	}
	return &StandAloneStorage{db: db}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil // 独立存储引擎无需额外启动步骤，直接返回 nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close() // 关闭 Badger 数据库
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(false)
	return &StandaloneStorageReader{txn: txn}, nil
}

// StandaloneStorageReader 实现了 StorageReader 接口
type StandaloneStorageReader struct {
	txn *badger.Txn
}

// GetCF 从指定列族中获取键的值
func (r *StandaloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

// IterCF 返回一个支持迭代指定列族的迭代器
func (r *StandaloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

// Close 关闭事务
func (r *StandaloneStorageReader) Close() {
	r.txn.Discard()
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	return s.db.Update(func(txn *badger.Txn) error {
		for _, m := range batch {
			switch m.Data.(type) {
			case storage.Put:
				key := engine_util.KeyWithCF(m.Cf(), m.Key())
				err := txn.Set(key, m.Value())
				if err != nil {
					return err
				}
			case storage.Delete:
				key := engine_util.KeyWithCF(m.Cf(), m.Key())
				err := txn.Delete(key)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}
