package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type badgerReader struct {
	txn *badger.Txn
}

func newBadgerReader(txn *badger.Txn) *badgerReader {
	return &badgerReader{
		txn: txn,
	}
}

func (r *badgerReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (r *badgerReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *badgerReader) Close() {
	r.txn.Discard()
}
