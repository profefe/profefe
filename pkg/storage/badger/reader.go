package badger

import (
	"github.com/dgraph-io/badger"
	pprofProfile "github.com/profefe/profefe/internal/pprof/profile"
)

type ProfilesReader struct {
	txn *badger.Txn
	it  *badger.Iterator

	prefixes [][]byte
	prefix   []byte
	nPrefix  int

	pp  *pprofProfile.Profile
	buf []byte

	err error
}

func (pl *ProfilesReader) Next() bool {
	if pl.err != nil {
		return false
	}

	for pl.nPrefix < len(pl.prefixes) {
		if pl.prefix == nil {
			pl.prefix = pl.prefixes[pl.nPrefix]
			pl.nPrefix++
			pl.it.Seek(pl.prefix)
		} else {
			pl.it.Next()
		}

		valid := pl.it.ValidForPrefix(pl.prefix)
		if valid {
			err := pl.it.Item().Value(func(val []byte) (err error) {
				pl.buf = val
				pl.pp, err = pprofProfile.ParseData(pl.buf)
				return err
			})
			if err != nil {
				pl.setErr(err)
				return false
			}
			return valid
		}

		pl.prefix = nil
	}

	return false
}

func (pl *ProfilesReader) setErr(err error) {
	if pl.err == nil {
		pl.err = err
	}
}

func (pl *ProfilesReader) Profile() *pprofProfile.Profile {
	return pl.pp
}

func (pl *ProfilesReader) Bytes() []byte {
	return pl.buf
}

func (pl *ProfilesReader) Err() error {
	return pl.err
}

func (pl *ProfilesReader) Close() error {
	pl.it.Close()
	pl.txn.Discard()
	return pl.err
}
