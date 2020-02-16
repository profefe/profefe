package badger

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"time"

	"github.com/cespare/xxhash"
	"github.com/dgraph-io/badger"
	pprofProfile "github.com/profefe/profefe/internal/pprof/profile"
	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/storage"
	"golang.org/x/xerrors"
)

const (
	metaPrefix    byte = 1 << 6 // 0b01000000
	profilePrefix byte = 1 << 7 // 0b10000000
)

const (
	serviceIndexID = metaPrefix | 1 + iota
	typeIndexID
	labelsIndexID
)

const (
	// see https://godoc.org/github.com/rs/xid
	sizeOfProfileID = 12

	labelSep byte = '\xff'
)

type Storage struct {
	logger *log.Logger
	db     *badger.DB
	ttl    time.Duration
	// holds the cache of stored services
	cache *cache
}

var _ storage.Reader = (*Storage)(nil)
var _ storage.Writer = (*Storage)(nil)

func New(logger *log.Logger, db *badger.DB, ttl time.Duration) *Storage {
	return &Storage{
		logger: logger,
		db:     db,
		ttl:    ttl,
		cache:  newCache(db),
	}
}

func (st *Storage) WriteProfile(ctx context.Context, meta profile.Meta, r io.Reader) error {
	var buf bytes.Buffer
	pp, err := pprofProfile.Parse(io.TeeReader(r, &buf))
	if err != nil {
		return xerrors.Errorf("could not parse profile: %w", err)
	}

	// XXX(narqo): update meta with time from parsed profile
	meta.CreatedAt = time.Unix(0, pp.TimeNanos)

	return st.writeProfileData(ctx, meta, buf.Bytes())
}

func (st *Storage) writeProfileData(ctx context.Context, meta profile.Meta, data []byte) error {
	var expiresAt uint64
	if st.ttl > 0 {
		expiresAt = uint64(time.Now().Add(st.ttl).Unix())
	}
	createdAt := meta.CreatedAt.UnixNano()

	entries := make([]*badger.Entry, 0, 1+1+2+len(meta.Labels)) // 1 for profile entry, 1 for meta entry, 2 for general indexes

	entries = append(entries, st.newBadgerEntry(createProfilePK(meta.ProfileID, createdAt), data))

	mk, mv, err := createMetaKV(meta)
	if err != nil {
		return xerrors.Errorf("could not encode meta %v: %w", meta, err)
	}
	entries = append(entries, st.newBadgerEntry(mk, mv))

	// indexes
	indexVal := make([]byte, 0, len(meta.Service)+64)

	// by-service index
	{
		indexVal = append(indexVal, meta.Service...)
		entries = append(entries, st.newBadgerEntry(createIndexKey(serviceIndexID, indexVal, meta.ProfileID, createdAt), nil))
	}

	// by-service-type index
	{
		indexVal = append(indexVal[:0], meta.Service...)
		indexVal = append(indexVal, byte(meta.Type))
		entries = append(entries, st.newBadgerEntry(createIndexKey(typeIndexID, indexVal, meta.ProfileID, createdAt), nil))
	}

	// by-labels index
	if len(meta.Labels) != 0 {
		for _, label := range meta.Labels {
			indexVal = append(indexVal[:0], meta.Service...)
			indexVal = appendLabelKV(indexVal, label.Key, label.Value)
			entries = append(entries, st.newBadgerEntry(createIndexKey(labelsIndexID, indexVal, meta.ProfileID, createdAt), nil))
		}
	}

	err = st.db.Update(func(txn *badger.Txn) error {
		for i := range entries {
			st.logger.Debugw("writeProfile: set entry", "pid", meta.ProfileID, log.ByteString("key", entries[i].Key), "expires_at", entries[i].ExpiresAt)
			if err := txn.SetEntry(entries[i]); err != nil {
				return xerrors.Errorf("could not write entry: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	st.cache.PutService(meta.Service, expiresAt)

	return nil
}

func (st *Storage) newBadgerEntry(key, val []byte) *badger.Entry {
	entry := badger.NewEntry(key, val)
	if st.ttl > 0 {
		entry = entry.WithTTL(st.ttl)
	}
	return entry
}

// profile primary key profilePrefix<pid><created-at>
func createProfilePK(pid profile.ID, createdAt int64) []byte {
	var buf bytes.Buffer
	buf.WriteByte(profilePrefix)
	buf.Write(pid)
	// special case to re-use the function for both write and read
	if createdAt != 0 {
		binary.Write(&buf, binary.BigEndian, createdAt)
	}
	return buf.Bytes()
}

// meta primary key metaPrefix<pid>, value json-encoded
func createMetaKV(meta profile.Meta) ([]byte, []byte, error) {
	key := make([]byte, 0, len(meta.ProfileID)+1)
	key = append(key, metaPrefix)
	key = append(key, meta.ProfileID...)

	val, err := json.Marshal(meta)

	return key, val, err
}

// index key <index-id><index-val><created-at><pid>
func createIndexKey(indexID byte, indexVal []byte, pid profile.ID, createdAt int64) []byte {
	var buf bytes.Buffer
	buf.WriteByte(indexID)
	buf.Write(indexVal)
	binary.Write(&buf, binary.BigEndian, createdAt)
	buf.Write(pid)
	return buf.Bytes()
}

func appendLabelKV(b []byte, key, val string) []byte {
	h := xxhash.New()
	h.WriteString(key)
	h.Write([]byte{labelSep})
	h.WriteString(val)
	return h.Sum(b)
}

func (st *Storage) ListServices(ctx context.Context) ([]string, error) {
	services := st.cache.Services()
	if len(services) == 0 {
		return nil, storage.ErrNotFound
	}
	return services, nil
}

func (st *Storage) ListProfiles(ctx context.Context, pids []profile.ID) (storage.ProfileList, error) {
	if len(pids) == 0 {
		return nil, xerrors.New("empty profile ids")
	}

	prefixes := make([][]byte, 0, len(pids))
	for _, pid := range pids {
		key := createProfilePK(pid, 0)
		st.logger.Debugw("listProfiles: create key", "pid", pid, "key", key)
		prefixes = append(prefixes, key)
	}

	txn := st.db.NewTransaction(false)

	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10

	pl := &ProfileList{
		txn:      txn,
		it:       txn.NewIterator(opts),
		logger:   st.logger,
		prefixes: prefixes,
	}
	return pl, nil
}

type ProfileList struct {
	txn    *badger.Txn
	it     *badger.Iterator
	logger *log.Logger

	prefixes [][]byte
	prefix   []byte
	nPrefix  int

	err error
}

func (pl *ProfileList) Next() bool {
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
			pl.logger.Debugw("next", log.ByteString("prefix", pl.prefix), "valid", valid)
			return true
		}
		pl.prefix = nil
	}
	return false
}

func (pl *ProfileList) Profile() (*pprofProfile.Profile, error) {
	var pp *pprofProfile.Profile
	err := pl.it.Item().Value(func(val []byte) (err error) {
		pp, err = pprofProfile.ParseData(val)
		return err
	})
	if err != nil {
		pl.setErr(err)
	}
	return pp, err
}

func (pl *ProfileList) Close() error {
	pl.it.Close()
	pl.txn.Discard()
	return pl.err
}

func (pl *ProfileList) setErr(err error) {
	if pl.err == nil {
		pl.err = err
	}
}

func (st *Storage) FindProfiles(ctx context.Context, params *storage.FindProfilesParams) ([]profile.Meta, error) {
	pids, err := st.FindProfileIDs(ctx, params)
	if err != nil {
		return nil, err
	}

	prefixes := make([][]byte, 0, len(pids))
	for _, pid := range pids {
		key := append(make([]byte, 0, 1+len(pid)), metaPrefix)
		key = append(key, pid...)
		st.logger.Debugw("findProfiles: create key", "pid", pid, log.ByteString("key", key))
		prefixes = append(prefixes, key)
	}

	metas := make([]profile.Meta, 0, len(pids))

	err = st.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		for _, prefix := range prefixes {
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				var meta profile.Meta
				err := it.Item().Value(func(val []byte) error {
					return json.Unmarshal(val, &meta)
				})
				if err != nil {
					return err
				}
				metas = append(metas, meta)
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}
	if len(metas) == 0 {
		return nil, storage.ErrNotFound
	}

	return metas, nil
}

func (st *Storage) FindProfileIDs(ctx context.Context, params *storage.FindProfilesParams) ([]profile.ID, error) {
	if params.Service == "" {
		return nil, xerrors.New("empty service")
	}

	if params.CreatedAtMin.IsZero() {
		return nil, xerrors.New("empty created_at")
	}

	createdAtMax := params.CreatedAtMax
	if createdAtMax.IsZero() {
		createdAtMax = time.Now().UTC()
	}

	indexesToScan := make([][]byte, 0, 1)
	{
		indexKey := make([]byte, 0, 64)
		if params.Type != profile.TypeUnknown {
			// by-service-type
			indexKey = append(indexKey, typeIndexID)
			indexKey = append(indexKey, params.Service...)
			indexKey = append(indexKey, byte(params.Type))
		} else {
			// by-service
			indexKey = append(indexKey, serviceIndexID)
			indexKey = append(indexKey, params.Service...)
		}

		indexesToScan = append(indexesToScan, indexKey)

		// by-service-labels
		if len(params.Labels) != 0 {
			for _, label := range params.Labels {
				indexKey := make([]byte, 0, 2+len(params.Service)+len(label.Key)+len(label.Value))
				indexKey = append(indexKey, labelsIndexID)
				indexKey = append(indexKey, params.Service...)
				indexKey = appendLabelKV(indexKey, label.Key, label.Value)
				indexesToScan = append(indexesToScan, indexKey)
			}
		}
	}

	// slice of slice of raw ids
	ids := make([][][]byte, 0, len(indexesToScan))

	// scan prepared indexes
	for i, s := range indexesToScan {
		keys, err := st.scanIndexKeys(s, params.CreatedAtMin, createdAtMax)
		if err != nil {
			return nil, err
		}

		ids = append(ids, make([][]byte, 0, len(keys)))
		for _, k := range keys {
			id := k[len(k)-sizeOfProfileID:] // extract profileID part from the key
			ids[i] = append(ids[i], id)
		}
	}

	if len(ids) == 0 {
		return nil, storage.ErrNotFound
	}

	pids := mergeJoinIDs(ids, params)
	if len(pids) == 0 {
		return nil, storage.ErrNotFound
	}
	return pids, nil
}

func (st *Storage) scanIndexKeys(indexKey []byte, createdAtMin, createdAtMax time.Time) (keys [][]byte, err error) {
	createdAtBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(createdAtBytes, uint64(createdAtMin.UnixNano()))

	err = st.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // keys-only iteration

		// key to start scan from
		key := append([]byte{}, indexKey...)
		key = append(key, createdAtBytes...)

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(key); scanIteratorValid(it, indexKey, createdAtMax.UnixNano()); it.Next() {
			item := it.Item()

			// check if item's key chunk before the timestamp is equal to indexKey
			tsStartPos := len(it.Item().Key()) - sizeOfProfileID - 8
			if bytes.Equal(indexKey, item.Key()[:tsStartPos]) {
				var key []byte
				key = item.KeyCopy(key)
				st.logger.Debugw("scanIndexKeys found", log.ByteString("key", key))
				keys = append(keys, key)
			}
		}
		return nil
	})

	return keys, err
}

func scanIteratorValid(it *badger.Iterator, prefix []byte, tsMax int64) bool {
	if !it.ValidForPrefix(prefix) {
		return false
	}

	// parse created-at from item's key
	tsPos := len(it.Item().Key()) - sizeOfProfileID - 8 // 8 is for created-at nanos
	ts := binary.BigEndian.Uint64(it.Item().Key()[tsPos:])

	return ts <= uint64(tsMax)
}

// does merge part of sort-merge join of N lists of ids
func mergeJoinIDs(ids [][][]byte, params *storage.FindProfilesParams) []profile.ID {
	mergedIDs := ids[0]

	if len(ids) > 1 {
		for i := 1; i < len(ids); i++ {
			mergedCap := len(mergedIDs)
			if mergedCap > len(ids[i]) {
				mergedCap = len(ids[i])
			}

			merged := make([][]byte, 0, mergedCap)

			for l, r := 0, 0; l < len(mergedIDs) && r < len(ids[i]); {
				switch bytes.Compare(mergedIDs[l], ids[i][r]) {
				case 0:
					// left == right
					merged = append(merged, mergedIDs[l])
					l++
					r++
				case 1:
					// left > right
					r++
				case -1:
					// left < right
					l++
				}
			}
			mergedIDs = merged
		}
	}

	// by this point the order of ids in mergedIDs is ASC, because of createdAt part of a key,
	// so limit from head
	if params.Limit > 0 && len(mergedIDs) > params.Limit {
		mergedIDs = mergedIDs[len(mergedIDs)-params.Limit:]
	}

	// reverse keys
	for left, right := 0, len(mergedIDs)-1; left < right; left, right = left+1, right-1 {
		mergedIDs[left], mergedIDs[right] = mergedIDs[right], mergedIDs[left]
	}

	pids := make([]profile.ID, 0, len(mergedIDs))
	for _, id := range mergedIDs {
		pids = append(pids, id)
	}
	return pids
}
