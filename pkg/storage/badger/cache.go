package badger

import (
	"sort"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
)

type cache struct {
	mu       sync.RWMutex
	services map[string]uint64
}

func newCache(db *badger.DB) *cache {
	c := &cache{
		services: make(map[string]uint64),
	}

	c.prefillServices(db)

	return c
}

func (cache *cache) prefillServices(db *badger.DB) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	return db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // keys-only iteration

		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte{serviceIndexID}

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			keyTTL := it.Item().ExpiresAt()
			service := key[1 : len(key)-sizeOfProfileID-8] // 8 is for ts-nanos
			if v, ok := cache.services[string(service)]; ok {
				if v > keyTTL {
					continue
				}
			}
			cache.services[string(service)] = keyTTL
		}
		return nil
	})
}

func (cache *cache) PutService(service string, expiresAt uint64) {
	cache.mu.Lock()
	cache.services[service] = expiresAt
	cache.mu.Unlock()
}

func (cache *cache) Services() []string {
	now := time.Now().Unix()
	services := make([]string, 0, len(cache.services))

	cache.mu.RLock()
	for s, v := range cache.services {
		if v > uint64(now) || v == 0 {
			services = append(services, s)
		} else {
			// the key has expired
			delete(cache.services, s)
		}
	}
	cache.mu.RUnlock()

	sort.Strings(services)

	return services
}
