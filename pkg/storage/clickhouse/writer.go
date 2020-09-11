package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go"
	pprofProfile "github.com/profefe/profefe/internal/pprof/profile"
	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/storage"
	"go.uber.org/zap"
)

const (
	sqlInsertPprofProfiles = `
		INSERT INTO pprof_profiles (
			profile_key,
			profile_type,
			external_id,
			service_name,
			created_at,
			labels.key,
			labels.value
		)
		VALUES (?, ?, ?, ?, ?, ?, ?)`

	sqlInsertPprofSamples = `
		INSERT INTO pprof_samples (
			profile_key,
			fingerprint,
			locations.func_name,
			locations.file_name,
			locations.lineno,
			values,
			values_type,
			values_unit,
			labels.key,
			labels.value
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
)

type ProfilesWriter interface {
	WriteProfile(ctx context.Context, pk ProfileKey, ptype ProfileType, createdAt time.Time, params *storage.WriteProfileParams) error
}

type SamplesWriter interface {
	WriteSamples(ctx context.Context, pk ProfileKey, samples []*pprofProfile.Sample, sampleTypes []*pprofProfile.ValueType) error
}

type beginTxer interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

func withinTx(ctx context.Context, txer beginTxer, f func(tx *sql.Tx) error) (err error) {
	tx, err := txer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			tx.Rollback()
		}
	}()

	return f(tx)
}

type ProfilesWriterWrapper func(ProfilesWriter) ProfilesWriter

type profilesWriter struct {
	logger *log.Logger
	db     *sql.DB
}

func NewProfilesWriter(logger *log.Logger, db *sql.DB) ProfilesWriter {
	return &profilesWriter{
		logger: logger,
		db:     db,
	}
}

func (pw *profilesWriter) WriteProfile(
	ctx context.Context,
	pk ProfileKey,
	ptype ProfileType,
	createdAt time.Time,
	params *storage.WriteProfileParams,
) error {
	return withinTx(ctx, pw.db, func(tx *sql.Tx) error {
		return pw.insertPprofProfiles(ctx, tx, pk, ptype, createdAt, params)
	})
}

func (pw *profilesWriter) insertPprofProfiles(
	ctx context.Context,
	tx *sql.Tx,
	pk ProfileKey,
	ptype ProfileType,
	createdAt time.Time,
	params *storage.WriteProfileParams,
) error {
	stmt, err := tx.PrepareContext(ctx, sqlInsertPprofProfiles)
	if err != nil {
		return err
	}

	ln := len(params.Labels)
	labels := make([]string, ln*2)
	for i, label := range params.Labels {
		labels[i] = label.Key
		labels[i+ln] = label.Value
		i++
	}

	args := []interface{}{
		pk,
		ptype,
		params.ExternalID,
		params.Service,
		clickhouse.DateTime(createdAt),
		clickhouse.Array(labels[:ln]),
		clickhouse.Array(labels[ln:]),
	}

	pw.logger.Debugw("insertPprofProfiles: insert profile", log.ByteString("pk", pk[:]), log.MultiLine("query", sqlInsertPprofProfiles), "args", args)

	if _, err := stmt.ExecContext(ctx, args...); err != nil {
		return fmt.Errorf("could not insert profile: %w", err)
	}

	return stmt.Close()
}

type samplesWriter struct {
	db     *sql.DB
	logger *log.Logger
}

func NewSamplesWriter(logger *log.Logger, db *sql.DB) SamplesWriter {
	return &samplesWriter{
		logger: logger,
		db:     db,
	}
}

func (sw *samplesWriter) WriteSamples(ctx context.Context, pk ProfileKey, samples []*pprofProfile.Sample, sampleTypes []*pprofProfile.ValueType) error {
	return withinTx(ctx, sw.db, func(tx *sql.Tx) error {
		return sw.insertPprofSamples(ctx, tx, pk, samples, sampleTypes)
	})
}

func (sw *samplesWriter) insertPprofSamples(ctx context.Context, tx *sql.Tx, pk ProfileKey, samples []*pprofProfile.Sample, sampleTypes []*pprofProfile.ValueType) error {
	stmt, err := tx.PrepareContext(ctx, sqlInsertPprofSamples)
	if err != nil {
		return err
	}

	args := make([]interface{}, 10) // size of the slice is from number of inserted values in the query
	args[0] = pk

	fingerprinter := samplesFingerprinterPool.Get().(*samplesFingerprinter)
	defer samplesFingerprinterPool.Put(fingerprinter)

	valueTypes := make([]string, len(sampleTypes))
	valueUnits := make([]string, len(sampleTypes))
	for i := 0; i < len(sampleTypes); i++ {
		valueTypes[i] = sampleTypes[i].Type
		valueUnits[i] = sampleTypes[i].Unit
	}

	// reusable slice buffers
	var (
		lines                                    []uint16
		locs, funcs, files, labelKeys, labelVals []string
	)
	for n, sample := range samples {
		// exit quickly on cancelled context
		if err := ctx.Err(); err != nil {
			return err
		}

		if isEmptySample(sample) {
			continue
		}

		nlocs := len(sample.Location)
		sz := nlocs * 2
		if cap(locs) < sz {
			locs = make([]string, sz, sz*2)
		} else {
			locs = locs[:sz]
		}
		if cap(lines) < nlocs {
			lines = make([]uint16, 0, nlocs)
		}

		args[1] = fingerprinter.Fingerprint(sample)

		funcs, files, lines = collectLocations(sample, locs, lines)
		args[2] = clickhouse.Array(funcs)
		args[3] = clickhouse.Array(files)
		args[4] = clickhouse.Array(lines)

		args[5] = clickhouse.Array(sample.Value)
		args[6] = clickhouse.Array(valueTypes)
		args[7] = clickhouse.Array(valueUnits)

		labelKeys, labelVals = collectLabels(sample, labelKeys, labelVals)
		args[8] = clickhouse.Array(labelKeys)
		args[9] = clickhouse.Array(labelVals)

		sw.logger.Debugw("insertPprofSamples: insert sample", log.ByteString("pk", pk[:]), log.MultiLine("query", sqlInsertPprofSamples), "args", args)

		if _, err := stmt.ExecContext(ctx, args...); err != nil {
			return fmt.Errorf("could not insert sample %d: %w", n, err)
		}
	}

	return stmt.Close()
}

func collectLocations(sample *pprofProfile.Sample, locs []string, lines []uint16) ([]string, []string, []uint16) {
	nlocs := len(sample.Location)
	if cap(locs) < nlocs*2 {
		panic(fmt.Sprintf("locs slice is under capacity: want %d, got %d", cap(locs), nlocs*2))
	}
	// point funcs and files to locs, allowing to reuse the slice
	funcs := locs[:0:nlocs]
	files := locs[nlocs:nlocs]
	lines = lines[:0]
	for _, loc := range sample.Location {
		// FIXME(narqo) always uses first location line; i.e. loses information about inlined functions
		line := loc.Line[0]
		funcs = append(funcs, line.Function.Name)
		files = append(files, line.Function.Filename)
		lines = append(lines, uint16(line.Line))
	}
	return funcs, files, lines
}

// supports only profiles string labels
func collectLabels(sample *pprofProfile.Sample, keys []string, svals []string) ([]string, []string) {
	keys = keys[:0]
	svals = svals[:0]
	for k, vv := range sample.Label {
		for _, v := range vv {
			keys = append(keys, k)
			svals = append(svals, v)
		}
	}
	return keys, svals
}

var errPoolClosed = errors.New("pool is closed")

type pooledSamplesWriter struct {
	baseCtx       context.Context
	cancelBaseCtx context.CancelFunc
	logger        *log.Logger
	sw            SamplesWriter
	jobs          chan func()
	wg            sync.WaitGroup
	closing       chan struct{}
}

func withPool(n int, logger *log.Logger, sw SamplesWriter) *pooledSamplesWriter {
	baseCtx, cancel := context.WithCancel(context.Background())
	p := &pooledSamplesWriter{
		baseCtx:       baseCtx,
		cancelBaseCtx: cancel,

		logger:  logger,
		sw:      sw,
		jobs:    make(chan func(), n),
		closing: make(chan struct{}),
	}

	p.spawnWorkers()

	return p
}

func (p *pooledSamplesWriter) spawnWorkers() {
	for n := cap(p.jobs); n > 0; n-- {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()

			for job := range p.jobs {
				job()
			}
		}()
	}
}

func (p *pooledSamplesWriter) WriteSamples(ctx context.Context, pk ProfileKey, samples []*pprofProfile.Sample, sampleTypes []*pprofProfile.ValueType) error {
	select {
	case <-p.closing:
		return errPoolClosed
	default:
	}

	job := func() {
		// job's context mustn't be bound to incoming context
		err := p.sw.WriteSamples(p.baseCtx, pk, samples, sampleTypes)
		if err != nil {
			p.logger.Errorw("pooledSamplesWriter failed to write samples", log.ByteString("pk", pk[:]), "samples_total", len(samples), zap.Error(err))
		}
	}

	select {
	case p.jobs <- job:
		return nil
	case <-p.closing:
		return errPoolClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// XXX(narqo) fix data race in pooledSamplesWriter.Close
func (p *pooledSamplesWriter) Close() error {
	close(p.closing)

	p.cancelBaseCtx()

	close(p.jobs)

	p.logger.Infow("pooledSamplesWriter: waiting jobs to finish")
	p.wg.Wait()

	return nil
}
