package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"time"

	pprofProfile "github.com/profefe/profefe/internal/pprof/profile"
	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/pprofutil"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/storage"
)

type Storage struct {
	logger         *log.Logger
	db             *sql.DB
	profilesWriter ProfilesWriter
	samplesWriter  SamplesWriter
}

var _ storage.Storage = (*Storage)(nil)

func NewStorage(logger *log.Logger, db *sql.DB, profilesWriter ProfilesWriter, samplesWriter SamplesWriter) (*Storage, error) {
	st := &Storage{
		logger:         logger,
		db:             db,
		profilesWriter: profilesWriter,
		samplesWriter:  samplesWriter,
	}
	return st, nil
}

func (st *Storage) WriteProfile(ctx context.Context, params *storage.WriteProfileParams, r io.Reader) (profile.Meta, error) {
	ptype, err := ProfileTypeToDBModel(params.Type)
	if err != nil {
		return profile.Meta{}, err
	}

	pp, err := pprofutil.ParseProfileFrom(r)
	if err != nil {
		return profile.Meta{}, fmt.Errorf("could not parse profile: %w", err)
	}

	createdAt := params.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Unix(0, pp.TimeNanos)
	}
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	pk := NewProfileKey(createdAt)
	createdAt = createdAt.Truncate(time.Second)

	if err := st.writeProfile(ctx, pk, ptype, createdAt, params, pp); err != nil {
		return profile.Meta{}, fmt.Errorf("could not write profile with pk %v, type %v, service %q: %w", pk, ptype, params.Service, err)
	}

	pid := pk.String()
	meta := profile.Meta{
		ProfileID:  profile.ID(pid),
		ExternalID: params.ExternalID,
		Service:    params.Service,
		Type:       params.Type,
		Labels:     params.Labels,
		CreatedAt:  createdAt,
	}
	return meta, nil
}

func (st *Storage) writeProfile(
	ctx context.Context,
	pk ProfileKey,
	ptype ProfileType,
	createdAt time.Time,
	params *storage.WriteProfileParams,
	pp *pprofProfile.Profile,
) error {
	if err := st.samplesWriter.WriteSamples(ctx, pk, pp.Sample, pp.SampleType); err != nil {
		return err
	}
	if err := st.profilesWriter.WriteProfile(ctx, pk, ptype, createdAt, params); err != nil {
		return err
	}
	return nil
}

/*
-- pprof top
select flat, cum, func
from (
    select sum(values[1])        as flat,
        `locations.func_name`[1] as func
    from pprof_samples
    group by func
) t1
join (
    select sum(values[1])     as cum,
        `locations.func_name` as func
    from pprof_samples
        array join `locations.func_name`
    group by func
) t2 using func
order by flat desc;

-- merge profiles
select
	sumForEach(values) as values,
	any(`locations.func_name`) as funcs
from pprof_samples
group by fingerprint;

-- diff profiles
select
    groupArray(values[-1]) as cpu_vals,
    arrayEnumerate(cpu_vals) as indexes,
    arrayMap(i -> cpu_vals[i] - cpu_vals[i-1], indexes) as diffs,
    any(funcs) as funcs
FROM (
    SELECT fingerprint, values, `locations.func_name` as funcs
    FROM pprof_samples
     ORDER BY profile_key)
GROUP BY fingerprint;

*/
