package clickhouse

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/storage"
)

const (
	sqlSelectProfiles = `SELECT %s FROM pprof_profiles PREWHERE service_name = ? %s;`

	sqlSelectServiceNames = `
		SELECT service_name
		FROM pprof_profiles
		GROUP BY service_name
		ORDER BY service_name;`
)

var selectProfilesColumns = []string{
	"profile_key", // the code below expects profile_key to be the first column
	"profile_type",
	"external_id",
	"service_name",
	"created_at",
	"labels.key",
	"labels.value",
}

func (st *Storage) FindProfiles(ctx context.Context, params *storage.FindProfilesParams) (metas []profile.Meta, err error) {
	query, args, err := buildSQLSelectProfiles(selectProfilesColumns, params)
	if err != nil {
		return nil, err
	}

	rows, err := st.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	st.logger.Debugw("findProfiles: query profiles", log.MultiLine("query", query), "args", args)

	var (
		pk        ProfileKey
		labelsKey []string
		labelsVal []string
	)
	for rows.Next() {
		var (
			meta  profile.Meta
			ptype string // clickhouse returns string value for enums
		)
		if err := rows.Scan(&pk, &ptype, &meta.ExternalID, &meta.Service, &meta.CreatedAt, &labelsKey, &labelsVal); err != nil {
			return nil, fmt.Errorf("scan rows: %w", err)
		}

		meta.ProfileID = profile.ID(pk.String())

		if err := meta.Type.FromString(ptype); err != nil {
			return nil, err
		}

		for i, key := range labelsKey {
			meta.Labels = append(meta.Labels, profile.Label{key, labelsVal[i]})
		}

		metas = append(metas, meta)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(metas) == 0 {
		return nil, storage.ErrNotFound
	}

	return metas, nil
}

func (st *Storage) FindProfileIDs(ctx context.Context, params *storage.FindProfilesParams) (pids []profile.ID, err error) {
	// profile_key is the first column in the slice
	query, args, err := buildSQLSelectProfiles(selectProfilesColumns[:1], params)
	if err != nil {
		return nil, err
	}

	rows, err := st.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	st.logger.Debugw("findProfileIDs: query profiles", log.MultiLine("query", query), "args", args)

	var pk ProfileKey
	for rows.Next() {
		if err := rows.Scan(&pk); err != nil {
			return nil, fmt.Errorf("scan rows: %w", err)
		}
		pids = append(pids, profile.ID(pk.String()))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(pids) == 0 {
		return nil, storage.ErrNotFound
	}

	return pids, nil
}

func (st *Storage) ListProfiles(ctx context.Context, pid []profile.ID) (storage.ProfileList, error) {
	return nil, storage.ErrNotImplemented
}

func (st *Storage) ListServices(ctx context.Context) (services []string, err error) {
	rows, err := st.db.QueryContext(ctx, sqlSelectServiceNames)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	st.logger.Debugw("listServices: query services", log.MultiLine("query", sqlSelectServiceNames))

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		services = append(services, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(services) == 0 {
		return nil, storage.ErrNotFound
	}

	return services, nil
}

// builds SELECT profiles SQL query and its corresponding arguments
func buildSQLSelectProfiles(columns []string, params *storage.FindProfilesParams) (string, []interface{}, error) {
	if params.Service == "" {
		return "", nil, fmt.Errorf("empty service")
	}

	if params.CreatedAtMin.IsZero() {
		return "", nil, fmt.Errorf("empty createdAtMin")
	}

	createdAtMax := params.CreatedAtMax
	if createdAtMax.IsZero() {
		createdAtMax = time.Now().UTC()
	}
	if params.CreatedAtMin.After(createdAtMax) {
		return "", nil, fmt.Errorf("createdAtMin after createdAtMax")
	}

	whereConds := make([]string, 0, 4)
	args := make([]interface{}, 1, 4)

	args[0] = params.Service

	if params.Type != profile.TypeUnknown {
		ptype, err := ProfileTypeToDBModel(params.Type)
		if err != nil {
			return "", nil, err
		}
		whereConds = append(whereConds, "(profile_type = ?)")
		args = append(args, ptype)
	}

	whereConds = append(whereConds, "(created_at >= ?)")
	args = append(args, params.CreatedAtMin)

	whereConds = append(whereConds, "(created_at < ?)")
	args = append(args, createdAtMax)

	if len(params.Labels) > 0 {
		// AND hasAll(arrayZip(labels.key, labels.value), [('host', 'backend-1'), ('region', 'eu')])
		labels := make([]string, 0, len(params.Labels))
		for _, label := range params.Labels {
			labels = append(labels, "(?, ?)")
			args = append(args, label.Key, label.Value)
		}
		whereConds = append(whereConds, fmt.Sprintf("hasAll(arrayZip(labels.key, labels.value), [%s])", strings.Join(labels, ",")))
	}

	conds := make([]string, 0, 3)
	if len(whereConds) > 0 {
		conds = append(conds, "WHERE "+strings.Join(whereConds, " AND "))
	}
	conds = append(conds, "ORDER BY service_name, profile_type, created_at")
	if params.Limit > 0 {
		conds = append(conds, fmt.Sprintf("LIMIT %d", params.Limit))
	}

	query := fmt.Sprintf(
		sqlSelectProfiles,
		strings.Join(columns, ","),
		strings.Join(conds, " "),
	)

	return query, args, nil
}
