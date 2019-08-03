package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/lib/pq"
	"github.com/profefe/profefe/internal/pprof/profile"
	"github.com/profefe/profefe/pkg/log"
	"golang.org/x/xerrors"
)

const (
	sqlCreateLocationsTempTable = `
		CREATE TEMPORARY TABLE IF NOT EXISTS pprof_locations_tmp (location jsonb) ON COMMIT DELETE ROWS;`

	sqlInsertFunctions = `
		WITH tmp_functions AS (
			SELECT t."Name" AS func_name, t."Filename" AS file_name
			FROM pprof_locations_tmp,
  				jsonb_array_elements(location -> 'Line') AS lines,
  				jsonb_to_record(lines -> 'Function') AS t ("Name" text, "Filename" text)
		)
		INSERT INTO pprof_functions (func_name, file_name)
		SELECT tmp.func_name, tmp.file_name 
		FROM tmp_functions tmp
		LEFT JOIN pprof_functions f
			ON f.func_name = tmp.func_name AND f.file_name = tmp.file_name
		WHERE f.func_name IS NULL
		ON CONFLICT (func_name, file_name) DO NOTHING;`

	sqlInsertMappings = `
		INSERT INTO pprof_mappings (mapping) 
		SELECT location -> 'Mapping' AS mapping
		FROM pprof_locations_tmp tmp
		LEFT JOIN pprof_mappings m ON m.mapping = tmp.location -> 'Mapping'
		WHERE m.mapping IS NULL
		ON CONFLICT (mapping) DO NOTHING;`

	sqlInsertLocations = `
		WITH tmp_locations AS (
			SELECT
				(tmp.location ->> 'ID')::int AS lid,
				(tmp.location ->> 'Address')::bigint AS address,
				tmp.location -> 'Mapping' AS mapping,
				array_agg(ROW(f.func_id, lines ->> 'Line')::pprof_frame) AS lines

			FROM pprof_locations_tmp tmp,
				jsonb_array_elements(tmp.location -> 'Line') AS lines
			INNER JOIN pprof_functions f
				ON f.func_name = lines -> 'Function' ->> 'Name'
		    	AND f.file_name = lines -> 'Function' ->> 'Filename'
			GROUP BY lid, address, mapping
		)
		INSERT INTO pprof_locations (mapping_id, address, lines)
		SELECT mapping_id, address, lines
		FROM tmp_locations tmp
		INNER JOIN pprof_mappings m ON m.mapping = tmp.mapping
		ORDER BY lid
		RETURNING location_id;`

	sqlSelectLocations = `
		SELECT
			location_id,
			m.mapping_id,
			mapping,
			address,
			line.line,
			f.func_id,
			f.func_name,
			f.file_name
		FROM pprof_locations l
		LEFT JOIN unnest(lines) AS line ON TRUE
		INNER JOIN pprof_functions f
			ON f.func_id = line.func_id
		INNER JOIN pprof_mappings m
			ON m.mapping_id = l.mapping_id
		WHERE location_id = ANY($1);`
)

var sqlCopyLocations = pq.CopyIn("pprof_locations_tmp", "location")

func copyLocations(ctx context.Context, logger *log.Logger, tx *sql.Tx, locs []*profile.Location) error {
	copyStmt, err := tx.PrepareContext(ctx, sqlCopyLocations)
	if err != nil {
		return err
	}
	defer copyStmt.Close()

	defer func(t time.Time) {
		logger.Debugw("copyLocations", "query", sqlCopyLocations, "nlocs", len(locs), "time", time.Since(t))
	}(time.Now())

	type locationRec struct {
		ID      uint64
		Line    []profile.Line
		Mapping *Mapping
		Address uint64
	}

	for _, loc := range locs {
		mapping := &Mapping{
			MemStart:     loc.Mapping.Start,
			MemLimit:     loc.Mapping.Limit,
			Offset:       loc.Mapping.Offset,
			File:         loc.Mapping.File,
			BuildID:      loc.Mapping.BuildID,
			HasFunctions: loc.Mapping.HasFunctions,
		}

		locRec := locationRec{
			ID:      loc.ID,
			Line:    loc.Line,
			Mapping: mapping,
			Address: loc.Address,
		}
		locBytes, err := json.Marshal(locRec)
		if err != nil {
			return err
		}
		_, err = copyStmt.ExecContext(ctx, locBytes)
		if err != nil {
			return xerrors.Errorf("could not exec sql statement: %w", err)
		}
	}

	_, err = copyStmt.Exec()
	if err != nil {
		err = xerrors.Errorf("could not finalize statement: %w", err)
	}
	return err
}
