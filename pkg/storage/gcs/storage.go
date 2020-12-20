package gcs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/rs/xid"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"

	gcs "cloud.google.com/go/storage"
	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/storage"
)

// gcs objects' key prefix indicates the key's naming schema
const profefeSchema = `P0.`

const (
	defaultListObjectsLimit = 100
)

// Storage stores and loads profiles from gcs.
//
// The schema for the object key:
// schemaV.service/profile_type/digest,label1=value1,label2=value2
//
// Where
// "schemaV" indicates the naming schema that was used when the profile was stored;
// "digests" uniquely describes the profile, it also includes profiles creation time.
type Storage struct {
	logger *log.Logger
	client *gcs.Client
	bucket string
}

func NewStorage(logger *log.Logger, client *gcs.Client, gcsBucket string) *Storage {
	return &Storage{
		logger: logger,
		client: client,
		bucket: gcsBucket,
	}
}

// WriteProfile uploads the profile to gcs.
// Context can be canceled and this is safe for multiple goroutines.
func (st *Storage) WriteProfile(ctx context.Context, params *storage.WriteProfileParams, r io.Reader) (profile.Meta, error) {
	createdAt := params.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	key := createProfileKey(params.Service, params.Type, createdAt, params.Labels)

	wc := st.client.Bucket(st.bucket).Object(key).NewWriter(ctx)
	if _, err := io.Copy(wc, r); err != nil {
		return profile.Meta{}, fmt.Errorf("io.Copy: %v", err)
	}
	if err := wc.Close(); err != nil {
		return profile.Meta{}, fmt.Errorf("wc.Close: %v", err)
	}

	meta := profile.Meta{
		ProfileID: profile.ID(key),
		Service:   params.Service,
		Type:      params.Type,
		Labels:    params.Labels,
		CreatedAt: createdAt,
	}

	st.logger.Debugw("writeProfile: gcs upload", "pid", meta.ProfileID, "key", key, "meta", meta)

	return meta, nil
}

func (st *Storage) ListProfiles(ctx context.Context, pids []profile.ID) (storage.ProfileList, error) {
	if len(pids) == 0 {
		return nil, fmt.Errorf("empty profile ids")
	}

	pl := &profileList{
		ctx:    ctx,
		pids:   pids,
		getter: st.getObject,
	}
	return pl, nil
}

type profileList struct {
	ctx  context.Context
	pids []profile.ID
	// points to the current key in the iteration
	key    string
	getter func(ctx context.Context, key string) (io.Reader, error)
	// first error preserved and always returned
	err error
}

func (pl *profileList) Next() (n bool) {
	if pl.err != nil {
		return false
	}

	if err := pl.ctx.Err(); err != nil {
		pl.setErr(err)
		return false
	}

	if len(pl.pids) == 0 {
		return false
	}

	pl.key, pl.pids = string(pl.pids[0]), pl.pids[1:]

	return true
}

func (pl *profileList) Profile() (io.Reader, error) {
	if err := pl.ctx.Err(); err != nil {
		return nil, err
	}

	if pl.err != nil {
		return nil, pl.err
	}

	if pl.key == "" {
		// this must never happen
		panic("gcs profileList: profile out of range")
	}

	reader, err := pl.getter(pl.ctx, pl.key)
	if err != nil {
		pl.setErr(err)
		return nil, err
	}

	return reader, nil
}

func (pl *profileList) Close() error {
	// prevent any use of this list's Profile or Next fn
	pl.err = fmt.Errorf("profile list closed")
	return nil
}

func (pl *profileList) setErr(err error) {
	if pl.err == nil {
		pl.err = err
	}
}

// ListServices returns the list of distinct services for which profiles are stored in the bucket.
func (st *Storage) ListServices(ctx context.Context) ([]string, error) {
	query := &gcs.Query{
		Prefix:    profefeSchema,
		Delimiter: "/",
	}
	var services []string
	it := st.client.Bucket(st.bucket).Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("it.Next: %v", err)
		}
		s := strings.TrimSuffix(strings.TrimPrefix(attrs.Prefix, profefeSchema), "/")
		if s != "" {
			services = append(services, s)
		}
	}
	if len(services) == 0 {
		return nil, storage.ErrNotFound
	}

	return services, nil
}

// FindProfiles queries gcs for profile metas matched searched criteria.
func (st *Storage) FindProfiles(ctx context.Context, params *storage.FindProfilesParams) ([]profile.Meta, error) {
	return st.findProfiles(ctx, params)
}

// FindProfileIDs queries gcs for profile IDs matched searched criteria.
func (st *Storage) FindProfileIDs(ctx context.Context, params *storage.FindProfilesParams) ([]profile.ID, error) {
	metas, err := st.findProfiles(ctx, params)
	if err != nil {
		return nil, err
	}

	ids := make([]profile.ID, len(metas))
	for i := range metas {
		ids[i] = metas[i].ProfileID
	}
	return ids, nil
}

func (st *Storage) findProfiles(ctx context.Context, params *storage.FindProfilesParams) ([]profile.Meta, error) {
	if params.Service == "" {
		return nil, fmt.Errorf("empty service")
	}

	if params.CreatedAtMin.IsZero() {
		return nil, fmt.Errorf("empty created_at min")
	}

	createdAtMax := params.CreatedAtMax
	if createdAtMax.IsZero() {
		createdAtMax = time.Now().UTC()
	}
	if params.CreatedAtMin.After(createdAtMax) {
		createdAtMax = params.CreatedAtMin
	}

	limit := params.Limit
	if limit == 0 {
		limit = defaultListObjectsLimit
	}

	prefix := profileKeyPrefix(params.Service)
	if params.Type != profile.TypeUnknown {
		prefix += strconv.Itoa(int(params.Type)) + "/"
	}

	query := &gcs.Query{
		Prefix: prefix,
	}
	err := query.SetAttrSelection([]string{"Name"})
	if err != nil {
		return nil, fmt.Errorf("query.SetAttrSelection: %v", err)
	}

	st.logger.Debugw("findProfiles: gcs list objects", "query", query)

	it := st.client.Bucket(st.bucket).Objects(ctx, query)

	var metas []profile.Meta
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("it.Next: %v", err)
		}
		if attrs.Name == "" {
			st.logger.Debugw("findProfiles: gcs list objects, empty object key")
			continue
		}

		meta, err := metaFromProfileKey(profefeSchema, attrs.Name)
		if err != nil {
			st.logger.Errorw("storage gcs failed to parse profile meta from object key", "key", attrs.Name, zap.Error(err))
			continue
		}

		if meta.CreatedAt.Before(params.CreatedAtMin) {
			continue
		}

		if meta.CreatedAt.After(createdAtMax) {
			break
		}

		if !meta.Labels.Include(params.Labels) {
			st.logger.Debugw("findProfiles: gcs list objects, labels mismatch", "left", meta.Labels, "right", params.Labels)
			continue
		}

		metas = append(metas, meta)
		if len(metas) >= limit {
			metas = metas[:limit]
			break
		}
	}

	if len(metas) == 0 {
		return nil, storage.ErrNotFound
	}

	return metas, nil
}

// getObject downloads a value from a key. Context can be canceled.
// This is safe for multiple go routines.
func (st *Storage) getObject(ctx context.Context, key string) (io.Reader, error) {
	reader, err := st.client.Bucket(st.bucket).Object(key).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("gcs NewReader: %v", err)
	}

	st.logger.Debugw("gcs got object", "bucket", st.bucket, "key", key)

	return reader, nil
}

func createProfileKey(service string, ptype profile.ProfileType, createdAt time.Time, labels profile.Labels) string {
	var buf bytes.Buffer
	buf.WriteString(profileKeyPrefix(service))
	buf.WriteString(strconv.Itoa(int(ptype)))
	buf.WriteByte('/')

	digest := xid.NewWithTime(createdAt)
	buf.WriteString(digest.String())
	if labels.Len() != 0 {
		buf.WriteByte(',')
		labels.EncodeTo(&buf)
	}

	return buf.String()
}

func profileKeyPrefix(service string) string {
	service = strings.ReplaceAll(service, "/", "__")
	return profefeSchema + service + "/"
}

// parses the gcs key by splitting by / to create a profile.Meta.
// The format of the key is:
// schemaV.service/profile_type/digest,label1,label2
func metaFromProfileKey(schemaV, key string) (meta profile.Meta, err error) {
	if !strings.HasPrefix(key, schemaV) {
		return meta, fmt.Errorf("invalid key format %q: schema version mismatch, want %s", key, schemaV)
	}

	// create profile ID from the original object's key
	pid := profile.ID(key)

	key = strings.TrimPrefix(key, schemaV)
	ks := strings.SplitN(key, "/", 3)
	if len(ks) != 3 {
		return meta, fmt.Errorf("invalid key format %q", key)
	}

	service, pt, tail := ks[0], ks[1], ks[2]

	v, _ := strconv.Atoi(pt)
	ptype := profile.ProfileType(v)
	if ptype == profile.TypeUnknown {
		return profile.Meta{}, fmt.Errorf("could not parse profile type %q, key %q", pt, key)
	}

	ks = strings.SplitN(tail, ",", 2)
	var dgst, lbls string
	if len(ks) == 1 {
		dgst = ks[0]
	} else {
		dgst, lbls = ks[0], ks[1]
	}

	digest, err := xid.FromString(dgst)
	if err != nil {
		return meta, fmt.Errorf("could not parse digest, key %q: %w", key, err)
	}

	var labels profile.Labels
	if err := labels.FromString(lbls); err != nil {
		return meta, fmt.Errorf("could not parse labels, key %q: %w", key, err)
	}

	meta = profile.Meta{
		ProfileID: pid,
		Service:   service,
		Type:      ptype,
		Labels:    labels,
		CreatedAt: digest.Time().UTC(),
	}
	return meta, nil
}
