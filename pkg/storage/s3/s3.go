package s3

import (
	"bytes"
	"context"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/storage"
	"github.com/rs/xid"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
)

// s3 objects' key prefix indicates the key's naming schema
const profefeSchema = `P0.`

const (
	// initial size of buffer pre-allocated for the s3 object
	getObjectBufferSize     = 16384
	defaultListObjectsLimit = 100
)

// Storage stores and loads profiles from s3.
//
// The schema for the object key:
// schemaV.service/profile_type/digest,label1=value1,label2=value2
//
// Where
// "schemaV" indicates the naming schema that was used when the profile was stored;
// "digests" uniquely describes the profile, it also includes profiles creation time.
type Storage struct {
	logger     *log.Logger
	svc        s3iface.S3API
	uploader   s3manageriface.UploaderAPI
	downloader s3manageriface.DownloaderAPI

	bucket string
}

var _ storage.Storage = (*Storage)(nil)

func New(logger *log.Logger, svc s3iface.S3API, s3Bucket string) *Storage {
	return &Storage{
		logger:     logger,
		svc:        svc,
		uploader:   s3manager.NewUploaderWithClient(svc),
		downloader: s3manager.NewDownloaderWithClient(svc),

		bucket: s3Bucket,
	}
}

// WriteProfile uploads the profile to s3.
// Context can be canceled and this is safe for multiple goroutines.
func (st *Storage) WriteProfile(ctx context.Context, params *storage.WriteProfileParams, r io.Reader) (profile.Meta, error) {
	createdAt := params.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	key := createProfileKey(params.Service, params.Type, createdAt, params.Labels)

	resp, err := st.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(st.bucket),
		Key:    aws.String(key),
		Metadata: map[string]*string{
			"service":    aws.String(params.Service),
			"type":       aws.String(params.Type.String()),
			"labels":     aws.String(params.Labels.String()),
			"created_at": aws.String(createdAt.Format(time.RFC3339)),
		},
		Body: r,
	})
	if err != nil {
		return profile.Meta{}, err
	}

	meta := profile.Meta{
		ProfileID: profile.ID(key),
		Service:   params.Service,
		Type:      params.Type,
		Labels:    params.Labels,
		CreatedAt: createdAt,
	}

	st.logger.Debugw("writeProfile: s3 upload", "pid", meta.ProfileID, "key", key, "resp", resp, "meta", meta)

	return meta, nil
}

func (st *Storage) ListProfiles(ctx context.Context, pids []profile.ID) (storage.ProfileList, error) {
	if len(pids) == 0 {
		return nil, xerrors.New("empty profile ids")
	}

	pl := &profileList{
		ctx:    ctx,
		pids:   pids,
		buf:    make([]byte, 0, getObjectBufferSize),
		getter: st.getObject,
	}
	return pl, nil
}

type profileList struct {
	ctx  context.Context
	pids []profile.ID
	// points to the current key in the iteration
	key string
	// intermediate buffer that keeps downloaded data
	buf    []byte
	getter func(ctx context.Context, w io.WriterAt, key string) error
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
		panic("s3 profileList: profile out of range")
	}

	w := aws.NewWriteAtBuffer(pl.buf[:0])
	err := pl.getter(pl.ctx, w, pl.key)
	if err != nil {
		pl.setErr(err)
		return nil, err
	}

	// reset our buffer with the one aws.WriterAt might have created on buffer grow
	pl.buf = w.Bytes()

	return bytes.NewReader(pl.buf), nil
}

func (pl *profileList) Close() error {
	// clear the pointer to the buffered data
	pl.buf = nil
	// prevent any use of this list's Profile or Next fn
	pl.err = xerrors.Errorf("profile list closed")
	return nil
}

func (pl *profileList) setErr(err error) {
	if pl.err == nil {
		pl.err = err
	}
}

// ListServices returns the list of distinct services for which profiles are stored in the bucket.
func (st *Storage) ListServices(ctx context.Context) ([]string, error) {
	input := &s3.ListObjectsV2Input{
		Bucket:    &st.bucket,
		Prefix:    aws.String(profefeSchema),
		Delimiter: aws.String("/"), // delimiter makes ListObjects to return only unique common prefixes
	}

	var services []string
	err := st.svc.ListObjectsV2PagesWithContext(ctx, input, func(page *s3.ListObjectsV2Output, _ bool) bool {
		if len(page.CommonPrefixes) == 0 {
			return false
		}

		prefix := aws.StringValue(page.Prefix)

		st.logger.Debugw("listServices: s3 list objects", "prefix", prefix, "common prefixes", page.CommonPrefixes)

		for _, cp := range page.CommonPrefixes {
			s := aws.StringValue(cp.Prefix)
			if s != "" {
				s = strings.TrimPrefix(s, prefix)
				services = append(services, strings.TrimSuffix(s, "/"))
			}
		}

		return *page.IsTruncated
	})
	if err != nil {
		return nil, err
	}
	if len(services) == 0 {
		return nil, storage.ErrNotFound
	}

	return services, nil
}

// FindProfiles queries s3 for profile metas matched searched criteria.
func (st *Storage) FindProfiles(ctx context.Context, params *storage.FindProfilesParams) ([]profile.Meta, error) {
	return st.findProfiles(ctx, params)
}

// FindProfileIDs queries s3 for profile IDs matched searched criteria.
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
		return nil, xerrors.Errorf("empty service")
	}

	if params.CreatedAtMin.IsZero() {
		return nil, xerrors.Errorf("empty created_at min")
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
	input := &s3.ListObjectsV2Input{
		Bucket:  &st.bucket,
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int64(int64(limit)),
	}

	st.logger.Debugw("findProfiles: s3 list objects pages", "input", input)

	var metas []profile.Meta
	err := st.svc.ListObjectsV2PagesWithContext(ctx, input, func(page *s3.ListObjectsV2Output, _ bool) bool {
		for _, object := range page.Contents {
			key := aws.StringValue(object.Key)
			if key == "" {
				st.logger.Debugw("findProfiles: s3 list objects, empty object key", "object", object)
				continue
			}

			meta, err := metaFromProfileKey(profefeSchema, key)
			if err != nil {
				st.logger.Errorw("storage s3 failed to parse profile meta from object key", "key", key, zap.Error(err))
				continue
			}

			if meta.CreatedAt.Before(params.CreatedAtMin) {
				continue
			}

			if meta.CreatedAt.After(createdAtMax) {
				return false
			}

			if !meta.Labels.Include(params.Labels) {
				st.logger.Debugw("findProfiles: s3 list objects, labels mismatch", "left", meta.Labels, "right", params.Labels)
				continue
			}

			metas = append(metas, meta)
		}

		if len(metas) >= limit {
			metas = metas[:limit]
			return false
		}

		if page.IsTruncated == nil {
			return false
		}
		return *page.IsTruncated
	})

	// TODO(narqo) parse NoCredentialProviders and similar to return meaningful errors to the user
	if err != nil {
		return nil, err
	}

	if len(metas) == 0 {
		return nil, storage.ErrNotFound
	}

	return metas, nil
}

// getObject downloads a value from a key. Context can be canceled.
// This is safe for multiple go routines.
func (st *Storage) getObject(ctx context.Context, w io.WriterAt, key string) error {
	input := &s3.GetObjectInput{
		Bucket: aws.String(st.bucket),
		Key:    aws.String(key),
	}
	n, err := st.downloader.DownloadWithContext(ctx, w, input)
	if err != nil {
		return err
	}

	st.logger.Debugw("s3 got object", "bucket", st.bucket, "key", key, "sz", n)

	return nil
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

// parses the s3 key by splitting by / to create a profile.Meta.
// The format of the key is:
// schemaV.service/profile_type/digest,label1,label2
func metaFromProfileKey(schemaV, key string) (meta profile.Meta, err error) {
	if !strings.HasPrefix(key, schemaV) {
		return meta, xerrors.Errorf("invalid key format %q: schema version mismatch, want %s", key, schemaV)
	}

	// create profile ID from the original object's key
	pid := profile.ID(key)

	key = strings.TrimPrefix(key, schemaV)
	ks := strings.SplitN(key, "/", 3)
	if len(ks) != 3 {
		return meta, xerrors.Errorf("invalid key format %q", key)
	}

	service, pt, tail := ks[0], ks[1], ks[2]

	v, _ := strconv.Atoi(pt)
	ptype := profile.ProfileType(v)
	if ptype == profile.TypeUnknown {
		return profile.Meta{}, xerrors.Errorf("could not parse profile type %q, key %q", pt, key)
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
		return meta, xerrors.Errorf("could not parse digest, key %q: %w", key, err)
	}

	var labels profile.Labels
	if err := labels.FromString(lbls); err != nil {
		return meta, xerrors.Errorf("could not parse labels, key %q: %w", key, err)
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
