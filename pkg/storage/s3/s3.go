package s3

import (
	"bytes"
	"context"
	"fmt"
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

const profefeSchema = `P0.`

// Storage stores and loads profiles from s3.
//
// The schema for the key:
// schemaV.service/profile_type/digest/label1=value1,label2=value2
type Storage struct {
	logger     *log.Logger
	svc        s3iface.S3API
	uploader   s3manageriface.UploaderAPI
	downloader s3manageriface.DownloaderAPI

	bucket string
}

var _ storage.Writer = (*Storage)(nil)
var _ storage.Reader = (*Storage)(nil)

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
func (st *Storage) WriteProfile(ctx context.Context, props *storage.WriteProfileParams, r io.Reader) (profile.Meta, error) {
	key := createProfileKey(props.Service, props.Type, props.CreatedAt, props.Labels)

	resp, err := st.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(st.bucket),
		Key:    aws.String(key),
		Metadata: map[string]*string{
			"service":    aws.String(props.Service),
			"type":       aws.String(props.Type.String()),
			"labels":     aws.String(props.Labels.String()),
			"created_at": aws.String(props.CreatedAt.Format(time.RFC3339)),
		},
		Body: r,
	})
	if err != nil {
		return profile.Meta{}, err
	}

	meta := profile.Meta{
		ProfileID: profile.ID(key),
		Service:   props.Service,
		Type:      props.Type,
		Labels:    props.Labels,
		CreatedAt: props.CreatedAt,
	}

	st.logger.Debugw("writeProfile: s3 upload", "pid", meta.ProfileID, "key", key, "resp", resp)

	return meta, nil
}

func (st *Storage) ListProfiles(ctx context.Context, pids []profile.ID) (storage.ProfileList, error) {
	panic("not implemented")
}

/*
func (st *Storage) ListProfiles(ctx context.Context, pids []profile.ID) (storage.ProfileList, error) {
	if len(pids) == 0 {
		return nil, xerrors.New("empty profile ids")
	}

	objs := make([]s3manager.BatchDownloadObject, 0, len(pids))
	for _, pid := range pids {
		resp, err := st.svc.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
			Bucket:  aws.String(st.bucket),
			Prefix:  aws.String(pid.String()),
			MaxKeys: aws.Int64(1), // at most one object can be found by pid
		})
		if err != nil {
			return nil, err
		}
		if aws.Int64Value(resp.KeyCount) == 0 {
			return nil, storage.ErrNotFound
		}
		for _, obj := range resp.Contents {
			keys = append(keys, obj.Key)
		}
	}

	if len(keys) == 0 {
		return nil, storage.ErrNotFound
	}

	pl := &profileList{
		ctx:    ctx,
		keys:   keys,
		getter: st.get,
	}
	return pl, nil
}

type profileList struct {
	ctx    context.Context
	keys   []*string
	cur    int
	getter func(ctx context.Context, key string) ([]byte, error)

	err error // first error preserved and always returned
}

func (p *profileList) Next() (n bool) {
	if p.ctx.Err() != nil {
		return false
	}
	if p.err != nil {
		return false
	}

	if len(p.pids) == 0 {
		return false
	}

	p.cur, p.pids = p.pids[0], p.pids[1:]
	return true
}

func (p *profileList) Profile() (io.Reader, error) {
	if err := p.ctx.Err(); err != nil {
		return nil, err
	}

	if p.cur == nil {
		return nil, fmt.Errorf("profile out of range")
	}

	b, err := p.getter(p.ctx, profilePath(p.cur))
	if err != nil {
		p.err = err
		return nil, err
	}

	return bytes.NewReader(b), nil
}

func (p *profileList) Close() error {
	// prevent any use of this list's Profile or Next fn
	p.err = fmt.Errorf("list closed")
	return nil
}
*/

func (st *Storage) ListServices(ctx context.Context) ([]string, error) {
	panic("not implemented")
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
		limit = 100
	}

	prefix := profileKeyPrefix(params.Service, params.Type)
	input := &s3.ListObjectsV2Input{
		Bucket:  &st.bucket,
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int64(int64(limit)),
	}

	var metas []profile.Meta
	err := st.svc.ListObjectsV2PagesWithContext(ctx, input, func(page *s3.ListObjectsV2Output, _ bool) bool {
		for _, object := range page.Contents {
			key := aws.StringValue(object.Key)
			if key == "" {
				continue
			}

			meta, err := metaFromProfileKey(profefeSchema, key)
			if err != nil {
				st.logger.Errorw("storage s3 failed to parse profile meta from object key", "key", key, zap.Error(err))
				continue
			}

			if meta.CreatedAt.After(createdAtMax) {
				return false
			}

			if !labelsInclude(meta.Labels, params.Labels) {
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
	return metas, err
}

// get downloads a value from a key. Context can be canceled.
// This is safe for multiple go routines.
func (st *Storage) get(ctx context.Context, key string) ([]byte, error) {
	input := &s3.GetObjectInput{
		Bucket: &st.bucket,
		Key:    &key,
	}

	buf := make([]byte, 0, 16384) // pre-allocated 16KB for the s3 object.
	w := aws.NewWriteAtBuffer(buf)
	_, err := st.downloader.DownloadWithContext(ctx, w, input)
	if err != nil {
		return nil, err
	}
	buf = w.Bytes()
	return buf, nil
}

func createProfileKey(service string, ptype profile.ProfileType, createdAt time.Time, labels profile.Labels) string {
	var buf bytes.Buffer
	buf.WriteString(profileKeyPrefix(service, ptype))

	digest := xid.NewWithTime(createdAt)
	buf.WriteString(digest.String())
	buf.WriteByte('/')
	buf.WriteString(labels.String())

	return buf.String()
}

func profileKeyPrefix(service string, ptype profile.ProfileType) string {
	service = strings.ReplaceAll(service, "/", "_")
	return fmt.Sprintf("%s%s/%d/", profefeSchema, service, ptype)
}

// parses the s3 key by splitting by / to create a profile.Meta.
// The format of the key is:
// schemaV.service/profile_type/digest/label1,label2
func metaFromProfileKey(schemaV, key string) (meta profile.Meta, err error) {
	if !strings.HasPrefix(key, schemaV) {
		return meta, xerrors.Errorf("bad key format %q: schema version mismatch, want %s", key, schemaV)
	}
	key = strings.TrimPrefix(key, schemaV)

	ks := strings.SplitN(key, "/", 4)
	if len(ks) == 0 {
		return meta, xerrors.Errorf("bad key format %q", key)
	}

	ks = ks[1:]
	var service, pt, dgst, lbls string
	switch len(ks) {
	case 3: // no labels are set in the path
		service, pt, dgst = ks[0], ks[1], ks[2]
	case 4:
		service, pt, dgst, lbls = ks[0], ks[1], ks[2], ks[3]
	default:
		return profile.Meta{}, xerrors.Errorf("invalid key format %q: want at most 4 fields", key)
	}

	v, _ := strconv.Atoi(pt)
	ptype := profile.ProfileType(v)
	if ptype == profile.TypeUnknown {
		return profile.Meta{}, xerrors.Errorf("could not parse profile type %q, key %q", pt, key)
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
		ProfileID: profile.ID(key),
		Service:   service,
		Type:      ptype,
		Labels:    labels,
		CreatedAt: digest.Time(),
	}
	return meta, nil
}

// includes checks if a includes all of b keys and values.
func labelsInclude(a, b profile.Labels) bool {
	hash := make(map[string]string, len(a))
	for _, l := range a {
		hash[l.Key] = l.Value
	}

	for _, l := range b {
		v, ok := hash[l.Key]
		if !ok {
			return false
		}
		if l.Value != v {
			return false
		}
	}
	return true
}
