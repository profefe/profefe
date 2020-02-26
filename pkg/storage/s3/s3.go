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

const profileIDVersion = "v0"

// Storage stores and loads profiles from s3.
//
// The schema for the key is:
// schemaV/service/profile_type/created_at_unix_time/digest/label1=value1,label2=value2
type Storage struct {
	logger     *log.Logger
	svc        s3iface.S3API
	uploader   s3manageriface.UploaderAPI
	downloader s3manageriface.DownloaderAPI

	s3Bucket string
}

var _ storage.Writer = (*Storage)(nil)
var _ storage.Reader = (*Storage)(nil)

func New(logger *log.Logger, svc s3iface.S3API, s3Bucket string) *Storage {
	return &Storage{
		logger:     logger,
		svc:        svc,
		uploader:   s3manager.NewUploaderWithClient(svc),
		downloader: s3manager.NewDownloaderWithClient(svc),

		s3Bucket: s3Bucket,
	}
}

// WriteProfile uploads the profile to s3.
// Context can be canceled and this is safe for multiple goroutines.
func (st *Storage) WriteProfile(ctx context.Context, props *storage.WriteProfileParams, r io.Reader) (profile.Meta, error) {
	pid := makeProfileID(props.Service, props.Type, props.CreatedAt, xid.New().String())
	meta := profile.Meta{
		ProfileID: []byte(pid),
		Service:   props.Service,
		Type:      props.Type,
		Labels:    props.Labels,
		CreatedAt: props.CreatedAt,
	}

	key := pid + "/" + meta.Labels.String()
	resp, err := st.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(st.s3Bucket),
		Key:    aws.String(key),
		Metadata: map[string]*string{
			"service": aws.String(props.Service),
		},
		Body: r,
	})
	if err != nil {
		return profile.Meta{}, err
	}

	st.logger.Debugw("s3 upload", "pid", meta.ProfileID, "loc", resp.Location, "upid", resp.UploadID)

	return meta, nil
}

func (st *Storage) ListProfiles(ctx context.Context, pids []profile.ID) (storage.ProfileList, error) {
	if len(pids) == 0 {
		return nil, xerrors.New("empty profile ids")
	}

	objs := make([]s3manager.BatchDownloadObject, 0, len(pids))
	for _, pid := range pids {
		resp, err := st.svc.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
			Bucket:  aws.String(st.s3Bucket),
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
		return nil, fmt.Errorf("empty created_at")
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

	prefix := fmt.Sprintf("%s/%s/%d/", profileIDVersion, params.Service, params.Type)
	startAfter := fmt.Sprintf("%s/%d", prefix, params.CreatedAtMin.UnixNano())
	input := &s3.ListObjectsV2Input{
		Bucket:     &st.s3Bucket,
		Prefix:     aws.String(prefix),
		StartAfter: aws.String(startAfter),
		MaxKeys:    aws.Int64(int64(limit)),
	}

	var metas []profile.Meta
	err := st.svc.ListObjectsV2PagesWithContext(ctx, input, func(page *s3.ListObjectsV2Output, _ bool) bool {
		for _, object := range page.Contents {
			key := aws.StringValue(object.Key)
			if key == "" {
				continue
			}
			meta, err := parseMetaFromKey(key)
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
		Bucket: &st.s3Bucket,
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

func makeProfileID(service string, ptype profile.ProfileType, createdAt time.Time, digest string) string {
	return fmt.Sprintf("%s/%s/%d/%d/%s", profileIDVersion, service, ptype, createdAt.UnixNano(), digest)
}

// parses the s3 key by splitting by / to create a profile.Meta.
// the format of the key is:
// schemaV/service/profile_type/created_at_unix_time/digest/label1,label2
func parseMetaFromKey(key string) (profile.Meta, error) {
	ks := strings.SplitN(key, "/", 4)
	if len(ks) == 0 {
		return profile.Meta{}, fmt.Errorf("invalid key format %q", key)
	}
	if ks[0] != profileIDVersion {
		return profile.Meta{}, fmt.Errorf("invalid key format %q, schema version mismatch", key)
	}

	ks = ks[1:]
	var service, pt, tm, digest, lbls string
	switch len(ks) {
	case 3: // no labels are set in the path
		service, pt, tm, digest = ks[0], ks[1], ks[2], ks[3]
	case 4:
		service, pt, tm, digest, lbls = ks[0], ks[1], ks[2], ks[3], ks[4]
	default:
		return profile.Meta{}, fmt.Errorf("invalid key format %s; expected 4 fields", key)
	}

	v, _ := strconv.Atoi(pt)
	ptype := profile.ProfileType(v)
	if ptype == profile.TypeUnknown {
		return profile.Meta{}, fmt.Errorf("could not parse profile type %q", pt)
	}

	ns, err := strconv.ParseInt(tm, 10, 64)
	if err != nil {
		return profile.Meta{}, err
	}
	createdAt := time.Unix(0, ns).UTC()

	pid := makeProfileID(service, ptype, createdAt, digest)

	var labels profile.Labels
	if err := labels.FromString(lbls); err != nil {
		return profile.Meta{}, err
	}

	meta := profile.Meta{
		ProfileID: []byte(pid),
		Service:   service,
		Type:      ptype,
		Labels:    labels,
		CreatedAt: createdAt,
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
