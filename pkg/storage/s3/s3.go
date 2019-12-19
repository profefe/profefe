package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	pprofProfile "github.com/profefe/profefe/internal/pprof/profile"
	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/storage"
)

const (
	// MaxRetries is the number of times to retry reading from s3.
	MaxRetries = 3
)

var _ storage.Writer = (*Store)(nil)
var _ storage.Reader = (*Store)(nil)

// S3Store stores and loads profiles from s3 using carefully constructed
// object key.
//
// The schema for the key is:
// /service/profile_type/created_at_unix_time/label1=value1,label2=value2/id
type Store struct {
	Region   string
	S3Bucket string

	logger *log.Logger

	session client.ConfigProvider
	svc     s3iface.S3API

	mu         sync.Mutex // protects the creation of uploader/downloader
	uploader   s3manageriface.UploaderAPI
	downloader s3manageriface.DownloaderAPI
}

// NewStore reads and writes profiles from region and bucket.
func NewStore(logger *log.Logger, region, s3bucket string) (*Store, error) {
	session, err := newSession(region)
	if err != nil {
		return nil, err
	}

	return &Store{
		Region:   region,
		S3Bucket: s3bucket,
		session:  session,
		logger:   logger,
		svc:      newService(session),
	}, nil
}

type s3Meta struct {
	Meta profile.Meta `json:"meta"`
	Path string       `json:"path"`
}

// WriteProfile uploads the profile to s3.
// Context can be canceled and this is safe for multiple go routines.
func (s *Store) WriteProfile(ctx context.Context, meta profile.Meta, r io.Reader) error {
	profilePath := profilePath(meta.ProfileID)
	// obj is a breadcrumb from the s3 index to the actual s3 path.
	obj := s3Meta{
		Meta: meta,
		Path: profilePath,
	}

	b, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	// First, we save the meta data at a searchable key.
	if err := s.put(ctx, key(meta), bytes.NewBuffer(b)); err != nil {
		return err
	}

	// Next, we write the entire profile at /profiles/id
	return s.put(ctx, profilePath, r)
}

var _ storage.ProfileList = (*profileList)(nil)

type profileList struct {
	ctx    context.Context
	pids   []profile.ID
	cur    profile.ID
	getter func(ctx context.Context, key string) ([]byte, error)
	parser func(data []byte) (*pprofProfile.Profile, error)

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

func (p *profileList) Profile() (*pprofProfile.Profile, error) {
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

	prof, err := pprofProfile.ParseData(b)
	if err != nil {
		p.err = err
	}
	return prof, err
}

func (p *profileList) Close() error {
	// prevent any use of this list's Profile or Next fn
	p.err = fmt.Errorf("list closed")
	return nil
}

func (s *Store) ListProfiles(ctx context.Context, pids []profile.ID) (storage.ProfileList, error) {
	return &profileList{
		ctx:    ctx,
		pids:   pids,
		getter: s.get,
		parser: pprofProfile.ParseData,
	}, nil
}

// FindProfiles searches s3 for profile meta data.
func (s *Store) FindProfiles(ctx context.Context, params *storage.FindProfilesParams) ([]profile.Meta, error) {
	return s.find(ctx, params)
}

// FindProfileIDs calls FindProfiles and returns just the IDs.
func (s *Store) FindProfileIDs(ctx context.Context, params *storage.FindProfilesParams) ([]profile.ID, error) {
	metas, err := s.find(ctx, params)
	if err != nil {
		return nil, err
	}
	ids := make([]profile.ID, len(metas))
	for i := range metas {
		ids[i] = metas[i].ProfileID
	}
	return ids, nil
}

func (s *Store) find(ctx context.Context, params *storage.FindProfilesParams) ([]profile.Meta, error) {
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

	// TODO: is this max ok?
	limit := params.Limit
	if limit == 0 {
		limit = 100
	}

	input := &s3.ListObjectsV2Input{
		Bucket:     &s.S3Bucket,
		Prefix:     aws.String(prefix(params)),
		StartAfter: aws.String(startAfter(params)),
		MaxKeys:    aws.Int64(1000),
	}

	metas := []profile.Meta{}
	err := s.svc.ListObjectsV2PagesWithContext(ctx, input,
		func(page *s3.ListObjectsV2Output, _ bool) bool {
			for _, object := range page.Contents {
				if object.Key == nil {
					continue
				}
				m, err := meta(*object.Key)
				if err != nil {
					s.logger.Error(err)
					continue
				}

				if m.CreatedAt.After(createdAtMax) {
					return false
				}

				if !includes(m.Labels, params.Labels) {
					continue
				}
				metas = append(metas, *m)
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

func (s *Store) put(ctx context.Context, key string, body io.Reader) error {
	s.newUploader()

	input := &s3manager.UploadInput{
		Body:   body,
		Bucket: &s.S3Bucket,
		Key:    &key,
	}

	_, err := s.uploader.UploadWithContext(ctx, input)
	return err
}

// get downloads a value from a key. Context can be canceled.
// This is safe for multiple go routines.
func (s *Store) get(ctx context.Context, key string) ([]byte, error) {
	s.newDownloader()

	input := &s3.GetObjectInput{
		Bucket: &s.S3Bucket,
		Key:    &key,
	}

	buf := make([]byte, 0, 16384) // pre-allocated 16KB for the s3 object.
	w := aws.NewWriteAtBuffer(buf)
	_, err := s.downloader.DownloadWithContext(ctx, w, input)
	if err != nil {
		return nil, err
	}
	buf = w.Bytes()
	return buf, nil
}

func (s *Store) newUploader() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.uploader == nil {
		s.uploader = s3manager.NewUploaderWithClient(s.svc)
	}
}

func (s *Store) newDownloader() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.downloader == nil {
		s.downloader = s3manager.NewDownloaderWithClient(s.svc)
	}
}

func newSession(region string) (client.ConfigProvider, error) {
	return session.NewSession(&aws.Config{
		Region:     aws.String(region),
		MaxRetries: aws.Int(MaxRetries),
	})
}

func newService(session client.ConfigProvider) *s3.S3 {
	return s3.New(session)
}

// key builds a searchable s3 key for the profile.Meta.
// The schema is: /service/profile_type/created_at_unix_time/label1,label2/id
//
// This schema allows us to do to prefix searches to select service,
// profile_type, and time range.
func key(meta profile.Meta) string {
	return strings.Join(
		[]string{
			meta.Service,
			meta.Type.String(),
			strconv.FormatInt(meta.CreatedAt.UnixNano(), 10),
			meta.Labels.String(),
			meta.ProfileID.String(),
		},
		"/",
	)
}

// meta parses the s3 key by splitting by / to create a profile.Meta.
//
// Note: InstanceID is not set.
func meta(key string) (*profile.Meta, error) {
	ks := strings.Split(key, "/")
	var svc, typ, tm, pid, lbls string
	switch len(ks) {
	case 4: // no labels are set in the path
		svc, typ, tm, pid, lbls = ks[0], ks[1], ks[2], ks[3], ""
	case 5:
		svc, typ, tm, lbls, pid = ks[0], ks[1], ks[2], ks[3], ks[4]
	default:
		return nil, fmt.Errorf("invalid key format %s; expected 5 fields", key)
	}

	profileID, err := profile.IDFromString(pid)
	if err != nil {
		return nil, err
	}

	var profileType profile.ProfileType
	if err := profileType.FromString(typ); err != nil {
		return nil, err
	}

	var labels profile.Labels
	if err := labels.FromString(lbls); err != nil {
		return nil, err
	}

	ns, err := strconv.ParseInt(tm, 10, 64)
	if err != nil {
		return nil, err
	}

	createdAt := time.Unix(0, ns).UTC()

	return &profile.Meta{
		ProfileID: profileID,
		Service:   svc,
		Type:      profileType,
		Labels:    labels,
		CreatedAt: createdAt,
	}, nil
}

// startAfter starts the s3 list after the CreatedAtMin time.
func startAfter(params *storage.FindProfilesParams) string {
	return strings.Join(
		[]string{
			params.Service,
			params.Type.String(),
			strconv.FormatInt(params.CreatedAtMin.UnixNano(), 10),
		},
		"/",
	)
}

// prefix is used to filter down the s3 objects by service and type.
func prefix(params *storage.FindProfilesParams) string {
	return strings.Join(
		[]string{
			params.Service,
			params.Type.String(),
		},
		"/",
	)
}

// includes checks if a includes all of b keys and values.
func includes(a, b profile.Labels) bool {
	hash := make(map[string]string)
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

func profilePath(id profile.ID) string {
	return fmt.Sprintf("v0/profiles/%s", id)
}
