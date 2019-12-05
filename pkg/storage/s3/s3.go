package s3

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/profefe/profefe/pkg/profile"
)

// S3Store stores and loads profiles from s3 using carefully constructed
// object key.
type S3Store struct{}

// key builds a searchable s3 key for the profile.Meta.
// The schema is: /service/profile_type/created_at_unix_time/label1,label2/id
//
// This schema allows us to do to prefix searches to select service,
// profile_type, and time range.
func key(meta profile.Meta) string {
	return strings.Join(
		[]string{
			"",
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
	if len(ks) != 6 {
		return nil, fmt.Errorf("invalid key format %s; expected 5 fields", key)
	}

	svc, typ, tm, lbls, pid := ks[1], ks[2], ks[3], ks[4], ks[5]

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
