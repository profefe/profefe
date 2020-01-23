package storage

import (
	"context"
	"io"

	"github.com/profefe/profefe/pkg/profile"
)

type WriteProfileMock func(ctx context.Context, meta profile.Meta, r io.Reader) error

type MockWriter struct {
	WriteProfileMock
}

var _ Writer = (*MockWriter)(nil)

func (sw *MockWriter) WriteProfile(ctx context.Context, meta profile.Meta, r io.Reader) error {
	return sw.WriteProfileMock(ctx, meta, r)
}

type ListServicesMock func(ctx context.Context) ([]string, error)

type FindProfilesMock func(ctx context.Context, params *FindProfilesParams) ([]profile.Meta, error)

type FindProfileIDsMock func(ctx context.Context, params *FindProfilesParams) ([]profile.ID, error)

type ListProfilesMock func(ctx context.Context, pid []profile.ID) (ProfileList, error)

type MockReader struct {
	ListServicesMock
	ListProfilesMock
	FindProfilesMock
	FindProfileIDsMock
}

var _ Reader = (*MockReader)(nil)

func (sr *MockReader) ListServices(ctx context.Context) ([]string, error) {
	return sr.ListServicesMock(ctx)
}

func (sr *MockReader) FindProfiles(ctx context.Context, params *FindProfilesParams) ([]profile.Meta, error) {
	return sr.FindProfilesMock(ctx, params)
}

func (sr *MockReader) FindProfileIDs(ctx context.Context, params *FindProfilesParams) ([]profile.ID, error) {
	return sr.FindProfileIDsMock(ctx, params)
}

func (sr *MockReader) ListProfiles(ctx context.Context, pid []profile.ID) (ProfileList, error) {
	return sr.ListProfilesMock(ctx, pid)
}
