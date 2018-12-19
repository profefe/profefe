package profile

import (
	"time"

	"github.com/rs/xid"
)

type Digest string

type Profile struct {
	Type       ProfileType
	CreatedAt  time.Time
	ReceivedAt time.Time
	Service    *Service
}

type Token xid.ID

func TokenFromString(s string) Token {
	token, _ := xid.FromString(s)
	return Token(token)
}

func (token Token) String() string {
	return xid.ID(token).String()
}

type Service struct {
	Name    string
	BuildID string
	Token   Token
	Labels  Labels
}

func NewService(name, id string, labels Labels) *Service {
	return &Service{
		Name:    name,
		BuildID: id,
		Token:   Token(xid.New()),
		Labels:  labels,
	}
}
