package main

import (
	"fmt"

	"github.com/profefe/profefe/pkg/profile"
)

type ProfileType profile.ProfileType

func (pt ProfileType) String() string {
	return profile.ProfileType(pt).String()
}

func (pt *ProfileType) Set(s string) error {
	var origType profile.ProfileType
	if err := origType.FromString(s); err != nil {
		return err
	}
	if origType == profile.UnknownProfile {
		return fmt.Errorf("unknown profile: %v", s)
	}
	*pt = ProfileType(origType)
	return nil
}

type Labels profile.Labels

func (ll Labels) String() string {
	return profile.Labels(ll).String()
}

func (ll *Labels) Set(s string) error {
	return (*profile.Labels)(ll).FromString(s)
}
