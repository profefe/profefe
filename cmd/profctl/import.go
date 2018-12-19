package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/profefe/profefe/pkg/config"
	"github.com/profefe/profefe/pkg/logger"
	"github.com/profefe/profefe/pkg/profile"
	"go.uber.org/zap"
)

func runImport(ctx context.Context, args []string) error {
	fset := flag.NewFlagSet("import", flag.ExitOnError)

	var (
		proType ProfileType
		service string
		labels  Labels
	)
	fset.Var(&proType, "type", "profile type (cpu, heap, etc)")
	fset.StringVar(&service, "service", "", "profile service")
	fset.Var(&labels, "labels", "profile service labels (e.g. az=home,host=local,version=1.0)")

	var conf config.Config
	conf.RegisterFlags(fset)

	if err := fset.Parse(args); err != nil {
		return err
	}

	profFile := fset.Arg(0)
	if profFile == "" {
		return fmt.Errorf("file can't be empty")
	}

	if service == "" {
		return fmt.Errorf("service can't be empty")
	}

	// TODO: init base logger
	baseLogger := zap.NewExample()
	defer baseLogger.Sync()

	log := logger.New(baseLogger)

	profileRepo, err := newProfileRepo(log, conf)
	if err != nil {
		return fmt.Errorf("failed to create profile repo: %v", err)
	}

	file, err := os.Open(profFile)
	if err != nil {
		return err
	}
	defer file.Close()

	profID := "imported" // TODO: generate ID (base on profFile, maybe)

	createReq := &profile.CreateProfileRequest{
		ID:      profID,
		Service: service,
		Labels:  profile.Labels(labels),
	}
	token, err := profileRepo.CreateProfile(ctx, createReq)
	if err != nil {
		return err
	}

	updateReq := &profile.UpdateProfileRequest{
		ID:    profID,
		Token: token,
		Type:  profile.ProfileType(proType),
	}
	if err := profileRepo.UpdateProfile(ctx, updateReq, file); err != nil {
		return err
	}

	return nil
}
