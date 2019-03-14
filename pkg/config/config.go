package config

import (
	"flag"
	"fmt"
	"time"
)

const (
	defaultAddr        = ":10100"
	defaultExitTimeout = 5 * time.Second
)

type Config struct {
	Addr        string
	ExitTimeout time.Duration
	Postgres    PostgresConfig
}

func (conf *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&conf.Addr, "addr", defaultAddr, "address to listen")
	f.DurationVar(&conf.ExitTimeout, "exit-timeout", defaultExitTimeout, "server shutdown timeout")

	conf.Postgres.RegisterFlags(f)
}

type PostgresConfig struct {
	Host     string
	Port     int
	SSLMode  string
	User     string
	Password string
	Database string
}

func (conf *PostgresConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&conf.Host, "pg.host", "localhost", "postgres host")
	f.IntVar(&conf.Port, "pg.port", 5432, "postgres port")
	f.StringVar(&conf.SSLMode, "pg.ssl-mode", "disable", "postgres connection ssl mode")
	f.StringVar(&conf.User, "pg.user", "postgres", "postgres user")
	f.StringVar(&conf.Password, "pg.password", "", "postgres password")
	f.StringVar(&conf.Database, "pg.database", "profiles", "postgres database name")
}

func (conf *PostgresConfig) ConnString() string {
	return fmt.Sprintf(
		"host=%v port=%v user=%v password='%s' dbname=%v sslmode=%v",
		conf.Host,
		conf.Port,
		conf.User,
		conf.Password,
		conf.Database,
		conf.SSLMode,
	)
}
