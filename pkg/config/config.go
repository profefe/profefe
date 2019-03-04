package config

import (
	"flag"
	"fmt"
)

type Config struct {
	Addr     string
	Postgres PostgresConfig
}

func (conf *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&conf.Addr, "addr", ":10100", "address to listen")

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
