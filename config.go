package main

var DefConfig = &Config{}

type Config struct {
	MySqlAddress         string
	MySqlUserName        string
	MySqlPassword        string
	MySqlDBName          string
	MySqlMaxIdleConnSize int
	MySqlMaxConnSize     int
	MySqlMaxOpenConnSize int
	MySqlConnMaxLifetime int
	OntologyRpcAddress   string
	HttpServerPort       uint
	DBBatchSize          int
	DBBatchTime          int
	MaxQueryPageSize     int
}
