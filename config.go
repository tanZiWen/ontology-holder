package main

var DefConfig = &Config{}

type Config struct {
	MysqlDataSourceName string
	OntologyRpcAddress  string
	HttpServerPort      uint
	DBBatchSize         int
	DBBatchTime         int
	MaxQueryPageSize    int
}
