package main

var DefConfig = &Config{}

type Config struct {
	MySqlAddress                 string
	MySqlUserName                string
	MySqlPassword                string
	MySqlDBName                  string
	MySqlMaxIdleConnSize         uint32
	MySqlMaxConnSize             uint32
	MySqlMaxOpenConnSize         uint32
	MySqlConnMaxLifetime         uint32
	MySqlHeartbeatTimeoutTime    uint32
	MySqlHeartbeatUpdateInterval uint32
	OntologyRpcAddress           string
	HttpServerPort               uint32
	DBBatchSize                  uint32
	DBBatchTime                  uint32
	MaxQueryPageSize             uint32
}

func (this *Config) GetHeartbeatUpdateInterval() uint32 {
	if this.MySqlHeartbeatUpdateInterval == 0 {
		return DEFAULT_HAARTBEAT_UPDATE_INTERVAL
	}
	return this.MySqlHeartbeatUpdateInterval
}

func (this *Config) GetHeartbeatTimeoutTime() uint32 {
	if this.MySqlHeartbeatTimeoutTime == 0 {
		return DEFAULT_HEARTBEAT_TIMEOUT_TIME
	}
	return this.MySqlHeartbeatTimeoutTime
}
