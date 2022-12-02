package conf

type AppConf struct {
	KafkaConf `ini:"kafka"`
	EtcdConf  `ini:"etcd"`
}

type KafkaConf struct {
	Address string `ini:"address"`
}

type EtcdConf struct {
	Address string `ini:"address"`
	TimeOut int    `ini:"timeout"`
	EtcdKey string `ini:"etcdkey"`
}
