package main

import (
	"fmt"
	"gopkg.in/ini.v1"
	"mylog/conf"
	"mylog/etcd"
	"mylog/kafka"
	"mylog/taillog"
	"sync"
	"time"
)

var (
	cfg *conf.AppConf = new(conf.AppConf) //用来从配置文件中获取配置信息的实例
	err error
)

func main() {
	//读取配置文件，获取配置信息
	err = ini.MapTo(cfg, "./conf/config.ini")
	if err != nil {
		fmt.Println("get conf from config.ini failed,err:", err)
		return
	}
	fmt.Println("get conf from config.ini success!")

	//对kafka进行初始化
	err = kafka.Init([]string{cfg.KafkaConf.Address})
	if err != nil {
		fmt.Println("Init kafka failed err:", err)
		return
	}
	fmt.Println("Init kafka failed success!")

	//初始化etcd
	err = etcd.Init([]string{cfg.EtcdConf.Address}, time.Duration(cfg.EtcdConf.TimeOut)*time.Second)
	if err != nil {
		fmt.Println("Init etcd failed err:", err)
		return
	}
	fmt.Println("Init etcd success!")
	
	//根据key从etcd中拉取日志收集项配置
	logEntrys, err := etcd.GetConfByKey(cfg.EtcdConf.EtcdKey)
	if err != nil {
		fmt.Println("Get tail Conf from etcd failed err:", err)
		return
	}
	taillog.Init(logEntrys) //将所有的配置项保存起来

	//将在etcd中的key注册一个watch，进行实时的监测etcd中的配置信息的变化
	newConfChan := taillog.OutNewConfChannel() //获取taillogMgr的一个只写的管道，用于接收新配置
	var wg sync.WaitGroup
	wg.Add(1)
	go etcd.WatchEtcdConf(cfg.EtcdConf.EtcdKey, newConfChan)
	wg.Wait()
}
