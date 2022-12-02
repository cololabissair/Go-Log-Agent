package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

// LogEntry 是日志收集项的配置信息
type LogEntry struct {
	Path  string `json:"path"`  //被读取日志的路径
	Topic string `json:"topic"` //发往kafka中的Topic
}

var (
	client *clientv3.Client
)

func Init(address []string, timeout time.Duration) (err error) {
	client, err = clientv3.New(clientv3.Config{Endpoints: address, DialTimeout: timeout})
	if err != nil {
		return
	}
	return
}

// GetConfByKey 根据配置文件中的key去etcd中拉取日志收集项的配置
func GetConfByKey(key string) (logEntrys []*LogEntry, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := client.Get(ctx, key)
	cancel() //手动取消ctx
	if err != nil {
		return
	}
	for _, ev := range resp.Kvs {
		//获取到etcd中存的所有日志收集项的配置信息
		err = json.Unmarshal(ev.Value, &logEntrys)
		if err != nil {
			fmt.Println("json Unmarshal failed err:", err)
			return nil, err
		}
	}
	return
}

// WatchEtcdConf 注册一个watch实时检测这个key
func WatchEtcdConf(key string, newConfChan chan<- []*LogEntry) {
	watchChan := client.Watch(context.Background(), key) //当配置变化时，会向watchChan发送信号

	var newConf []*LogEntry //用于接收etcd更新后所有新的配置
	//遍历watchChan
	for wsp := range watchChan {
		for _, ev := range wsp.Events {
			//判断配置内容改变的情况
			if ev.Type != clientv3.EventTypeDelete {
				//说明不是删除操作，即ev.Value是肯定能取到值的,会将etcd中更新后的所有配置拉取下来
				//将最新的配置填充进newConf
				err := json.Unmarshal(ev.Kv.Value, &newConf)
				if err != nil {
					fmt.Println("watch unmarshal failed err:", err)
					continue
				}
			}
			//说明是删除操作,我们直接将空的slice塞进kafka监听新配置的管道
			newConfChan <- newConf
		}
	}
}
