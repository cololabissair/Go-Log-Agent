package taillog

import (
	"fmt"
	"mylog/etcd"
	"time"
)

type tailTskMgr struct {
	logEntrys   []*etcd.LogEntry      //从etcd上拉取的所有日志收集项的配置信息
	tailTskMap  map[string]*tailTask  //当前已经启动的所有服务的字典，key:日志项配置的路径（path）+topic
	newConfChan chan []*etcd.LogEntry //获取最新配置的管道
}

// 定义一个全局的，用来管理所有的日志收集项
var tskMgr *tailTskMgr

// Init
func Init(logEntrys []*etcd.LogEntry) {
	//初始化全局管理的tskMgr
	tskMgr = &tailTskMgr{
		logEntrys:   logEntrys,
		tailTskMap:  make(map[string]*tailTask, 32),
		newConfChan: make(chan []*etcd.LogEntry), //设计成无缓冲的管道，只要没有新的配置来就会一直阻塞
	}
	//将当前的日志收集项保存起来
	for _, ev := range logEntrys {
		//将logEntrys中所有的日志收集项服务启动
		tailObj := NewTailObj(ev.Path, ev.Topic)
		//将这个启动的tailObj管理起来
		mapKey := fmt.Sprintf("%s_%s", ev.Path, ev.Topic)
		tskMgr.tailTskMap[mapKey] = tailObj
	}

	//开启goutinue，实时监听newConfChan这个channel
	go tskMgr.WatchNewConfChan()
}

// 监听newConfChan这个channel,进行及时的更新配置
func (t *tailTskMgr) WatchNewConfChan() {
	for {
		select {
		case newconfs := <-t.newConfChan:
			//说明有新的配置来了判断是否需要开启新的服务
			for _, conf := range newconfs {
				//与之前存在tailTskMap中的服务进行对比，看是否进行了改变
				mapKey := fmt.Sprintf("%s_%s", conf.Path, conf.Topic)
				_, ok := t.tailTskMap[mapKey]
				if ok {
					//当前服务配置项未发生改变

					continue
				} else {

					//说明新增了一个配置或是更新了一个配置，需要去开启对应的服务
					tsk := NewTailObj(conf.Path, conf.Topic)
					//将新增的服务保存进tailTskMap中
					t.tailTskMap[mapKey] = tsk
				}
			}

			//新配置中没有了之前的配置，需要删除配置并停止服务
			for _, oldConf := range t.logEntrys {
				isdelete := true
				for _, newConf := range newconfs {
					if oldConf.Path == newConf.Path && oldConf.Topic == newConf.Topic {
						fmt.Printf("oldconf:%v,newconf:%v\n", oldConf.Path, newConf.Path)
						//说明之前有，新的配置中也有
						isdelete = false
						break
					}
				}
				if isdelete {
					fmt.Printf("开始停止%v的服务\n", oldConf.Path)
					//说明需要删除
					mapkey := fmt.Sprintf("%s_%s", oldConf.Path, oldConf.Topic)
					t.tailTskMap[mapkey].cancleFunc() //取消服务
					//删除tailTskMap中对应的key-value
					fmt.Printf("需要停止的服务有%v\n", oldConf.Path)
					delete(t.tailTskMap, mapkey)
				}

			}
			fmt.Println("新配置来了...", newconfs)
		default:
			time.Sleep(time.Second)
		}
	}
}

// 向外部暴露自己内部的一个只写管道
func OutNewConfChannel() chan<- []*etcd.LogEntry {
	return tskMgr.newConfChan
}
