package taillog

import (
	"context"
	"fmt"
	"github.com/hpcloud/tail"
	"mylog/kafka"
	"time"
)

// 具体的日志收集项
type tailTask struct {
	Path       string
	Topic      string
	Instance   *tail.Tail      //具体收集项的tail对象
	ctx        context.Context //用于更新配置后，停止服务
	cancleFunc context.CancelFunc
}

// 获取一个具体的日志收集项 , 主要是为了方便后续的管理，将所有的*tailTask保存起来
func NewTailObj(path, topic string) (tsk *tailTask) {
	context, cancel := context.WithCancel(context.Background())
	tsk = &tailTask{
		Path:       path,
		Topic:      topic,
		ctx:        context,
		cancleFunc: cancel,
	}
	//填充Instance字段
	tsk.Init()
	return
}

// 初始化一个tailtsk工作项
func (t *tailTask) Init() {
	config := tail.Config{
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2}, //从文件的哪个地方开始读,
		ReOpen:    true,                                 //日志分割后从重新打开文件
		MustExist: false,                                //文件不存在不会报错，会等待日志出现
		Poll:      true,
		Follow:    true,
	}
	tailObj, err := tail.TailFile(t.Path, config)
	if err != nil {
		fmt.Println("get tails failed err:", err)
		return
	}
	t.Instance = tailObj
	//开启协程，在后台读取日志文件
	go t.readLogFile()
}

// readLogFile 去日志文件中读取信息
func (t *tailTask) readLogFile() {
	//循环读取日志
	for {
		select {
		case <-t.ctx.Done():
			//说明更新配置后，需要停止该服务
			return
		case line := <-t.Instance.Lines:
			//读取到信息后将信息发送给kafka外部暴露的管道
			kafka.SendToLogChan(t.Topic, line.Text)
		default:
			time.Sleep(time.Second)
		}
	}
}
