package filequeue

import (
	"bytes"
	"github.com/jingyanbin/basal"
	"os"
	"sync"
	"sync/atomic"
)

const byteMB1 = 1024 * 1024 //1MB
const msgSeparator = "\n"   //默认分隔符
const msgEOF byte = 27

//pusher 配置
type configDataPusher struct {
	configDataBase
	pushChanSize   int   //pusher 缓冲chan大小
	msgFileMaxByte int64 //消息文件最大字节

	//数据
	index int64 //push 消息文件的index
}

func (m *configDataPusher) Next() error {
	index := m.index + 1
	err := m.configDataBase.Save(false, index)
	if err == nil {
		m.index = index
	}
	return err
}

func (m *configDataPusher) Load() error {
	nums, err := m.configDataBase.Load(1)
	if err != nil {
		return err
	}
	if len(nums) >= 1 {
		m.index = nums[0]
		return nil
	} else {
		m.Save()
	}
	return nil
}

func (m *configDataPusher) Save() error {
	return m.configDataBase.Save(false, m.index)
}

//文件队列入队器
type FileQueuePusher struct {
	conf   configDataPusher //配置数据
	ch     chan []byte      //缓冲
	closed int32            //关闭状态
	wg     sync.WaitGroup   //关闭等待组
	f      *os.File         //当前写入文件
}

//当前大小
func (m *FileQueuePusher) size() (size int64, err error) {
	if err = m.reopen(false); err != nil {
		return 0, err
	}
	var fi os.FileInfo
	if fi, err = m.f.Stat(); err != nil {
		if err = m.reopen(true); err != nil {
			return 0, err
		}
		if fi, err = m.f.Stat(); err != nil {
			return 0, err
		}
	}
	return fi.Size(), nil
}

func (m *FileQueuePusher) write(buf []byte) (n int, err error) {
	size := len(buf)
	var nn int
	for n < size && err == nil {
		nn, err = m.f.Write(buf[n:])
		n += nn
	}
	return
}

func (m *FileQueuePusher) reopen(force bool) error {
	if m.f == nil || force {
		if m.f != nil {
			m.f.Sync()
			m.f.Close()
		}
		f, err := basal.OpenFileB(m.conf.GetMsgFileName(m.conf.index), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			m.f = nil
			return err
		}
		m.f = f
	}
	return nil
}

func (m *FileQueuePusher) push(data []byte) (err error) {
	var size int64
	if size, err = m.size(); err != nil {
		return err
	}
	if m.conf.msgFileMaxByte > 0 && size > m.conf.msgFileMaxByte {
		if _, err = m.write([]byte{msgEOF}); err != nil {
			return err
		}
		if err = m.conf.Next(); err != nil {
			return err
		}
		if err = m.reopen(true); err != nil {
			return err
		}
	}
	dLen := len(data)
	if dLen > 0 {
		if !bytes.HasSuffix(data, m.conf.sep) {
			data = append(data, m.conf.sep...)
		}
	} else {
		data = append(data, m.conf.sep...)
	}

	var n int
	if n, err = m.write(data); err != nil {
		if err = m.reopen(true); err != nil {
			return err
		}
		_, err = m.write(data[n:])
	}
	return err
}

func (m *FileQueuePusher) exit() {
	defer m.wg.Done()
	m.conf.Sync()
	m.conf.Close()
	if m.f != nil {
		m.f.Sync()
		m.f.Close()
	}
}

func (m *FileQueuePusher) run() {
	defer m.exit()
	var err error
	for data := range m.ch {
		err = m.push(data)
		if err != nil {
			log.ErrorF("FileQueuePusher run push error: %v, data: %v", err, string(data))
		}
	}
}

func (m *FileQueuePusher) Close() {
	if atomic.CompareAndSwapInt32(&m.closed, 0, 1) {
		close(m.ch)
	}
	m.Wait()
	return
}

func (m *FileQueuePusher) Wait() {
	m.wg.Wait()
}

//设置分割符
func (m *FileQueuePusher) SetSeparator(sep string) {
	m.conf.sep = []byte(sep)
}

//设置文件最大占用 单位MB
func (m *FileQueuePusher) SetMaxSize(maxMb int64) {
	m.conf.msgFileMaxByte = maxMb * byteMB1
}

func (m *FileQueuePusher) Push(data []byte) (err error) {
	defer basal.Exception(func(stack string, e error) {
		err = e
	})
	//if pushLen := len(m.ch); pushLen >= m.conf.pushChanSize {
	//	log.ErrorF("FileQueue Push full: %d", pushLen)
	//}
	m.ch <- data
	return
}

func (m *FileQueuePusher) PushString(data string) error {
	return m.Push([]byte(data))
}

func NewFileQueuePusher(confDataDir, msgFileDir, msgFilePrefix string, msgFileMaxMB int64, pushChanSize int) (*FileQueuePusher, error) {
	pusher := &FileQueuePusher{}
	pusher.conf.confDataDir = confDataDir
	pusher.conf.msgFileDir = msgFileDir
	pusher.conf.msgFilePrefix = msgFilePrefix
	pusher.conf.sep = []byte(msgSeparator)
	pusher.conf.filename = basal.Path.ProgramDirJoin(confDataDir, basal.Sprintf("%s_push.fq", msgFilePrefix))
	pusher.conf.msgFileMaxByte = msgFileMaxMB * byteMB1
	pusher.conf.pushChanSize = pushChanSize

	err := pusher.conf.Load()
	if err != nil {
		return nil, err
	}
	pusher.ch = make(chan []byte, pushChanSize)
	pusher.wg.Add(1)
	go pusher.run()
	return pusher, nil
}
