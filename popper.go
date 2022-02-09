package filequeue

import (
	"github.com/jingyanbin/basal"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const popperBufSize = 4096

//popper 配置
type configDataPopper struct {
	configDataBase
	deletePopped bool //是否删除已经处理后的文件
	//数据
	index  int64 //pop 消息文件的index
	offset int64 //pop 消息文件内容偏移量
}

func (m *configDataPopper) Load() error {
	nums, err := m.configDataBase.Load(2)
	if err != nil {
		return err
	}
	if len(nums) >= 2 {
		m.index = nums[0]
		m.offset = nums[1]
		return nil
	} else {
		m.Save()
	}
	return nil
}

func (m *configDataPopper) Save() error {
	if err := m.configDataBase.Save(false, m.index, m.offset); err != nil {
		return err
	}
	return nil
}

func (m *configDataPopper) SaveEx(index, offset int64) error {
	if err := m.configDataBase.Save(true, index, offset); err != nil {
		return err
	}
	m.index = index
	m.offset = offset
	return nil
}

//弹出处理函数
//data:数据
//popped:已弹出
type PopHandler func(data []byte) (popped bool, exit bool)

//元素分割函数
type SplitHandler func(lines []byte, r int, sep []byte) (pos int, nextStart int, ok bool, nextFile bool)

//confDir, msgFileDir, msgFilePrefix string, msgFileMaxMB int64, pushChanSize int, deletePopped bool

//默认 元素分割函数
//pos 元素结束位
//ok 是否分割成功
//nextStart 下一个开始位置
func splitHandlerLine(lines []byte, r int) (pos int, nextStart int, ok bool) {
	if lines[r] == '\n' {
		if r > 0 && lines[r-1] == '\r' {
			pos = r - 1
		} else {
			pos = r
		}
		return pos, r + 1, true
	} else {
		return 0, 0, false
	}
}

func splitHandlerNormal(lines []byte, r int, sep []byte) (pos int, nextStart int, ok bool, nextFile bool) {
	if lines[r] == msgEOF { //数据末尾
		//log.ErrorF("数据末尾: %v", lines[r])
		return 0, 0, false, true
	}
	sepLen := len(sep)
	if sepLen == 1 {
		if sep[0] == '\n' { //分割行
			pos, nextStart, ok = splitHandlerLine(lines, r)
			return pos, nextStart, ok, false
		} else if sep[0] == lines[r] {
			return r, r + 1, true, false
		} else {
			return 0, 0, false, false
		}
	} else {
		linesLen := len(lines)
		if linesLen < sepLen {
			return 0, 0, false, false
		}
		for i := 0; i < sepLen; i++ {
			if lines[r-i] != sep[sepLen-1-i] {
				return 0, 0, false, false
			}
		}
		pos = r - (sepLen - 1)
		return pos, r + 1, true, false
	}
}

//文件队列弹出器
type FileQueuePopper struct {
	conf       configDataPopper //配置数据
	f          *os.File         //队列数文件
	fileOffset int64            //文件偏移量
	buf        []byte           //临时读取缓冲
	lines      []byte           //已读出数据 未出队
	r          int              //当前已读出下标
	pos        int              //待出队数据结束位置
	nextStart  int              //下一次开始位置
	canPop     bool             //是否已经有待出队数据
	nextFile   bool
	splitFunc  SplitHandler //分割函数
	closed     int32
	wg         sync.WaitGroup
	mu         sync.Mutex
}

func (m *FileQueuePopper) read() (n int, err error) {
	if m.nextFile {
		if !m.isExistNext() {
			//log.ErrorF("00000000000000")
			return 0, io.EOF //下一个文件不存在, 表示本文件读到EOF
		}
		if err = m.openNext(); err != nil {
			return 0, err //打开下一个文件失败,表示本文件读到EOF
		}
	} else {
		if err = m.reopen(false); err != nil {
			return 0, err
		}
	}

	n, err = m.f.Read(m.buf)
	if err != nil {
		if err == io.EOF && n == 0 {
			return 0, err
		}
		if err = m.reopen(true); err != nil {
			return 0, err
		}
		n, err = m.f.Read(m.buf)
	}
	if n > 0 {
		m.lines = append(m.lines, m.buf[:n]...)
	}
	return
}

func (m *FileQueuePopper) deleteMsgFile(index int64) {
	if m.conf.deletePopped {
		filename := m.conf.GetMsgFileName(index)
		os.Remove(filename)
	}
}

func (m *FileQueuePopper) closeFile() {
	if m.f != nil {
		m.f.Close()
		m.f = nil
	}
}

func (m *FileQueuePopper) isExistNext() bool {
	filename := m.conf.GetMsgFileName(m.conf.index + 1)
	has, err := basal.IsExist(filename)
	if err != nil {
		//log.ErrorF("FileQueuePopper isExistNext error: %v, %v", err, filename)
		return false
	}
	return has
}

func (m *FileQueuePopper) openNext() error {
	index := m.conf.index
	nextIndex := m.conf.index + 1
	f, err := basal.OpenFileB(m.conf.GetMsgFileName(nextIndex), os.O_RDONLY, 0666)
	if err != nil {
		return err
	}
	if err = m.conf.SaveEx(nextIndex, 0); err != nil {
		return err
	}
	m.closeFile()
	m.deleteMsgFile(index)
	return m.setFile(f)
}

func (m *FileQueuePopper) reopen(force bool) error {
	if m.f == nil || force {
		m.closeFile()
		f, err := basal.OpenFileB(m.conf.GetMsgFileName(m.conf.index), os.O_RDONLY, 0666)
		if err != nil {
			return err
		}
		return m.setFile(f)
	}
	return nil
}

func (m *FileQueuePopper) setFile(f *os.File) error {
	if _, err := f.Seek(m.conf.offset, io.SeekStart); err != nil {
		return err
	}
	if m.f != nil {
		m.f.Close()
	}
	m.f = f
	m.lines = m.lines[:0]
	m.r = 0
	m.canPop = false
	m.nextFile = false
	m.pos = 0
	return nil
}

//当前偏移量 不是文件偏移量, 这里是成功出队后的偏移量
func (m *FileQueuePopper) Offset() int64 {
	return m.conf.offset
}

//设置偏移量
//func (m *FileQueuePopper) SetOffset(offset int64) (err error) {
//	_, err = m.f.Seek(offset, io.SeekStart)
//	if err == nil {
//		m.lines = m.lines[:0]
//		m.r = 0
//		m.conf.offset = offset
//		m.canPop = false
//		m.pos = 0
//	}
//	return
//}

//设置分割函数
func (m *FileQueuePopper) SetSplitHandler(f SplitHandler) {
	m.splitFunc = f
}

//丢弃队头数据 必须调用过 Front 有数据才能丢弃
func (m *FileQueuePopper) DiscardFront() bool {
	if m.canPop {
		m.canPop = false
		m.r = 0
		m.lines = m.lines[m.nextStart:]
		m.conf.offset += int64(m.nextStart)
		m.conf.Save()
		return true
	} else {
		return false
	}
}

func (m *FileQueuePopper) PopFrontBlock() (line []byte, ok bool) {
	interval := time.Millisecond
	var err error
	for atomic.LoadInt32(&m.closed) == 0 {
		if line, err = m.PopFront(); err == nil {
			return line, true
		} else if err == io.EOF {
			if interval < time.Second {
				interval += time.Millisecond * 200
			}
		} else {
			interval = time.Second
			log.ErrorF("FileQueuePopper PopFrontBlock error: %v", err)
		}
		if interval > 0 {
			time.Sleep(interval)
		}
	}
	return nil, false
}

//直接弹出队头数据
func (m *FileQueuePopper) PopFront() (line []byte, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if line, err = m.Front(); err == nil && m.canPop {
		m.DiscardFront()
	}
	return
}

//获得队头数据
func (m *FileQueuePopper) Front() (line []byte, err error) {
	if m.canPop {
		return m.lines[:m.pos], nil
	}
	var n int
	for {
		for ; (m.r < len(m.lines)) && (m.nextFile == false); m.r++ {
			m.pos, m.nextStart, m.canPop, m.nextFile = m.splitFunc(m.lines, m.r, m.conf.sep)
			if m.canPop {
				return m.lines[:m.pos], nil
			}
		}
		n, err = m.read()
		if n == 0 {
			return nil, err
		}
	}
}

func (m *FileQueuePopper) PopToHandler(handler PopHandler) {
	m.wg.Add(1)
	go m.popTo(handler)
}

func (m *FileQueuePopper) exit() {
	defer m.wg.Done()
	m.conf.Sync()
	m.conf.Close()
	if m.f != nil {
		m.f.Close()
	}
}

func (m *FileQueuePopper) popTo(handler PopHandler) {
	var interval time.Duration
	defer m.exit()
	var exit bool
	for atomic.LoadInt32(&m.closed) == 0 {
		m.mu.Lock()
		if data, err := m.Front(); err == nil {
			popped := false
			basal.Try(func() {
				popped, exit = handler(data)
			}, func(stack string, e error) {
				log.ErrorF("FileQueuePopper popTo error: %v, %v", err, string(data))
			})
			if exit {
				atomic.StoreInt32(&m.closed, 1)
			}
			if popped {
				m.DiscardFront()
				interval = 0
			} else {
				interval = time.Second
			}
		} else if err == io.EOF {
			if interval < time.Second {
				interval += time.Millisecond * 200
			}
		} else {
			interval = time.Second
			log.ErrorF("FileQueuePopper popTo error: %v", err)
		}
		m.mu.Unlock()
		if interval > 0 {
			time.Sleep(interval)
		}
	}
}

func (m *FileQueuePopper) Close() {
	atomic.CompareAndSwapInt32(&m.closed, 0, 1)
	m.Wait()
	return
}

func (m *FileQueuePopper) Wait() {
	m.wg.Wait()
}

func NewFileQueuePopper(confDataDir, msgFileDir, msgFilePrefix string, deletePopped bool) (*FileQueuePopper, error) {
	popper := &FileQueuePopper{}
	popper.conf.confDataDir = confDataDir
	popper.conf.msgFileDir = msgFileDir
	popper.conf.msgFilePrefix = msgFilePrefix
	popper.conf.sep = []byte(msgSeparator)
	popper.conf.filename = basal.Path.ProgramDirJoin(confDataDir, basal.Sprintf("%s_pop.fq", msgFilePrefix))
	popper.conf.deletePopped = deletePopped
	err := popper.conf.Load()
	if err != nil {
		return nil, err
	}
	popper.buf = make([]byte, popperBufSize)
	popper.lines = make([]byte, 0, popperBufSize)
	popper.splitFunc = splitHandlerNormal
	return popper, nil
}
