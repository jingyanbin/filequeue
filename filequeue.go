package filequeue

type FileQueue struct {
	pusher *FileQueuePusher
	popper *FileQueuePopper
}

func (m *FileQueue) ClosePusher() {
	m.pusher.Close()
}

func (m *FileQueue) ClosePopper() {
	m.popper.Close()
}

func (m *FileQueue) Close() {
	m.pusher.Close()
	m.popper.Close()
}

func (m *FileQueue) Wait() {
	m.pusher.Wait()
	m.popper.Wait()
}

func (m *FileQueue) Push(data []byte) error {
	return m.pusher.Push(data)
}

func (m *FileQueue) PopToHandler(handler PopHandler) {
	m.popper.PopToHandler(handler)
}

func (m *FileQueue) Pop() (data []byte, ok bool) {
	return m.popper.PopFrontBlock()
}

//创建文件消息队列
//confDataDir 配置文件路径
//msgFileDir 消息文件目录
//msgFilePrefix 消息文件前缀
//msgFileMaxSize 消息文件最大占用空间 单位MB
//pushChanSize 入队缓冲大小
//deletePopped 删除已出队的文件
func NewFileQueue(confDataDir, msgFileDir, msgFilePrefix string, msgFileMaxMB int64, pushChanSize int, deletePopped bool) (*FileQueue, error) {
	q := &FileQueue{}
	if confDataDir == "" {
		confDataDir = configDataDir
	}
	if pusher, err := NewFileQueuePusher(confDataDir, msgFileDir, msgFilePrefix, msgFileMaxMB, pushChanSize); err != nil {
		return nil, err
	} else {
		q.pusher = pusher
	}
	if popper, err := NewFileQueuePopper(confDataDir, msgFileDir, msgFilePrefix, deletePopped); err != nil {
		return nil, err
	} else {
		q.popper = popper
	}
	return q, nil
}
