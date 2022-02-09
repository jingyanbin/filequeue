package filequeue

import (
	"github.com/jingyanbin/basal"
	"github.com/jingyanbin/datetime"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

var log = basal.GetLogger()

func SetLogger(logger basal.ILogger) {
	log = logger
}

//默认配置数据目录
const configDataDir = "file_queue"

//配置数据基础结构
type configDataBase struct {
	confDataDir   string   //配置数据目录
	msgFileDir    string   //消息文件目录
	msgFilePrefix string   //消息文件前缀
	filename      string   //配置文件名
	sep           []byte   //数据元素分隔符
	f             *os.File //配置文件
}

//消息文件名
func (m *configDataBase) GetMsgFileName(index int64) string {
	name := basal.Sprintf("%s.%d", m.msgFilePrefix, index)
	fp := basal.Path.ProgramDirJoin(m.msgFileDir, name)
	return fp
}

//同步
func (m *configDataBase) Sync() error {
	if m.f == nil {
		return nil
	}
	return m.f.Sync()
}

//关闭
func (m *configDataBase) Close() error {
	if m.f == nil {
		return nil
	}
	return m.f.Close()
}

//重新打开
func (m *configDataBase) reopen(force bool) error {
	if m.f == nil || force {
		if m.f != nil {
			m.f.Sync()
			m.f.Close()
		}
		f, err := basal.OpenFileB(m.filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			m.f = nil
			return err
		}
		m.f = f
	}
	return nil
}

//加载配置参数
func (m *configDataBase) Load(numCount int) ([]int64, error) {
	data, err := ioutil.ReadFile(m.filename)
	if err != nil {
		if os.IsNotExist(err) {
			//log.ErrorF("不存在配置: %v", err)
			return nil, nil
		} else {
			return nil, err
		}
	}
	if nums := datetime.NewNextNumber(string(data)).Numbers(); len(nums) >= numCount {
		result := make([]int64, 0, len(nums))
		for _, n := range nums {
			result = append(result, int64(n))
		}
		return result, nil
	} else {
		return nil, basal.NewError("load config file num len error: %v", string(data))
	}
}

//保存配置数据
func (m *configDataBase) Save(clear bool, nums ...int64) (err error) {
	if err = m.reopen(false); err != nil {
		return err
	}
	data := make([]string, 0, len(nums))
	for _, n := range nums {
		data = append(data, strconv.FormatInt(n, 10))
	}
	dataStr := strings.Join(data, ",")
	//log.ErrorF("data: %v, %v", dataStr, nums)
	if clear {
		if err = m.f.Truncate(0); err != nil {
			return err
		}
	}
	_, err = m.f.WriteAt([]byte(dataStr), 0)
	if err != nil {
		if err = m.reopen(true); err != nil {
			return err
		}
		if clear {
			if err = m.f.Truncate(0); err != nil {
				return err
			}
		}
		_, err = m.f.WriteAt([]byte(dataStr), 0)
	}
	return err
}
