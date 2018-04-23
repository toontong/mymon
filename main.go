// MySQL Performance Monitor(For open-falcon)
// Write by Li Bin<libin_dba@xiaomi.com>
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	goconf "github.com/akrennmair/goconf"
	"github.com/kingsoft-wps/go/log"
	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/native"
)

type Cfg struct {
	LogFile      string
	LogLevel     int
	FalconClient string
	Endpoint     string

	User string
	Pass string
	Host string
	Port int

	//是否 Percona 的ProxySQL
	ProxySQL bool
}

var cfg Cfg

func init() {
	log.SetLevel(2) //Info=2,Warn=3

	var cfgFile string
	flag.StringVar(&cfgFile, "c", "mymon.cfg", "myMon configure file")
	flag.Parse()

	if _, err := os.Stat(cfgFile); err != nil {
		if os.IsNotExist(err) {
			println("Usage: mymon -c mymon.cfg")
			return
		}
	}

	log.Info("using configure file[%v]", cfgFile)
	if err := cfg.readConf(cfgFile); err != nil {
		log.Error("Read configure file error=[%v]", err)
		println("Usage: mymon -c ./etc/mon.cfg")
		return
	}

	log.Info("Config=%+v", cfg)

	if cfg.LogFile != "" {
		log.Info("set log-file=[%v] level=[%v]", cfg.LogFile, cfg.LogLevel)
		maxBytes, backupCount := 1024*1024*1024, 3 // 1G * 3
		hdlr, err := log.NewRotatingFileHandler(cfg.LogFile, maxBytes, backupCount)
		if err == nil {
			log.Info("all log will Output to File[%v]", cfg.LogFile)
			log.SetDefaultLogger(log.NewDefault(hdlr))
		} else {
			panic(err)
			log.Error("NewRotatingFile() err=%v", err)
		}
	}

	log.SetLevel(cfg.LogLevel) //Info=2,Warn=3
}

func (conf *Cfg) readConf(file string) error {
	c, err := goconf.ReadConfigFile(file)
	if err != nil {
		return err
	}

	conf.LogFile, err = c.GetString("default", "log_file")
	if err != nil {
		return err
	}

	conf.LogLevel, err = c.GetInt("default", "log_level")
	if err != nil {
		return err
	}

	conf.FalconClient, err = c.GetString("default", "falcon_client")
	if err != nil {
		return err
	}

	conf.Endpoint, err = c.GetString("default", "endpoint")
	if err != nil {
		return err
	}

	conf.User, err = c.GetString("mysql", "user")
	if err != nil {
		return err
	}

	conf.Pass, err = c.GetString("mysql", "password")
	if err != nil {
		return err
	}

	conf.Host, err = c.GetString("mysql", "host")
	if err != nil {
		return err
	}

	conf.Port, err = c.GetInt("mysql", "port")
	var nPorxySQL int = 0
	nPorxySQL, err = c.GetInt("mysql", "proxysql")
	conf.ProxySQL = (nPorxySQL == 1)
	return err
}

func timeout() {
	time.AfterFunc(TIME_OUT*time.Second, func() {
		log.Error("Execute timeout")
		os.Exit(1)
	})
}

func MysqlAlive(m *MysqlIns, ok bool) {
	data := NewMetric("mysql_alive_local")
	if ok {
		data.SetValue(1)
	}
	msg, err := sendData([]*MetaData{data})
	if err != nil {
		log.Error("Send alive data failed: %v", err)
		return
	}
	log.Info("Alive data response %s: %s", m.String(), string(msg))
}

func FetchData(m *MysqlIns) (err error) {
	defer func() {
		MysqlAlive(m, err == nil)
	}()

	log.Info("Try Connect MySQL(%s:%d?user=%s&pass=%s)",
		cfg.Host, cfg.Port, cfg.User, cfg.Pass)

	db := mysql.New("tcp", "", fmt.Sprintf("%s:%d", m.Host, m.Port),
		cfg.User, cfg.Pass)
	db.SetTimeout(500 * time.Millisecond)
	if err = db.Connect(); err != nil {
		log.Error("Connect(%s:%d?user=%s&pass=%s) fail err=[%s].",
			m.Host, m.Port, cfg.User, cfg.Pass, err)
		return
	}
	defer db.Close()

	data := make([]*MetaData, 0)
	/*-------------------------------------------------

	slaveState_test, err := slaveStatus(m, db)
	if err != nil {
		return
	}

	data = append(data, slaveState_test...)
	if true {
		for _, r := range data {
			println(fmt.Sprintf("slave-status : %v", r))
		}
		return
	}
	// ------------------------------------------------*/

	if cfg.ProxySQL {
		globalStatus, err := ProxySQLGlobalStatus(m, db)
		if err != nil {
			log.Error("ProxySQLGlobalStatus() err=%v", err)
			return err
		}
		data = append(data, globalStatus...)
		globalStatus, err = ProxySQLConnectionPoolStats(m, db)
		if err != nil {
			log.Error("ProxySQLConnectionPoolStats() err=%v", err)
			return err
		}
		data = append(data, globalStatus...)
	} else {
		globalStatus, err := GlobalStatus(m, db)
		if err != nil {
			log.Error("GlobalStatus() err=%v", err)
			return err
		}
		data = append(data, globalStatus...)

		globalVars, err := GlobalVariables(m, db)
		if err != nil {
			log.Error("GlobalVariables() err=%v", err)
			return err
		}
		data = append(data, globalVars...)

		innodbState, err := innodbStatus(m, db)
		if err != nil {
			log.Error("innodbStatus() err=%v", err)
			return err
		}
		data = append(data, innodbState...)

		slaveState, err := slaveStatus(m, db)
		if err != nil {
			log.Error("slaveStatus() err=%v", err)
			return err
		}
		data = append(data, slaveState...)
	}
	msg, err := sendData(data)
	if err != nil {
		log.Error("sendData() err=%v", err)
		return err
	}
	log.Info("Send response %s: %s", m.String(), string(msg))
	return
}

func (m *MysqlIns) String() string {
	return fmt.Sprintf("%s:%d", m.Host, m.Port)
}

func main() {
	go timeout()

	err := FetchData(&MysqlIns{
		Host: cfg.Host,
		Port: cfg.Port,
		Tag:  fmt.Sprintf("port=%d", cfg.Port),
	})
	if err != nil {
		log.Error("main exit err=%v", err)
	}
	time.Sleep(time.Microsecond * 200)
	log.Close()

}
