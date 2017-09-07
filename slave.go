package main

import (
	"fmt"
	"github.com/kingsoft-wps/go/log"
	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/native"
)

var SlaveStatusToSend = []string{
	"Exec_Master_Log_Pos",
	"Read_Master_Log_Pos",
	"Relay_Log_Pos",
	"Seconds_Behind_Master",
	"Slave_IO_Running",
	"Slave_SQL_Running",
}

func slaveStatus(m *MysqlIns, db mysql.Conn) ([]*MetaData, error) {

	isSlave := NewMetric("Is_slave")

	rows, res, err := db.Query("SHOW SLAVE STATUS")
	if err != nil {
		return nil, err
	}

	// be master
	if rows == nil {
		isSlave.SetValue(0)
		return []*MetaData{isSlave}, nil
	}

	// be slave
	isSlave.SetValue(1)
	// MySQL-5.7 支持 parallel-slave 特性
	is_mutil_slave_thread := len(rows) > 1

	data := make([]*MetaData, len(SlaveStatusToSend))
	for idx, row := range rows {
		for i, s := range SlaveStatusToSend {
			data[i] = NewMetric(s)
			switch s {
			case "Slave_SQL_Running", "Slave_IO_Running":
				data[i].SetValue(0)
				v := row.Str(res.Map(s))
				if v == "Yes" {
					data[i].SetValue(1)
				}
			default:
				v, err := row.Int64Err(res.Map(s))
				if err != nil {
					data[i].SetValue(-1)
				} else {
					data[i].SetValue(v)
				}
			}
			// the first default slave thread do not change.
			if idx > 0 && is_mutil_slave_thread {
				channel_name := row.Str(res.Map("Channel_Name"))
				if channel_name == "" {
					log.Error("error on parse Channel_Name was empty.", err)
				} else {
					data[i].Endpoint = fmt.Sprintf("%s-%s", data[i].Endpoint, channel_name)
					log.Info("MTS of Channel_Name=%v change Metric.Endponit.", channel_name)
				}
			}
		}

	} //End ranges rows
	return append(data, isSlave), nil
}
