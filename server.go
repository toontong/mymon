package main

import (
	"github.com/kingsoft-wps/go/log"
	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/native"
)

func GlobalStatus(m *MysqlIns, db mysql.Conn) ([]*MetaData, error) {
	log.Info("execute `SHOW /*!50001 GLOBAL */ STATUS`")
	return mysqlState(m, db, "SHOW /*!50001 GLOBAL */ STATUS")
}

func GlobalVariables(m *MysqlIns, db mysql.Conn) ([]*MetaData, error) {
	log.Info("execute `SHOW /*!50001 GLOBAL */ VARIABLES`")
	return mysqlState(m, db, "SHOW /*!50001 GLOBAL */ VARIABLES")
}

func mysqlState(m *MysqlIns, db mysql.Conn, sql string) ([]*MetaData, error) {
	rows, _, err := db.Query(sql)
	if err != nil {
		return nil, err
	}

	data := make([]*MetaData, len(rows))
	i := 0
	for _, row := range rows {
		key_ := row.Str(0)
		if wsrepStatus, ok := WsrepStatusToConvert[key_]; ok {
			originVal := row.Str(1)
			convertVal, ok := wsrepStatus[originVal];
			if !ok {
				log.Debug("noo expected key-val=[%v]->[%v]",
					key_, originVal)
				continue
			}

			data[i] = NewMetric(key_)
			data[i].SetValue(convertVal)
		} else {
			v, err := row.Int64Err(1)
			// Ignore non digital value
			if err != nil {
				log.Debug("noo digital key-val=[%v]->[%v]",
					key_, row.Str(1))
				continue
			}

			data[i] = NewMetric(key_)
			data[i].SetValue(v)
		}
		i++
	}
	return data[:i], nil
}
