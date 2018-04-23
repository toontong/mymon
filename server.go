package main

import (
	"fmt"
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

func ProxySQLGlobalStatus(m *MysqlIns, db mysql.Conn) ([]*MetaData, error) {
	log.Info("ProxySQL execute `SELECT * FROM stats_mysql_global;`")
	return mysqlState(m, db, "SELECT * FROM stats_mysql_global")
}

func ProxySQLConnectionPoolStats(m *MysqlIns, db mysql.Conn) ([]*MetaData, error) {
	log.Info("`SELECT * FROM stats_mysql_connection_pool`")
	sql := `SELECT * FROM stats_mysql_connection_pool`
	/***************************
	  Admin> select * from stats_mysql_connection_pool;
	  +-----------+--------------+----------+--------------+----------+----------+--------+---------+---------+-----------------+-----------------+------------+
	  | hostgroup | srv_host     | srv_port | status       | ConnUsed | ConnFree | ConnOK | ConnERR | Queries | Bytes_data_sent | Bytes_data_recv | Latency_us |
	  +-----------+--------------+----------+--------------+----------+----------+--------+---------+---------+-----------------+-----------------+------------+
	  | 1001      | 172.16.8.220 | 3306     | ONLINE       | 0        | 241      | 0      | 0       | 1163570 | 80144356        | 9129422         | 74         |
	  | 1001      | 172.16.9.169 | 3306     | ONLINE       | 0        | 1        | 0      | 0       | 1133    | 77971           | 7032            | 144        |
	  | 1001      | 172.16.8.221 | 3306     | OFFLINE_HARD | 0        | 0        | 0      | 0       | 0       | 0               | 0               | 0          |
	  | 1501      | 172.16.9.169 | 3306     | ONLINE       | 57       | 0        | 470    | 0       | 674712  | 106308454       | 20586234        | 144        |
	  | 2001      | 172.16.8.245 | 3306     | ONLINE       | 0        | 1499     | 0      | 0       | 0       | 0               | 0               | 135        |
	  | 2101      | 172.16.9.232 | 3306     | OFFLINE_SOFT | 0        | 0        | 0      | 0       | 0       | 0               | 0               | 0          |
	  | 2101      | 172.16.8.246 | 3306     | ONLINE       | 0        | 8        | 0      | 0       | 22922   | 1481297         | 1302399         | 102        |
	  | 2101      | 172.16.8.245 | 3306     | ONLINE       | 0        | 1        | 0      | 0       | 23      | 1502            | 692             | 135        |
	  +-----------+--------------+----------+--------------+----------+----------+--------+---------+---------+-----------------+-----------------+------------+
	  ****************************/
	rows, _, err := db.Query(sql)
	if err != nil {
		log.Error("ProxySQLConnectionPoolStats() err=[%v]", err)
		return nil, err
	}
	data := make([]*MetaData, len(rows)*8)
	j := 0

	_append := func(host, key, typ string, val int64) {
		k := fmt.Sprintf("%v/%v", host, key)
		data[j] = NewMetric(k)
		data[j].SetValue(val)
		j++
	}
	i := 0
	for i = 0; i < len(rows); i++ {
		row := rows[i]

		host := fmt.Sprintf("%v:%v", row.Str(1), row.Str(2))
		ConnUsed, _ := row.Int64Err(4)
		_append(host, "ConnUsed", ORIGIN, ConnUsed)

		ConnFree, _ := row.Int64Err(5)
		_append(host, "ConnFree", ORIGIN, ConnFree)

		ConnOK, _ := row.Int64Err(6)
		_append(host, "ConnOK", ORIGIN, ConnOK)

		ConnERR, _ := row.Int64Err(7)
		_append(host, "ConnERR", ORIGIN, ConnERR)
		Queries, _ := row.Int64Err(8)
		_append(host, "Queries", DELTA_PS, Queries)
		Bytes_data_sent, _ := row.Int64Err(9)
		_append(host, "Bytes_data_sent", DELTA_PS, Bytes_data_sent)
		Bytes_data_recv, _ := row.Int64Err(10)
		_append(host, "Bytes_data_recv", DELTA_PS, Bytes_data_recv)
		Latency_us, _ := row.Int64Err(11)
		_append(host, "Latency_us", ORIGIN, Latency_us)
	}
	return data[:i], nil
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
			convertVal, ok := wsrepStatus[originVal]
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
