package main

var WsrepStatusToConvert = map[string]map[string]int8{
	"wsrep_cluster_status": {"Primary": 1, "Non-Primary": 2, "Disconnected": 3}, // pxc集群状态  Primary / Non-Primary / Disconnected
	"wsrep_connected":      {"ON": 1, "OFF": 2},                                 // pxc节点是否连接到集群， ON / OFF
	"wsrep_ready":          {"ON": 1, "OFF": 2},                                 // pxc节点是否已经准备好接收query  ON / OFF
}
