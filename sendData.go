package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/kingsoft-wps/go/log"
)

func sendData(data []*MetaData) ([]byte, error) {

	js, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	log.Debug("Send to %s, size: %d", cfg.FalconClient, len(data))
	for _, m := range data {
		log.Debug("%s", m)
	}

	res, err := http.Post(cfg.FalconClient, "Content-Type: application/json", bytes.NewBuffer(js))
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	return ioutil.ReadAll(res.Body)
}
