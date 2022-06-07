package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
)

func simpleGet(subPath string) int64 { // just return int to verify
	res, err := http.Get(fmt.Sprintf("http://%s%s", HostPort, subPath))
	if err != nil {
		panic(fmt.Sprintf("simpleGet failed : %s", err))
	}
	defer res.Body.Close()
	rawBody, err := ioutil.ReadAll(res.Body)
	if err != nil {
		panic(fmt.Sprintf("simpleGet read failed : %s", err))
	}
	result, _ := strconv.ParseInt(string(rawBody), 10, 64)
	return result
}

func getID(id int64) int64 {
	return simpleGet(fmt.Sprintf("/get_id/%d", id))
}

func getSerial(id int64) int64 {
	return simpleGet(fmt.Sprintf("/get_serial/%d", id))
}

func getAggregateID(id int64) int64 {
	return simpleGet(fmt.Sprintf("/get_aggregate_id/%d", id))
}

func getAggregateSerial(id int64) int64 {
	return simpleGet(fmt.Sprintf("/get_aggregate_serial/%d", id))
}
