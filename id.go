package main

import (
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var NodeId uint32

func GenNodeId() uint32 {
	return uint32(rand.Int31n(1000000))
}

func InitNodeId(file string) (uint32, error) {
	if !IsFileExisted(file) {
		NodeId = GenNodeId()
		err := SaveJsonObject(file, NodeId)
		if err != nil {
			return 0, err
		}
		return NodeId, nil
	}
	err := GetJsonObject(file, &NodeId)
	if err != nil {
		return 0, err
	}
	return NodeId, nil
}
