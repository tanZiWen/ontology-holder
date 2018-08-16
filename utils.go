package main

import (
	"encoding/json"
	"fmt"
	"github.com/ontio/ontology/common"
	"io/ioutil"
	"os"
	"sync"
)

type OneThreadExecLock struct {
	isWorking bool
	lock      sync.Mutex
}

func NewOneThreadExecLock() *OneThreadExecLock {
	return &OneThreadExecLock{}
}

func (this *OneThreadExecLock) TryLock() bool {
	this.lock.Lock()
	defer this.lock.Unlock()
	if this.isWorking {
		return false
	}
	this.isWorking = true
	return true
}

func (this *OneThreadExecLock) Release() {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.isWorking = false
}

func GetJsonObject(filePath string, jsonObject interface{}) error {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
	if err != nil {
		return err
	}
	defer file.Close()
	data, err := ioutil.ReadAll(file)
	if err != nil {
		return fmt.Errorf("ioutil.ReadAll %s error %s", filePath, err)
	}
	err = json.Unmarshal(data, jsonObject)
	if err != nil {
		return fmt.Errorf("json.Unmarshal %s error %s", data, err)
	}
	return nil
}

func SystemContractAddressTransfer(contractAddress string) string {
	switch contractAddress {
	case ONT_CONTRACT_ADDRESS_BASE58:
		return ONT_CONTRACT_ADDRESS
	case ONG_CONTRACT_ADDRESS_BASE58:
		return ONG_CONTRACT_ADDRESS
	case GOVERNANCE_CONTRACT_ADDRESS_BASE58:
		return GOVERNANCE_CONTRACT_ADDRESS
	default:
		return contractAddress
	}
}

func GetAddress(address string) (common.Address, error) {
	addr, err := common.AddressFromBase58(address)
	if err == nil {
		return addr, nil
	}
	addr, err = common.AddressFromHexString(address)
	if err == nil {
		return addr, nil
	}
	return common.ADDRESS_EMPTY, fmt.Errorf("invalid address")
}
