package main

import (
	log4 "github.com/alecthomas/log4go"
	ontsdk "github.com/ontio/ontology-go-sdk"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

var (
	CfgPath       = "./config.json"
	LogPath       = "./log4go.xml"
	DBInstallFile = "./install.sql"
)

func main() {
	defer time.Sleep(time.Millisecond * 10)
	runtime.GOMAXPROCS(runtime.NumCPU())
	log4.LoadConfiguration(LogPath)

	err := GetJsonObject(CfgPath, DefConfig)
	if err != nil {
		log4.Error("Init config error:%s", err)
		return
	}

	mySqlHelper := NewMySqlHelper()
	err = mySqlHelper.Open(DefConfig.MysqlDataSourceName)
	if err != nil {
		log4.Error("Open mysql error:%s", err)
		return
	}

	err = mySqlHelper.InitDB(DBInstallFile)
	if err != nil {
		log4.Error("InitDB error:%s", err)
		return
	}

	ontSdk := ontsdk.NewOntologySdk()
	rpcClient := ontSdk.NewRpcClient().SetAddress(DefConfig.OntologyRpcAddress)
	ontSdk.SetDefaultClient(rpcClient)

	DefOntologyMgr = NewOntologyManager(ontSdk, mySqlHelper)
	err = DefOntologyMgr.Start()
	if err != nil {
		log4.Error("DefOntologyMgr Start error:%s", err)
		return
	}
	defer DefOntologyMgr.Close()

	DefHttpSvr.Start(DefConfig.HttpServerPort)

	waitToExit()
}

func Get(p int, helper *MySqlHelper) {
	size := 50
	from := (p - 1) * size
	holders, err := helper.GetAssetHolder(from, size, ONT_CONTRACT_ADDRESS)
	if err != nil {
		log4.Info("GetAssetHolder error:%s", err)
		return
	}
	for _, holder := range holders {
		log4.Info("%+v", holder)
	}
}

func waitToExit() {
	exit := make(chan bool, 0)
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go func() {
		for sig := range sc {
			log4.Info("Ontology received exit signal:%v.", sig.String())
			close(exit)
			break
		}
	}()
	<-exit
}
