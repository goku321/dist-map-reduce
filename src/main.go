package main

import (
	"github.com/goku321/dist-map-reduce/master"

	log "github.com/sirupsen/logrus"
)

func main() {
	log.SetFormatter(&log.JSONFormatter{})
	m := master.NewMaster()
	m.StartServer()
}
