package main

import log "github.com/sirupsen/logrus"

func main() {
	log.SetFormatter(&log.JSONFormatter{})
}
