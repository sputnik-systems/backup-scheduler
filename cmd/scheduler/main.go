package main

import (
	"log"

	"github.com/sputnik-systems/backup-scheduler/pkg/cmd"
)

func main() {
	err := cmd.Execute()
	if err != nil {
		log.Fatalln(err)
	}
}
