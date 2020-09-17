//go:generate go run tools/imports.go SchemaType
package main

import (
	"encoding/json"
	_ "github.com/sirupsen/logrus"
	"github.com/skhatri/elastics3/model"
	"github.com/skhatri/elastics3/tasks/elastic"
	"github.com/skhatri/elastics3/tasks/s3client"
	"github.com/skhatri/elastics3/utils"
	"log"
)

func main() {
	file, err := utils.Load()
	if err != nil {
		log.Fatal("config file required", err)
	}
	var cfg model.ElasticS3Config
	json.NewDecoder(file).Decode(&cfg)
	elastic.IndexFile(cfg)
	fileName, err := elastic.DumpElasticIndexDataToFile(cfg)
	if fileName != nil {
		s3client.UploadToS3(*fileName, cfg)
	}
}
