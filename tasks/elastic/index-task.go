package elastic

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/skhatri/elastics3/model"
	"github.com/skhatri/elastics3/schema"
	"io"
	"log"
	"os"
	"time"
)

func IndexFile(cfg model.ElasticS3Config) {
	if !cfg.Tasks.Index {
		return
	}
	file, err := os.Open(cfg.Input)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	elasticClient, err := newElasticClient(cfg.ElasticSearch)

	if err != nil {
		log.Fatal("elasticsearch client error", err)
	}
	indexName := cfg.ElasticSearch.Index
	EnsureIndexExists(cfg, elasticClient)

	var total = 0

	start := time.Now()

	reader := bufio.NewReader(file)
	skip := 0
	limit := 100
	var success = 0
	var failure = 0
	for {
		item, e := reader.ReadString('\n')
		if e == io.EOF {
			break
		}
		if e != nil {
			fmt.Println("error reading file", file.Name())
			break
		}
		total = total + 1

		if total < skip {
			continue
		}
		var record schema.SchemaType
		json.NewDecoder(bytes.NewBufferString(item)).Decode(&record)
		payload := IndexPayload{
			Index: indexName, Key: record.GetKey(), Data: item,
		}
		indexed := IndexDocument(context.TODO(), payload, elasticClient)
		if indexed {
			success = success + 1
		} else {
			failure = failure + 1
		}

		if limit > 0 && total == limit {
			break
		}
		if total%100 == 0 {
			fmt.Printf("total=%d, ✅ success=%d, ❌ failure=%d\n", total, success, failure)
		}
		if total > 4800 {
			fmt.Printf("total=%d, ✅ success=%d, ❌ failure=%d\n", total, success, failure)
		}
	}
	fmt.Println("Queued", total)

	fmt.Println("total records", total)
	fmt.Println("indexed", success)
	fmt.Printf("time taken %f seconds\n", time.Since(start).Seconds())

}
