package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	elasticsearch7 "github.com/elastic/go-elasticsearch/v7"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/skhatri/elastics3/model"
	"github.com/skhatri/elastics3/schema"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"
)

type IndexPayload struct {
	Index string
	Key   string
	Data  string
}

func NewElasticClient(elasticConfig model.ElasticSearchConfig) *elasticsearch7.Client {
	client, _ := newElasticClient(elasticConfig)
	return client
}

func newElasticClient(elasticConfig model.ElasticSearchConfig) (*elasticsearch7.Client, error) {
	elasticCfg := elasticsearch7.Config{
		Addresses: []string{
			elasticConfig.Host,
		},
	}

	if elasticConfig.Username != nil && elasticConfig.Password != nil {
		password := *elasticConfig.Password

		if strings.Index(password, "file:") == 0 {
			passwordData, err := ioutil.ReadFile(strings.Replace(password, "file:", "", -1))
			if err != nil {
				log.Fatal("could not read password file", err)
			}
			password = string(passwordData)
		}
		elasticCfg.Username = *elasticConfig.Username
		elasticCfg.Password = password
	}

	return elasticsearch7.NewClient(elasticCfg)
}

func DumpElasticIndexDataToFile(cfg model.ElasticS3Config) (*string, error) {
	fileName := fmt.Sprintf("%s/%s", cfg.Output.TmpFolder, "dump.jsonl")

	if !cfg.Tasks.Dump {
		return nil, nil
	}
	elasticClient, err := newElasticClient(cfg.ElasticSearch)
	if err != nil {
		log.Fatal("elastic client error", err)
	}

	dumpFile, err := os.Create(fileName)
	if err != nil {
		log.Println("could not create file for writing", err)
		return nil, err
	}
	defer dumpFile.Close()

	start := 0
	size := 200
	total := 0
	for {
		searchRequest := esapi.SearchRequest{
			Index: []string{cfg.ElasticSearch.Index},
			Body: bytes.NewBufferString(`{
			
		}`),
			From: &start,
			Size: &size,
		}
		res, err := searchRequest.Do(context.TODO(), elasticClient)
		if err != nil {
			log.Println("error searching client")
			return nil, err
		} else {
			if res.StatusCode == 200 {
				_, fErr := os.Stat(cfg.Output.TmpFolder)
				if fErr != nil {
					err = os.MkdirAll(cfg.Output.TmpFolder, os.ModePerm)
				}
				responseData := model.ElasticsearchResponse{}
				json.NewDecoder(res.Body).Decode(&responseData)
				if total == 0 {
					total = responseData.Hits.Total.Value
				}
				for _, hit := range responseData.Hits.Hits {
					data := hit.Source
					outputData := make(map[string]interface{}, 0)
					for _, transformField := range cfg.Upload.Transform.Fields {
						outputData[transformField.Name] = data[transformField.Name]
					}
					buff := bytes.Buffer{}
					json.NewEncoder(&buff).Encode(outputData)
					dumpFile.WriteString(buff.String())
				}
				docCount := len(responseData.Hits.Hits)
				start = start + docCount
				if docCount < size {
					break
				}
			} else {
				return nil, errors.New("search result has invalid response code")
			}
		}
	}
	return &fileName, nil
}

func IndexDocument(ctx context.Context, payload IndexPayload, elasticClient *elasticsearch7.Client) bool {
	indexRequest := esapi.IndexRequest{
		Index:      payload.Index,
		Body:       bytes.NewBufferString(payload.Data),
		DocumentID: payload.Key,
	}
	c, cfn := context.WithTimeout(ctx, time.Duration(5*time.Second))
	defer cfn()
	r, err := indexRequest.Do(c, elasticClient)
	if err != nil {
		fmt.Println("error indexing", payload.Data, err)
	}
	result := false
	if r != nil {
		result = r.StatusCode == 201
	}
	return result
}
func AliasExists(ctx context.Context, aliasName string, elasticClient *elasticsearch7.Client) bool {
	c, cfn := context.WithTimeout(ctx, time.Duration(5*time.Second))
	defer cfn()
	aliasCheckRequest := esapi.IndicesExistsAliasRequest{
		Name: []string{aliasName},
	}
	res, err := aliasCheckRequest.Do(c, elasticClient)
	if err != nil {
		log.Fatal("error checking alias", err)
	}
	return res.StatusCode <= 300
}

func GetIndexForAlias(ctx context.Context, aliasName string, elasticClient *elasticsearch7.Client) []string {
	c, cfn := context.WithTimeout(ctx, time.Duration(5*time.Second))
	defer cfn()
	aliasGetRequest := esapi.IndicesGetAliasRequest{
		Name: []string{aliasName},
	}
	res, err := aliasGetRequest.Do(c, elasticClient)
	if err != nil {
		log.Fatal("error getting alias", err)
	}
	var indices = make([]string, 0)
	if res.StatusCode <= 300 {
		aliasResult := make(map[string]interface{}, 0)
		json.NewDecoder(res.Body).Decode(&aliasResult)
		for k, _ := range aliasResult {
			indices = append(indices, k)
		}
	}
	return indices
}

func AliasUpdate(ctx context.Context,
	indexName string,
	aliasName string,
	elasticClient *elasticsearch7.Client) bool {
	updateResult := false
	if AliasExists(ctx, aliasName, elasticClient) {
		c, cfn := context.WithTimeout(ctx, time.Duration(5*time.Second))
		defer cfn()
		indices := GetIndexForAlias(context.TODO(), aliasName, elasticClient)
		instructions := make([]string, 0)
		for _, existingIndices := range indices {
			instruction := fmt.Sprintf(`{
  "remove": {
    "index": "%s", "alias": "%s"
  } 
}`, existingIndices, aliasName)
			instructions = append(instructions, instruction)
		}
		addInstruction := fmt.Sprintf(`{
  "add": {
    "index": "%[1]s", "alias": "%[2]s"
  } 
}`, indexName, aliasName)
		instructions = append(instructions, addInstruction)
		body := strings.Join(instructions, ",\n")
		updateBody := bytes.NewBufferString(fmt.Sprintf(`{
			"actions": [
				%s		
			]
		}`, body))
		aliasUpdateRequest := esapi.IndicesUpdateAliasesRequest{
			Body: updateBody,
		}
		res, err := aliasUpdateRequest.Do(c, elasticClient)
		if err != nil {
			log.Fatal("error updating alias", err)
		}
		updateResult = res.StatusCode <= 300

	} else {
		c, cfn := context.WithTimeout(ctx, time.Duration(5*time.Second))
		defer cfn()
		aliasPutRequest := esapi.IndicesPutAliasRequest{
			Index: []string{indexName},
			Name:  aliasName,
		}
		res, err := aliasPutRequest.Do(c, elasticClient)
		if err != nil {
			log.Fatal("error adding alias", err)
		}
		updateResult = res.StatusCode <= 300
	}
	return updateResult
}

func DeleteIndex(ctx context.Context, indexName string, elasticClient *elasticsearch7.Client) bool {
	delRequest := esapi.IndicesDeleteRequest{
		Index: []string{indexName},
	}
	res, err := delRequest.Do(ctx, elasticClient)
	if err != nil {
		log.Fatal("error deleting index", err)
	}
	return res.StatusCode <= 300
}

func EnsureIndexExists(cfg model.ElasticS3Config, elasticClient *elasticsearch7.Client) {
	indexName := cfg.ElasticSearch.Index
	ctx := context.TODO()
	if cfg.Tasks.Recreate {

		indexDeleteRequest := esapi.IndicesDeleteRequest{
			Index: []string{indexName},
		}
		indexDeleteRequest.Do(ctx, elasticClient)
	}

	indexCheckRequest := esapi.IndicesExistsRequest{
		Index: []string{indexName},
	}

	res, err := indexCheckRequest.Do(ctx, elasticClient)

	if err != nil {
		fmt.Println("error in index check", err)
	} else {
		if res.StatusCode == 404 {
			fmt.Println(fmt.Sprintf("create index %s", indexName))
			indexBody := `{
				"settings": {
				  "index": {
					"number_of_shards": 1,
					"analysis": {
					  "normalizer": {
						"lowercase_normalizer": {
						  "filter": [
							"lowercase"
						  ],
						  "type": "custom"
						}
					  }
					},
					"number_of_replicas": 0
				  }
				}
			}`
			indexCreateRequest := esapi.IndicesCreateRequest{
				Index: indexName,
				Body:  bytes.NewBufferString(indexBody),
			}
			indexCreateRes, indexCreateErr := indexCreateRequest.Do(ctx, elasticClient)
			if indexCreateErr != nil {
				log.Fatal("index could not be created", indexCreateErr)
			}
			fmt.Println(indexCreateRes.String())
			mappingText := bytes.NewBufferString(schema.MappingText)
			indexMappingRequest := esapi.IndicesPutMappingRequest{
				Body:  mappingText,
				Index: []string{indexName},
			}
			res, err = indexMappingRequest.Do(ctx, elasticClient)
			fmt.Println("mapping", res.StatusCode)
			fmt.Println(res.String())
		} else {
			fmt.Println(fmt.Sprintf("index %s exists", indexName))
		}
	}

}
