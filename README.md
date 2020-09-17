### JSONL to Elastic and then back to S3
A recipe to index random dataset into elasticsearch which can then be archived to s3 all using json config


1. Uses jsonl file to index against elastic
1. Can download data from elastic
1. Can upload data to s3


#### Using it

##### Local Environment Setup
``` 
docker-compose up -d
```

##### Code Config
Check provided s3.json to configure this for your dataset. 

##### Code Generate
```
go generate
```

##### Build

``` 
CGO_ENABLED=0 go build
```

##### Run 

``` 
./elastics3
```

##### Quickstart
```
#other examples
CONFIG_FILE=s3-private.json go generate
CONFIG_FILE=s3-private.json go build -o s3-private
CONFIG_FILE=s3-private.json ./s3-private
```
