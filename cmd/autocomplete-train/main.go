package main

import (
	"flag"
	"log"
	"strings"

	"golang.org/x/net/context"

	"github.com/sajari/sajari-sdk-go"
	"github.com/sajari/sajari-sdk-go/autocomplete"
)

var (
	endpoint   = flag.String("endpoint", "", "endpoint `address`, uses default if not set")
	project    = flag.String("project", "", "project `name` to query")
	collection = flag.String("collection", "", "collection `name` to query")
	creds      = flag.String("creds", "", "calling credentials `key-id,key-secret`")

	name  = flag.String("name", "en.dict", "`name` of autocomplete model to train")
	terms = flag.String("terms", "", "comma-seperated list of correctly spelt words to add to autocomplete dictionary")
)

func main() {
	flag.Parse()

	var opts []sajari.Opt
	if *endpoint != "" {
		opts = append(opts, sajari.WithEndpoint(*endpoint))
	}

	if *project == "" {
		log.Println("project: cannot be empty")
		return
	}

	if *collection == "" {
		log.Println("project: cannot be empty")
		return
	}

	if *creds == "" {
		log.Println("creds: cannot be empty")
		return
	}

	credsSplit := strings.Split(*creds, ",")
	if len(credsSplit) != 2 {
		log.Printf("creds: expected 'id,secret', got '%v'", *creds)
		return
	}
	kc := sajari.KeyCredentials(credsSplit[0], credsSplit[1])
	opts = append(opts, sajari.WithCredentials(kc))

	client, err := sajari.New(*project, *collection, opts...)
	if err != nil {
		log.Printf("error from sajari.New(): %v", err)
		return
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("error closing Client: %v", err)
		}
	}()

	var termList []string
	if *terms != "" {
		termList = strings.Split(*terms, ",")
	}

	if len(termList) == 0 {
		log.Printf("no terms specified")
		return
	}

	if err := autocomplete.New(client, *name).TrainCorpus(context.Background(), termList); err != nil {
		log.Println(err)
	}
}
