package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strings"

	"golang.org/x/net/context"

	"google.golang.org/grpc"

	"code.sajari.com/sajari-sdk-go"
)

var (
	endpoint   = flag.String("endpoint", "", "endpoint `address`, uses default if not set")
	project    = flag.String("project", "", "project `name` to query")
	collection = flag.String("collection", "", "collection `name` to query")
	creds      = flag.String("creds", "", "calling credentials `key-id,key-secret`")

	name   = flag.String("name", "website", "`algorithm` to run")
	values = flag.String("values", "", "`key:value` pairs, comma-seperated")

	tracking      = flag.String("tracking", "", "tokens to create for each result, either `CLICK or POS_NEG`")
	trackingField = flag.String("tracking-field", "", "unique field to use in tracking (must be returned in result set)")
	trackingData  = flag.String("tracking-data", "", "`key:value` pairs, comma-seperated")
)

func main() {
	flag.Parse()

	var opts []sajari.Opt
	if *endpoint != "" {
		opts = append(opts, sajari.WithEndpoint(*endpoint))
	}

	if *creds != "" {
		credsSplit := strings.Split(*creds, ",")
		if len(credsSplit) != 2 {
			log.Printf("creds: expected 'id,secret', got '%v'", *creds)
			return
		}
		kc := sajari.KeyCredentials(credsSplit[0], credsSplit[1])
		opts = append(opts, sajari.WithCredentials(kc))
	}

	input := make(map[string]string)
	if *values != "" {
		pairs := strings.Split(*values, ",")
		for _, pair := range pairs {
			kv := strings.Split(pair, ":")
			if len(kv) != 2 {
				log.Printf("expected 'key:value': got %q", pair)
				return
			}
			input[kv[0]] = kv[1]
		}
	}

	tr := sajari.Tracking{}
	if *tracking != "" {
		if *trackingField == "" {
			log.Printf("must specify -tracking-field with -tracking")
			return
		}

		switch *tracking {
		case "CLICK":
			tr.Type = sajari.TrackingClick

		case "POS_NEG":
			tr.Type = sajari.TrackingPosNeg

		default:
			log.Printf("unknown tracking type: %q", *tracking)
			return
		}

		tr.Field = *trackingField
	}

	if *trackingData != "" {
		m := make(map[string]string)
		pairs := strings.Split(*trackingData, ",")
		for _, pair := range pairs {
			kv := strings.Split(pair, ":")
			if len(kv) != 2 {
				log.Printf("expected 'key:value': got %q", pair)
				return
			}
			m[kv[0]] = kv[1]
		}
		tr.Data = m
	}

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

	ctx := context.Background()
	resp, _, err := client.Pipeline(*name).Search(ctx, input, tr)
	if err != nil {
		log.Printf("Code: %v Message: %v", grpc.Code(err), grpc.ErrorDesc(err))
		return
	}

	for _, result := range resp.Results {
		b, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			log.Printf("could not write out result (%v): %v", result, err)
		}
		fmt.Println(string(b))
	}

	fmt.Println("Total Results", len(resp.Results))
	fmt.Println("Reads", resp.Reads)
	fmt.Println("Time", resp.Time)
}
