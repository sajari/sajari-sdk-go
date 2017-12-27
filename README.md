# Sajari APIs Go Client &middot; [![Build Status](https://travis-ci.org/sajari/sajari-sdk-go.svg?branch=master)](https://travis-ci.org/sajari/sajari-sdk-go)

This repository provides functionality for interacting with Sajari APIs.

# Installation

If you haven't setup Go before, you need to first set a `GOPATH` (see [https://golang.org/doc/code.html#GOPATH](https://golang.org/doc/code.html#GOPATH)).

To fetch and build the code:

    $ go get code.sajari.com/sajari-sdk-go/...

This will also build the command line tools (in particular `query`, `csv-importer`, `schema` and `pipeline` which can be used to interaction with Sajari collections) into `$GOPATH/bin` (assumed to be in your `PATH` already).

# Getting Started

```go
package main

import (
	"log"

	"golang.org/x/net/context"

	"code.sajari.com/sajari-sdk-go"
)

func main() {
	creds := sajari.KeyCredentials("<key-id>", "<key-secret>")
	client, err := sajari.New("<project>", "<collection>", sajari.WithCredentials(creds))
	if err != nil {
		log.Fatalf("error creating client: %v", err)
	}
	defer client.Close()

	// Initialise the "website" pipeline. Ideal starting point for querying
	// website collections.
	pipeline := client.Pipeline("website")

	// Setup parameters for the search.
	values := map[string]string{
		"q":              "search terms",
		"resultsPerPage": "10",
	}

	// Setup tracking for the search results.
	t := sajari.Tracking{
		Type:  sajari.TrackingClick, // Enable click tracking.
		Field: "url",         // Use the url field for identifying records.
	}

	// Perform the search.
	resp, _, err := pipeline.Search(context.Background(), values, t)
	if err != nil {
		log.Fatalf("error performing search: %v", err)
	}

	for _, r := range resp.Results {
		log.Printf("Values: %v", r.Values)
		log.Println("Tokens: %v", r.Tokens)
	}
}
```
