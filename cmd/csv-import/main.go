package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"

	"golang.org/x/net/context"

	"github.com/sajari/sajari-sdk-go"
)

var (
	endpoint   = flag.String("endpoint", "", "endpoint `address`, uses default if not set")
	project    = flag.String("project", "", "project `ID` to use")
	collection = flag.String("collection", "", "collection `name` to import into (should already exist)")
	creds      = flag.String("creds", "", "calling credentials in the form `key-id,key-secret`")

	workers   = flag.Int("workers", 8, "use `N` workers to process data, queue and send")
	batchSize = flag.Int("batch-size", 100, "submit records in groups of at most `N`")
	debug     = flag.Bool("debug", false, "only print imported record, don't submit")
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %v [flags] file\n", os.Args[0])
	flag.PrintDefaults()
}

var client *sajari.Client

func main() {
	flag.Parse()

	file := flag.Arg(0)
	if file == "" {
		usage()
		return
	}

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

	var err error
	client, err = sajari.New(*project, *collection, opts...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error dialing endpoint: %v\n", err)
	}

	if err := importCSV(file); err != nil {
		fmt.Fprintf(os.Stderr, "Error importing data: %v\n", err)
		return
	}
}

func sendList(list []sajari.Record) {
	if !*debug {
		_, err := client.AddMulti(context.Background(), list)
		if err != nil {
			log.Printf("error adding records: %v", err)
			return
		}
	}

	for _, d := range list {
		b, err := json.MarshalIndent(d, "", "  ")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(b))
	}
}

func importCSV(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	cr := csv.NewReader(f)
	row, err := cr.Read()
	if err != nil {
		return fmt.Errorf("error reading header row: %v", err)
	}

	titles := make([]string, len(row))
	for i, r := range row {
		titles[i] = strings.Replace(strings.ToLower(r), " ", "_", -1)
	}

	ch := make(chan []string, 10)
	wg := sync.WaitGroup{}
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			batch := make([]sajari.Record, 0, *batchSize)
			for fields := range ch {
				m := make(map[string]interface{}, len(titles))
				for i := range titles {
					m[titles[i]] = fields[i]
				}

				batch = append(batch, sajari.Record(m))
				if len(batch) == *batchSize {
					sendList(batch)
					batch = batch[:0]
				}
			}

			if len(batch) > 0 {
				sendList(batch)
			}
			wg.Done()
		}()
	}
	defer wg.Wait()

	count := 0
	for {
		fields, err := cr.Read()
		if err != nil {
			close(ch)
			if err == io.EOF {
				log.Printf("Loaded %d records from csv", count)
				return nil
			}
			return fmt.Errorf("error reading row: %v", err)
		}

		ch <- fields

		count++
		if count%1000 == 0 {
			log.Println("Done", count)
		}
	}
	return nil
}
