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
	endpoint   = flag.String("endpoint", "", "engine endpoint `address`, uses default if not set")
	project    = flag.String("project", "", "project `name` to query")
	collection = flag.String("collection", "", "collection `name` to query")
	creds      = flag.String("creds", "", "calling credentials in the form `key-id,key-secret`")

	add       = flag.Bool("add", false, "add a record")
	mutate    = flag.String("mutate", "", "`field:value` pair to identify a record")
	get       = flag.String("get", "", "`field:value` pair to identify a record")
	deleteKey = flag.String("delete", "", "`field:value` pair which identifies the record to delete")

	data = flag.String("data", "", "`json` map of keys to values")
)

func newClient() *sajari.Client {
	var opts []sajari.Opt
	if *endpoint != "" {
		opts = append(opts, sajari.WithEndpoint(*endpoint))
	}

	if *creds != "" {
		credsSplit := strings.Split(*creds, ",")
		if len(credsSplit) != 2 {
			log.Printf("creds: expected 'id,secret', got '%v'", *creds)
			return nil
		}
		kc := sajari.KeyCredentials(credsSplit[0], credsSplit[1])
		opts = append(opts, sajari.WithCredentials(kc))
	}

	client, err := sajari.New(*project, *collection, opts...)
	if err != nil {
		log.Fatal("error from sajari.New():", err)
	}
	return client
}

func errMsg(err error) string {
	return fmt.Sprintf("Code: %v Error: %v", grpc.Code(err), grpc.ErrorDesc(err))
}

func main() {
	flag.Parse()

	if *get != "" {
		fieldValue := strings.SplitN(*get, ":", 2)
		if len(fieldValue) != 2 {
			log.Fatalln("-get value must be of the form field:value")
		}

		k := sajari.NewKey(fieldValue[0], fieldValue[1])
		d, err := newClient().Get(context.Background(), k)
		if err != nil {
			log.Fatalf("error from Get(%v): %v\n", k, err)
		}

		b, err := json.MarshalIndent(d, "", "  ")
		if err != nil {
			log.Fatalf("error marshaling JSON output: %v\n", err)
		}

		fmt.Println(string(b))
		return
	}

	if *add {
		if *data == "" {
			log.Fatalln("no data found, supply json string with -data")
		}
		d := map[string]interface{}{}
		if err := json.Unmarshal([]byte(*data), &d); err != nil {
			log.Fatalf("got error unmarshalling json from -data: %v\n", err)
		}

		for k, v := range d {
			if vv, ok := v.([]interface{}); ok {
				x := make([]string, 0, len(vv))
				for _, vvv := range vv {
					x = append(x, fmt.Sprintf("%v", vvv))
				}
				d[k] = x
			}
		}

		k, err := newClient().Add(context.Background(), d)
		if err != nil {
			log.Fatalf("got error adding record: %v\n", errMsg(err))
		}

		fmt.Println(k)
		return
	}

	if *mutate != "" {
		if *data == "" {
			log.Fatalln("no data found, supply json string with -data")
		}
		d := map[string]interface{}{}
		if err := json.Unmarshal([]byte(*data), &d); err != nil {
			log.Fatalf("got error unmarshalling json from -data: %v\n", err)
		}

		ids := strings.Split(*mutate, ":")
		if len(ids) != 2 {
			log.Fatalln("mutate value should be formatted \"key:value\"")
		}
		ctx := context.Background()
		k := sajari.NewKey(ids[0], ids[1])
		if err := newClient().Mutate(ctx, k, sajari.SetFields(d)...); err != nil {
			log.Fatalf("error mutating record: %v\n", errMsg(err))
		}
		return
	}

	if *deleteKey != "" {
		fieldValue := strings.SplitN(*deleteKey, ":", 2)
		if len(fieldValue) != 2 {
			log.Fatalf("-delete value must be of the form field:value")
		}

		k := sajari.NewKey(fieldValue[0], fieldValue[1])
		if err := newClient().Delete(context.Background(), k); err != nil {
			log.Fatalf("error from Delete(%v): %v\n", k, errMsg(err))
		}
		return
	}
	log.Fatalln("command not found, please use -add, -mutate, or -get")
}
