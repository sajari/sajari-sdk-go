package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"golang.org/x/net/context"

	sajari "code.sajari.com/sajari-sdk-go"
)

var (
	endpoint   = flag.String("endpoint", "", "endpoint `address`, uses default if not set")
	project    = flag.String("project", "", "project `name` to query")
	collection = flag.String("collection", "", "collection `name` to query")
	creds      = flag.String("creds", "", "calling credentials `key-id,key-secret`")

	fetch        = flag.String("fetch", "-", "`path` to file to write JSON schema to, or '-' for stdout")
	add          = flag.String("add", "", "`path` to file to read JSON schema from")
	ignoreFields = flag.String("ignore-fields", "", "list of comma seperated fields `field1,field2,...` to ignore")
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

	if *project == "" {
		log.Fatal("project not set")
	}

	if *collection == "" {
		log.Fatal("collection not set")
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

	ignoreFieldsMap := map[string]bool{}
	if *ignoreFields != "" {
		for _, field := range strings.Split(*ignoreFields, ",") {
			ignoreFieldsMap[field] = true
		}
	}

	schema := client.Schema()

	if *add != "" {
		if err := schema.Add(context.Background(), getFields(*add, ignoreFieldsMap)...); err != nil {
			log.Fatalf("error adding fields: %v", err)
		}
		return
	}

	if *fetch != "" {
		fields, err := schema.Fields(context.Background())
		if err != nil {
			log.Fatalf("error fetching schema: %v", err)
		}

		flds := make([]Field, 0, len(fields))
		for _, field := range fields {
			if !ignoreFieldsMap[field.Name] {
				flds = append(flds, Field{
					Name:        field.Name,
					Description: field.Description,
					Type:        field.Type,
					Repeated:    field.Repeated,
					Required:    field.Required,
					Indexed:     field.Indexed,
					Unique:      field.Unique,
				})
			}
		}

		sch := Schema{
			Fields: flds,
		}

		b, err := json.MarshalIndent(sch, "", "  ")
		if err != nil {
			log.Fatalf("error marshalling JSON: %v", err)
		}

		var out io.Writer = os.Stdout
		if *fetch != "-" {
			f, err := os.Create(*fetch)
			if err != nil {
				log.Fatalf("error creating file for schema: %v", err)
			}
			out = f
			defer f.Close()
		}
		fmt.Fprintf(out, "%s\n", b)
		return
	}
}

func getFields(path string, ignoreFieldsMap map[string]bool) []sajari.Field {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatalf("error reading JSON schema file: %v", err)
	}

	s := Schema{}
	if err := json.Unmarshal(b, &s); err != nil {
		log.Fatalf("error unmarshalling JSON schema file: %v", err)
	}

	var fields []sajari.Field
	for _, f := range s.Fields {
		if !ignoreFieldsMap[f.Name] {
			fields = append(fields, sajari.Field{
				Name:        f.Name,
				Description: f.Description,
				Type:        f.Type,
				Repeated:    f.Repeated,
				Required:    f.Required,
				Indexed:     f.Indexed,
				Unique:      f.Unique,
			})
		}
	}
	return fields
}

type Field struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
	Type        sajari.Type `json:"type"`
	Repeated    bool        `json:"repeated"`
	Required    bool        `json:"required"`
	Indexed     bool        `json:"indexed"`
	Unique      bool        `json:"unique"`
}

type Schema struct {
	Fields []Field `json:"fields"`
}
