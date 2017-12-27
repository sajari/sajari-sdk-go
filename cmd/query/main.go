package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/context"

	"google.golang.org/grpc"

	"code.sajari.com/sajari-sdk-go"
)

var (
	endpoint   = flag.String("endpoint", "", "endpoint `address`, uses default if not set")
	project    = flag.String("project", "", "project `name` to query")
	collection = flag.String("collection", "", "collection `name` to query")
	creds      = flag.String("creds", "", "calling credentials `key-id,key-secret`")

	text          = flag.String("text", "", "body `text` to search for")
	limit         = flag.Int("limit", 10, "fetch `N` results")
	offset        = flag.Int("offset", 0, "fetch results starting with the `N`th")
	fields        = flag.String("fields", "", "comma separated list of `field names`")
	sort          = flag.String("sort", "", "comma seperated `list` of [-]field")
	filter        = flag.String("filter", "", "comma seperated `list` of field[ ]op:value")
	indexBoost    = flag.String("indexboost", "", "comma seperated `list` of field:value")
	count         = flag.Int("count", 1, "run the query `N` times and record stats")
	tracking      = flag.String("tracking", "", "tokens to create for each result, either `CLICK or POS_NEG`")
	trackingField = flag.String("tracking-field", "", "unique field to use in tracking (must be returned in result set)")
	trackingData  = flag.String("tracking-data", "", "`key:value` pairs, comma-seperated")
	transforms    = flag.String("transforms", "", "comma seperated `list` of transform identifiers")
	aggregates    = flag.String("aggregates", "", "comma seperated `list` of `aggregate-type:field")
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

	r := &sajari.Request{
		Limit:  *limit,
		Offset: *offset,
	}

	if *fields != "" {
		fieldList := strings.Split(*fields, ",")
		if len(fieldList) > 0 {
			r.Fields = fieldList
		}
	}

	iq := sajari.IndexQuery{
		Text: *text,
	}
	if *indexBoost != "" {
		boosts := strings.Split(*indexBoost, ",")
		for _, boost := range boosts {
			boostSplit := strings.Split(boost, ":")
			if len(boostSplit) != 2 {
				log.Printf("index boost: expected two items field:value, got: %v", boost)
				return
			}
			value, err := strconv.ParseFloat(boostSplit[1], 64)
			if err != nil {
				log.Printf("index boost: error parsing boost value %q: %v", boostSplit[1], err)
				return
			}

			iq.InstanceBoosts = append(iq.InstanceBoosts, sajari.FieldInstanceBoost(boostSplit[0], value))
		}
	}

	if *sort != "" {
		sortList := strings.Split(*sort, ",")
		sorts := make([]sajari.Sort, 0, len(sortList))
		for _, sortItem := range sortList {
			sorts = append(sorts, sajari.SortByField(sortItem))
		}
		if len(sorts) > 0 {
			r.Sort = sorts
		}
	}

	if *filter != "" {
		filterList := strings.Split(*filter, ",")
		fs := make([]sajari.Filter, 0, len(filterList))
		for _, filterItem := range filterList {
			items := strings.SplitN(filterItem, ":", 2)
			if len(items) != 2 {
				log.Printf("filter: expected two items field[ ]op:value, got: %q", filterItem)
				return
			}
			fs = append(fs, sajari.FieldFilter(items[0], items[1]))
		}
		r.Filter = sajari.AllFilters(fs...)
	}

	if *transforms != "" {
		transformList := strings.Split(*transforms, ",")
		for _, transform := range transformList {
			r.Transforms = append(r.Transforms, sajari.Transform(transform))
		}
	}

	if *aggregates != "" {
		aggregateList := strings.Split(*aggregates, ",")
		for _, aggregate := range aggregateList {
			items := strings.SplitN(aggregate, ":", 3)
			if len(items) != 3 {
				log.Printf("aggregates: invalid aggregate %q (should be of the form type:field:name)", aggregate)
				return
			}
			var a sajari.Aggregate
			switch items[0] {
			case "count":
				a = sajari.CountAggregate(items[1])

			default:
				log.Printf("aggregates: invalid aggregate type %q", items[0])
				return
			}

			if r.Aggregates == nil {
				r.Aggregates = make(map[string]sajari.Aggregate)
			}
			r.Aggregates[items[2]] = a
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
				log.Printf("expected 'key:value': got %q", kv)
				return
			}
			m[kv[0]] = kv[1]
		}
		tr.Data = m
	}

	r.Tracking = tr
	r.IndexQuery = iq

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

	totalResults := 0
	totalTime := time.Duration(0)
	totalReads := 0

	for i := 0; i < *count; i++ {
		ctx := context.Background()
		resp, err := client.Query().Search(ctx, r)
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

		if len(resp.Aggregates) > 0 {
			b, err := json.MarshalIndent(resp.Aggregates, "", "  ")
			if err != nil {
				log.Printf("could not write out aggregates (%v): %v", resp.Aggregates, err)
			}
			fmt.Println()
			fmt.Println("Aggregates:")
			fmt.Println(string(b))
		}

		totalResults = resp.TotalResults
		totalTime += resp.Time
		totalReads += resp.Reads
	}

	fmt.Println("Total Results", totalResults)
	fmt.Println("Reads", totalReads)
	fmt.Println("Time", totalTime)

	if totalReads > 0 {
		fmt.Println("Time per Read:", time.Duration(int64(totalTime)/int64(totalReads)))
	}
}
