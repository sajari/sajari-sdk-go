// Package bayes provides methods for creating, training and querying bayes models.
package bayes

import (
	"errors"

	"golang.org/x/net/context"

	"github.com/sajari/sajari-sdk-go"
	"github.com/sajari/sajari-sdk-go/internal"

	qpb "github.com/sajari/protogen-go/sajari/bayes/query"
	tpb "github.com/sajari/protogen-go/sajari/bayes/train"
	tspb "github.com/sajari/protogen-go/sajari/bayes/trainingset"
)

// New returns a handler which provides access to functionality for
// building and manipulating bayes models.
func New(client *sajari.Client) *Client {
	return &Client{
		c: client,
	}
}

// Client provides methods for interacting with bayes models.
type Client struct {
	c *sajari.Client
}

// Model returns a handle for using a bayes model.
func (c *Client) Model(name string) *Model {
	return &Model{
		c:    c.c,
		name: name,
	}
}

// TrainingSet returns a handle for using bayes training sets.
func (c *Client) TrainingSet(name string) *TrainingSet {
	return &TrainingSet{
		c:    c.c,
		name: name,
	}
}

type TrainingSet struct {
	c *sajari.Client

	name string
}

func (t *TrainingSet) newContext(ctx context.Context) context.Context {
	return internal.NewContext(ctx, t.c.Project, t.c.Collection)
}

// Create creates a new training set.
func (t *TrainingSet) Create(ctx context.Context) error {
	_, err := tspb.NewTrainingSetClient(t.c.ClientConn).Create(t.newContext(ctx), &tspb.CreateRequest{
		Name: t.name,
	})
	return err
}

// AddClass adds a class to a training set and returns a reference to it
func (t *TrainingSet) AddClass(ctx context.Context, class string) (Class, error) {
	_, err := tspb.NewTrainingSetClient(t.c.ClientConn).AddClass(t.newContext(ctx), &tspb.AddClassRequest{
		Name:  t.name,
		Class: class,
	})
	if err != nil {
		return Class{}, err
	}
	return Class{name: class}, nil
}

// AddRecord adds a record to a class and gives you the SHA1 of the data back
func (t *TrainingSet) AddRecord(ctx context.Context, class Class, data []string) (string, error) {
	res, err := tspb.NewTrainingSetClient(t.c.ClientConn).Upload(t.newContext(ctx), &tspb.UploadRequest{
		Name:  t.name,
		Class: class.Name(),
		Data:  data,
	})
	if err != nil {
		return "", err
	}
	return res.Hash, nil
}

// Info returns info about a training set such as class names
func (t *TrainingSet) Classes(ctx context.Context) ([]Class, error) {
	cls, err := tspb.NewTrainingSetClient(t.c.ClientConn).Info(ctx, &tspb.InfoRequest{
		Name: t.name,
	})
	if err != nil {
		return nil, err
	}

	out := make([]Class, 0, len(cls.Classes))
	for _, v := range cls.Classes {
		out = append(out, Class{name: v})
	}

	return out, nil
}

// Train trains a the training set, creating a model which can be loaded
// as well as returning the result data from the training.
func (t *TrainingSet) Train(ctx context.Context, name string) (*TrainResults, error) {
	tr, err := tpb.NewTrainClient(t.c.ClientConn).Train(t.newContext(ctx), &tpb.Request{
		Name:  t.name,
		Model: name,
	})
	if err != nil {
		return nil, err
	}

	res := &TrainResults{
		Errors:    map[Class][]ClassErrorCount{},
		Correct:   tr.Correct,
		Incorrect: tr.Incorrect,
	}

	for _, i := range tr.Errors {
		c := Class{
			name: i.Got,
		}
		res.Errors[c] = append(res.Errors[c], ClassErrorCount{
			Class: c,
			Count: i.Count,
		})
	}
	return res, nil
}

// Model provides methods for interacting with bayes models.
type Model struct {
	c *sajari.Client

	name string
}

func (m *Model) newContext(ctx context.Context) context.Context {
	return internal.NewContext(ctx, m.c.Project, m.c.Collection)
}

// Classes returns the list of classes in the bayes model.
func (m *Model) Classes() ([]string, error) {
	panic("Not implemented")
}

// Classify classifies the data into a model class.
func (m *Model) Classify(ctx context.Context, data []string) (*Class, error) {
	qr, err := qpb.NewQueryClient(m.c.ClientConn).Query(m.newContext(ctx), &qpb.Request{
		Model: m.name,
		Data:  data,
	})
	if err != nil {
		return nil, err
	}

	return &Class{
		name: qr.Best,
	}, nil
}

// Class is a bayes class.
type Class struct {
	name string
}

// Name is the name of the class.
func (c *Class) Name() string {
	return c.name
}

// ClassErrorCount is a measure of how many records were incorrectly classified
// into a particular Class.
type ClassErrorCount struct {
	Class Class

	// Number of records incorrectly classified into this class.
	Count uint32
}

// TrainResults is a collection of information
type TrainResults struct {
	// Errors is mapping of records that were incorrectly
	// classified.
	Errors map[Class][]ClassErrorCount

	Correct, Incorrect uint32
}

// Accuracy returns the total accuracy percentage of the results
func (r TrainResults) Accuracy() float64 {
	return (float64(r.Correct) / float64(r.Correct+r.Incorrect))
}

// ErrNotEnoughData is returned from train if there isn't enough training
// data.
var ErrNotEnoughData = errors.New("not enough data")

// ErrNotTrained is returned from Query if the model has not been trained.
var ErrNotTrained = errors.New("model not trained")
