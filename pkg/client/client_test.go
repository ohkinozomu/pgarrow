package client

import (
	"context"
	"strconv"
	"testing"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/google/go-cmp/cmp"
	"github.com/orlangure/gnomock"
	"github.com/orlangure/gnomock/preset/postgres"
)

func TestDB(t *testing.T) {
	p := postgres.Preset(
		postgres.WithUser("gnomock", "gnomick"),
		postgres.WithDatabase("mydb"),
		postgres.WithQueriesFile("testdata/schema.sql"),
	)
	container, err := gnomock.Start(p)
	if err != nil {
		panic(err)
	}
	t.Cleanup(func() { _ = gnomock.Stop(container) })

	config := Config{
		DatabaseURL: "postgres://gnomock:gnomick@127.0.0.1:" + strconv.Itoa(container.DefaultPort()) + "/mydb",
	}
	c, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	databases := c.GetDatabases(context.Background())

	fields := []arrow.Field{
		{
			Name: "postgres",
			Type: &arrow.BinaryType{},
		},
		{
			Name: "mydb",
			Type: &arrow.BinaryType{},
		},
		{
			Name: "template1",
			Type: &arrow.BinaryType{},
		},
		{
			Name: "template0",
			Type: &arrow.BinaryType{},
		},
	}
	var metadata *arrow.Metadata
	expect := arrow.NewSchema(fields, metadata)
	if diff := cmp.Diff(expect, databases); diff != "" {
		t.Errorf(diff)
	}
}
