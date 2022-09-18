package client

import (
	"context"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/jackc/pgx/v4/pgxpool"
)

type Client struct {
	conn *pgxpool.Pool
}

func New(config Config) (*Client, error) {
	conn, err := pgxpool.Connect(context.Background(), config.DatabaseURL)
	if err != nil {
		return nil, err
	}

	return &Client{conn: conn}, nil
}

func (c *Client) GetDatabases(ctx context.Context) *arrow.Schema {
	var datnames []string
	pgxscan.Select(ctx, c.conn, &datnames, "SELECT datname FROM pg_database;")
	var fields []arrow.Field
	var metadata *arrow.Metadata

	for _, datname := range datnames {
		fields = append(fields, arrow.Field{
			Name: datname,
			Type: &arrow.BinaryType{},
		})
	}
	schema := arrow.NewSchema(fields, metadata)
	return schema
}
