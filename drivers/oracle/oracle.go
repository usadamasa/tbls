package oracle

import "database/sql"

import (
	_ "github.com/godror/godror"
	"github.com/k1LoW/tbls/schema"
	"github.com/pkg/errors"
	"regexp"
)

var reFK = regexp.MustCompile(`FOREIGN KEY \((.+)\) REFERENCES ([^\s]+)\s?\((.+)\)`)

// oracle struct
type Oracle struct {
	db *sql.DB
}

// New return new Oracle
func New(db *sql.DB) *Oracle {
	return &Oracle{
		db: db,
	}
}

// Analyze MySQL database schema
func (o *Oracle) Analyze(s *schema.Schema) error {
	d, err := o.Info()
	if err != nil {
		return errors.WithStack(err)
	}
	s.Driver = d
	return nil
}

// Info return schema.Driver
func (o *Oracle) Info() (*schema.Driver, error) {
	var v string
	row := o.db.QueryRow(`SELECT * FROM v$version`)
	err := row.Scan(&v)
	if err != nil {
		return nil, err
	}

	d := &schema.Driver{
		Name:            "oracle",
		DatabaseVersion: v,
	}
	return d, nil
}
