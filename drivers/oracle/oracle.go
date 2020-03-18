package oracle

import (
	"database/sql"
	"regexp"

	"github.com/k1LoW/tbls/schema"
	"github.com/pkg/errors"
)

var reFK = regexp.MustCompile(`FOREIGN KEY \((.+)\) REFERENCES ([^\s]+)\s?\((.+)\)`)

// Oracle struct
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

	// Get schemas
	tableRows, err := o.db.Query(`
		SELECT
			TABLE_NAME,
			TABLE_TYPE,
			COMMENTS
		FROM DBA_TAB_COMMENTS
		WHERE owner = :owner_name
		ORDER BY TABLE_NAME
		`, sql.Named("owner_name", s.Name))
	if err != nil {
		return errors.WithStack(err)
	}
	defer tableRows.Close()

	tables := []*schema.Table{}

	for tableRows.Next() {
		var (
			tableName    string
			tableType    string
			tableComment string
		)
		err := tableRows.Scan(&tableName, &tableType, &tableComment)
		if err != nil {
			return errors.WithStack(err)
		}
		table := &schema.Table{
			Name:    tableName,
			Type:    tableType,
			Comment: tableComment,
		}
		tables = append(tables, table)
	}
	s.Tables = tables

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
