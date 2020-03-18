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

		// columns and comments
		columnRows, err := o.db.Query(`
			select
				COLUMN_NAME,
				DATA_TYPE,
-- 				DATA_LENGTH,
-- 				DATA_PRECISION,
-- 				DATA_SCALE,
				NULLABLE,
-- 				COLUMN_ID,
				DATA_DEFAULT
-- 				CHAR_LENGTH
				
			from DBA_TAB_COLUMNS
			where
				OWNER = :owner and
				TABLE_NAME= :table_name
		`,
			sql.Named("owner", s.Name),
			sql.Named("table_name", tableName),
		)
		if err != nil {
			return errors.WithStack(err)
		}
		defer columnRows.Close()
		columns := []*schema.Column{}
		for columnRows.Next() {
			var (
				columnName    string
				columnDefault sql.NullString
				isNullable    string
				columnType    string
				columnComment sql.NullString
			)
			err = columnRows.Scan(&columnName, &columnType, &isNullable, &columnDefault)
			if err != nil {
				return errors.WithStack(err)
			}
			column := &schema.Column{
				Name:     columnName,
				Type:     columnType,
				Nullable: convertColumnNullable(isNullable),
				Default:  columnDefault,
				Comment:  columnComment.String,
			}

			columns = append(columns, column)
		}
		table.Columns = columns

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

func convertColumnNullable(str string) bool {
	if str == "N" {
		return false
	}
	return true
}
