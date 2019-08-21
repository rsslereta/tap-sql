package tapsql

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

var (
	maxIdleConns       = 10
	maxConnMaxLifetime = 10 * time.Minute
)

// =========================================================

// MappedSQLRow is a row keyed by column name
// IMPORTANT: Columns names MUST be unique
type MappedSQLRow map[string]interface{}

// SQLRow collects sqlx.SQLRow and columns
type SQLRow struct {
	rows *sqlx.Rows
}

// Next returns true if another row is available.
func (rc *SQLRow) Next() bool {
	return rc.rows.Next()
}

// Scan takes a record and scans the values of a row into the record.
func (rc *SQLRow) Scan(r MappedSQLRow) error {
	if err := rc.rows.MapScan(r); err != nil {
		return err
	}
	convertBytesToString(r)
	return nil
}

// Close closes the iterator.
func (rc *SQLRow) Close() {
	rc.rows.Close()
}

// =========================================================

// Connect connects to a database given a driver name and set of connection parameters.
func Connect(driver string, params map[string]interface{}) (*sqlx.DB, error) {
	// Select the driver.
	driver, ok := ValidateDriver[driver]
	if !ok {
		return nil, errors.New("Unknown driver")
	}
	// Connect to the database.
	conConv := sqlDrivers[driver]
	params = cleanParams(params)
	return sqlx.Connect(driver, conConv(params))
}

// =========================================================

// ExecuteQuery takes a database instance, SQL statement, and parameters and executes the query
// returning the resulting rows.
func ExecuteQuery(db *sqlx.DB, tblName, syncCol string, params map[string]interface{}) (*SQLRow, error) {
	var (
		rows     *sqlx.Rows
		err      error
		sqlQuery string
	)
	switch {
	case getMinMaxRange(params):
		sqlQuery = fmt.Sprintf(`
			SELECT * FROM "%s" WHERE "%s" >= :min AND "%s" < :max ORDER BY "%s" ASC`,
			tblName, syncCol, syncCol, syncCol,
		)
		rows, err = db.NamedQuery(sqlQuery, params)
	case getOffset(params):
		sqlQuery = fmt.Sprintf(`
			SELECT * FROM "%s" WHERE "%s" > :offset ORDER BY "%s" ASC`,
			tblName, syncCol, syncCol,
		)
		rows, err = db.NamedQuery(sqlQuery, params)
	case getTimestamp(params):
		sqlQuery = fmt.Sprintf(`
			SELECT * FROM "%s" WHERE "%s" = :timestamp ORDER BY "%s" ASC`,
			tblName, syncCol, syncCol,
		)
		rows, err = db.NamedQuery(sqlQuery, params)
	default:
		fmt.Println("default")
		sqlQuery = fmt.Sprintf(`
			SELECT * FROM "%s" ORDER BY "%s" ASC`,
			tblName, syncCol,
		)
		rows, err = db.Queryx(sqlQuery)
	}
	if err != nil {
		return nil, err
	}
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if hasDuplicateHeaders(cols) {
		return nil, errors.New("Has duplicate headers")
	}
	return &SQLRow{
		rows: rows,
	}, nil
}

func hasDuplicateHeaders(col []string) bool {
	headers := make(map[string]struct{})
	for _, r := range col {
		if _, ok := headers[r]; ok {
			return true
		}
		headers[r] = struct{}{}
	}
	return false
}

func getMinMaxRange(params map[string]interface{}) bool {
	if params == nil {
		return false
	}
	if len(params) == 0 {
		return false
	}
	// check if there is a max parameter
	_, ok := params["max"]
	if !ok {
		return false
	}
	// check if there is a min parameter
	_, ok = params["min"]
	if !ok {
		return false
	}
	return true
}

func getOffset(params map[string]interface{}) bool {
	if params == nil {
		return false
	}
	if len(params) == 0 {
		return false
	}
	// check if there is a offset parameter
	_, ok := params["offset"]
	if !ok {
		return false
	}
	return true
}

func getTimestamp(params map[string]interface{}) bool {
	if params == nil {
		return false
	}
	if len(params) == 0 {
		return false
	}
	// check if there is a timestamp parameter
	_, ok := params["timestamp"]
	if !ok {
		return false
	}
	return true
}

// =========================================================

var (
	connMap      = make(map[string]*sqlx.DB)
	connMapMutex = &sync.Mutex{}
)

// ConnectionPool generates a collection of connections
// 	maximum Idle Connections    = 10
// 	maximum Connection Lifespan = 10 * time.Minute
func ConnectionPool(driver string, conParams map[string]interface{}) (*sqlx.DB, error) {
	var (
		db  *sqlx.DB
		ok  bool
		err error
	)
	connKey, err := json.Marshal(conParams)
	if err != nil {
		return nil, err
	}
	// generate unique composit key
	key := driver + string(connKey)

	connMapMutex.Lock()
	defer connMapMutex.Unlock()

	// Create connection, check if there is an existing connection
	if db, ok = connMap[key]; !ok {
		db, err = Connect(driver, conParams)
		if err != nil {
			return nil, err
		}
		db.SetMaxIdleConns(maxIdleConns)
		db.SetConnMaxLifetime(maxConnMaxLifetime)
		connMap[key] = db
	}
	return db, nil
}

// =========================================================

// Shutdown closes database connections.
func Shutdown() {
	connMapMutex.Lock()
	for _, db := range connMap {
		db.Close()
	}
	connMapMutex.Unlock()
}
