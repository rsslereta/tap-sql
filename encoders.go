package tapsql

import (
	"encoding/json"
	"io"
	"reflect"
)

// =========================================================

// EncodeJSONLD encodes the iterator as a line delimited JSON.
// this is the default
func EncodeJSONLD(w io.Writer, rc *SQLRow, syncCol string, timestamp string) (*int, error) {
	record := make(MappedSQLRow)
	encoder := json.NewEncoder(w)
	var lastRecord int
	for rc.Next() {
		if err := rc.Scan(record); err != nil {
			return nil, err
		}
		record["Process_Date"] = timestamp
		if err := encoder.Encode(record); err != nil {
			return nil, err
		}
		lastRecord = int(reflect.ValueOf(record[syncCol]).Int())
	}
	return &lastRecord, nil
}

// =========================================================

// EncodeJSON encodes the iterator as a JSON array of records.
func EncodeJSON(w io.Writer, rc *SQLRow, syncCol string, timestamp string) (*int, error) {
	record := make(MappedSQLRow)
	var lastRecord int

	if _, err := w.Write([]byte{'['}); err != nil {
		return nil, err
	}
	var c int
	encoder := json.NewEncoder(w)
	delimiter := []byte{',', '\n'}
	for rc.Next() {
		if c > 0 {
			if _, err := w.Write(delimiter); err != nil {
				return nil, err
			}
		}
		c++
		if err := rc.Scan(record); err != nil {
			return nil, err
		}
		record["Process_Date"] = timestamp
		if err := encoder.Encode(record); err != nil {
			return nil, err
		}
		lastRecord = int(reflect.ValueOf(record[syncCol]).Int())
	}
	if _, err := w.Write([]byte{']'}); err != nil {
		return nil, err
	}
	return &lastRecord, nil
}
