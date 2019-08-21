package tapsql

import (
	"fmt"
	"strings"
)

// connConverter converts map of connection parameters to connection string.
type connConverter func(map[string]interface{}) string

// ValidateDriver common driver names to registered drivers.
var ValidateDriver = map[string]string{
	"postgresql": "postgres",
	"postgres":   "postgres",
}

// sqlDrivers defines the driver connection string
var sqlDrivers = map[string]connConverter{
	"postgres": func(params map[string]interface{}) string {
		connStr := make([]string, len(params))
		i := 0
		for k, v := range params {
			connStr[i] = fmt.Sprintf("%s=%v", k, v)
			i++
		}
		return strings.Join(connStr, " ")
	},
}

// =========================================================

// convertBytesToString ensures byte slices that were returned from the database
// are represented as strings.
func convertBytesToString(m map[string]interface{}) {
	for k, v := range m {
		if b, ok := v.([]byte); ok {
			m[k] = string(b)
		}
	}
}

// =========================================================

// cleanParams removes any key with empty string values.
func cleanParams(params map[string]interface{}) map[string]interface{} {
	cleanParams := make(map[string]interface{})
	for k, v := range params {
		switch x := v.(type) {
		case string:
			if x == "" {
				continue
			}
		}
		cleanParams[k] = v
	}
	return cleanParams
}

// =========================================================
