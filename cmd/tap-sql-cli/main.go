package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"time"

	tapsql "github.com/rsslereta/tap-sql"

	_ "github.com/lib/pq"
	yaml "gopkg.in/yaml.v2"
)

// =========================================================

// configPayload contains DB connection information
type configPayload struct {
	Driver     string                 `yaml:"driver"`
	Connection map[string]interface{} `yaml:"connection"`
	Tablename  string                 `yaml:"tablename"`
	SyncCol    string                 `yaml:"syncCol"`
	Params     map[string]interface{} `yaml:"params"`
}

// statePayload handles the state of the process
type statePayload struct {
	LastRecord int `yaml:"lastRecord"`
}

func main() {
	timeStamp := time.Now().Format(time.RFC3339)
	var (
		confPath  string
		statePath string
		encType   string
	)
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	flag.StringVar(&confPath, "confPath", "", "Config filepath")
	flag.StringVar(&statePath, "statePath", "", "State filepath")
	flag.StringVar(&encType, "encType", "", "Encoding type: CSV, JSON, JSONLD ")
	flag.Parse()
	if len(confPath) == 0 {
		log.Fatalln("configuration file required")
	}
	if len(statePath) == 0 {
		log.Fatalln("configuration file required")
	}

	// Read in configuration file -- required
	var conPay configPayload
	config, err := ioutil.ReadFile(confPath)
	if err != nil {
		log.Fatalf("Error reading config file: %s", err)
	}

	// Read in state file -- optional
	var statePay statePayload
	if len(statePath) > 0 {
		state, err := ioutil.ReadFile(statePath)
		if err != nil {
			log.Fatalf("Error reading state file: %s", err)
		}
		err = yaml.Unmarshal(state, &statePay)
		if err != nil {
			log.Fatalf("Error decoding state file: %s", err)
		}
	}

	// Unmarshal configuration file to configPayload struct
	// e.g.{postgres map[dbname: datasync host: 35.93.208.368 password: ##### port: 5432 sslmode: disable user: datasync] SELECT * FROM "XAGX" LIMIT 1000 map[]}
	err = yaml.Unmarshal(config, &conPay)
	if err != nil {
		log.Fatalf("Error decoding config file: %s", err)
	}
	// Validate database table name
	if len(conPay.Tablename) == 0 || conPay.Tablename == "" {
		log.Fatalln("Error table name required")
	}

	// Validate driver name
	// Current supported drivers: "postgresql", "postgres"
	// e.g. {"conPayload.Driver" : "postgres" }
	if _, ok := tapsql.ValidateDriver[conPay.Driver]; !ok {
		log.Fatalf("Error validating driver: %v\n", conPay.Driver)
	}
	// Establish DB connection, set idle times and expirations
	// 	maximum Idle Connections    = 10
	// 	maximum Connection Lifespan = 10 * time.Minute
	db, err := tapsql.ConnectionPool(conPay.Driver, conPay.Connection)
	if err != nil {
		log.Fatalf("Error connecting to database: %s\n", err)
	}
	// Send and Ping to validate DB connection
	if err := db.Ping(); err != nil {
		log.Fatalf("Error pinging database: %s\n", err)
	}
	// Execute Query parameter from conPayload
	// e.g. SELECT * FROM "XAGX" LIMIT 1000
	row, err := tapsql.ExecuteQuery(db, conPay.Tablename, conPay.SyncCol, conPay.Params)
	if err != nil {
		log.Fatalf("Error executing query: %s\n", err)
	}
	defer row.Close()

	// Verify specified encoding type then stream encoded output
	var lastRecord *int
	switch encType {
	case "json":
		lastRecord, err = tapsql.EncodeJSON(os.Stdout, row, conPay.SyncCol, timeStamp)
	default:
		lastRecord, err = tapsql.EncodeJSONLD(os.Stdout, row, conPay.SyncCol, timeStamp)
	}
	if err != nil {
		log.Fatalf("Error encoding output: %s\n", err)
	}
	statePay.LastRecord = *lastRecord
	file, err := os.Open(statePath)
	defer file.Close()
	if err != nil {
		log.Fatalf("Error opening state file: %s\n", err)
	}
	sp, err := json.Marshal(statePay)
	if err != nil {
		log.Fatalf("Error marshalling payload: %s\n", err)
	}
	err = ioutil.WriteFile(statePath, sp, 0644)
	if err != nil {
		log.Fatalf("Error writing state file: %s\n", err)
	}
	// shutdown all database connections
	tapsql.Shutdown()
}
