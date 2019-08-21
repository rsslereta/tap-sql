package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"mime"
	"net/http"
	tapsql "tap-sql"

	_ "github.com/lib/pq"
	yaml "gopkg.in/yaml.v2"
)

func main() {
	var (
		host string
		port int
	)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	flag.StringVar(&host, "host", "localhost", "Host of the agent.")
	flag.IntVar(&port, "port", 5000, "Port of the agent.")
	flag.Parse()

	addr := fmt.Sprintf("%s:%d", host, port)
	log.Printf("* Listening on %s...\n", addr)
	http.HandleFunc("/", requestHandler)
	err := http.ListenAndServe(addr, nil)
	tapsql.Shutdown()
	log.Fatal(err)
}

// configPayload contains connection data
type configPayload struct {
	Driver     string                 `yaml:"driver"`
	Connection map[string]interface{} `yaml:"connection"`
	SQL        string                 `yaml:"sql"`
	Params     map[string]interface{} `yaml:"params"`
}

func requestHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" || r.Method == "HEAD" {
		return
	}
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	// Check if ping param is set. This will only test the connection and does not require a query.
	_, pingOnly := r.URL.Query()["ping"]

	mimetype := r.Header.Get("Accept")
	// Validate the Accept header and parse it to ensure it is
	// supported.
	if !pingOnly {
		if mimetype = parseMimetype(mimetype); mimetype == "" {
			w.WriteHeader(http.StatusNotAcceptable)
			return
		}
	}

	var conPayload configPayload

	b, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		w.WriteHeader(http.StatusUnprocessableEntity)
		w.Write([]byte(fmt.Sprintf("Error reading body: %s", err)))
		return
	}
	err = yaml.Unmarshal(b, &conPayload)
	if err != nil {
		w.WriteHeader(http.StatusUnprocessableEntity)
		w.Write([]byte(fmt.Sprintf("Error decoding body: %s", err)))
		return
	}
	if _, ok := tapsql.ValidateDriver[conPayload.Driver]; !ok {
		w.WriteHeader(http.StatusUnprocessableEntity)
		w.Write([]byte(fmt.Sprintf("Error validating driver: %v", conPayload.Driver)))
		return
	}
	db, err := tapsql.ConnectionPool(conPayload.Driver, conPayload.Connection)
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(fmt.Sprintf("Error connecting to database: %s", err)))
		return
	}
	if pingOnly {
		if err := db.Ping(); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(fmt.Sprintf("Error pinging database: %s", err)))
		}
		return
	}
	rowCol, err := tapsql.ExecuteQuery(db, conPayload.SQL, conPayload.Params)
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(fmt.Sprintf("Error executing query: %s", err)))
		return
	}
	defer rowCol.Close()

	w.Header().Set("content-type", mimetype)
	switch mimetypeFormats[mimetype] {
	case "csv":
		err = tapsql.EncodeCSV(w, rowCol)
	case "json":
		err = tapsql.EncodeJSON(w, rowCol)
	case "jsonld":
		err = tapsql.EncodeJSONLD(w, rowCol)
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("Error encoding output: %s", err)))
		return
	}
}

var (
	defaultMimetype = "application/json"
	mimetypeFormats = map[string]string{
		"*/*":                  "json",
		"text/csv":             "csv",
		"application/json":     "json",
		"application/x-ldjson": "jsonld",
	}
)

func parseMimetype(mimetype string) string {
	mimetype, params, err := mime.ParseMediaType(mimetype)
	if err != nil {
		return ""
	}
	if mimetype == "" {
		return defaultMimetype
	}
	switch mimetype {
	case "application/json":
		if params["boundary"] == "NL" {
			return "application/x-ldjson"
		}
	default:
		if _, ok := mimetypeFormats[mimetype]; !ok {
			return ""
		}
	}
	return mimetype
}
