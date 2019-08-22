# tap-sql (Postgres)

A service that extracts data from a Postgres database and outputs a stream for line delimited JSON.  Can be extended to support other RDMS systems as well such as SQL Server, MySQL etc.

NOTE: Not Ready For Production

## config file

```json
{
    "driver": "postgres",
    "connection": {
        "host": "<Host>",
        "user": "<user>",
        "password": "<password>",
        "port": "<port>",
        "dbname": "<dbname>",
        "sslmode": "disable"
    },
    "tablename": "<tablename>",
    "syncCol": "<syncCol>"
}
```

## state file

```json
{
    "LastRecord": 509015
}
```
