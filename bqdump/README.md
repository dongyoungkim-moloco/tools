# Example usage

1. Load clipboard with your desired query (Saved Queries are not accessible by `bq` CLI atm)
2. Run
```
pbpaste | bq query --format=prettyjson --use_legacy_sql=false | bqdump
```
