# simple cache server
- `cargo watch -x run`
- `curl -X PUT -T readme.md http://localhost:8000/a.txt`
- `curl -X GET http://localhost:8000/a.txt`
- `curl -X DELETE http://localhost:8000/a.txt`

# test
- `cargo tarpaulin --skip-clean -o html --output-dir htmlcov`

export DATABASE_URL=sqlite:////tmp/db.sqlite?mode=rwc
 sea-orm-cli migrate -v
 
 sea-orm-cli generate entity -l -o entity/src
