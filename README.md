# simple cache server
- `cargo watch -x run`
- `curl -X PUT -T readme.md http://localhost:8000/a.txt`
- `curl -X GET http://localhost:8000/a.txt`
- `curl -X DELETE http://localhost:8000/a.txt`
- `curl -X POST -H "Content-Type: application/json" -d '{"max_num":100, "key": "a.txt", "store_time": 1671671784, "prefix": null}'  http://localhost:8000/-/keys | jq`

# test
- `cargo test`
- `cargo tarpaulin --skip-clean -o html --output-dir htmlcov`
- `cargo run -p bench -- --server http://localhost:8000`
