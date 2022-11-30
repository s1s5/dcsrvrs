# simple cache server
- `cargo watch -x run`
- `curl -X PUT -T readme.md http://localhost:8000/a.txt`
- `curl -X GET http://localhost:8000/a.txt`
- `curl -X DELETE http://localhost:8000/a.txt`
