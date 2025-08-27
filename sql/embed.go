package embed

import _ "embed"

//go:embed schema_sqlite.sql
var SqliteSchema string

//go:embed schema_chai.sql
var ChaiSchema string

//go:embed schema_postgres.sql
var PgSchema string
