# datomic-py

A Python library for accessing the [Datomic](http://www.datomic.com) database via its REST API, including an EDN (Extensible Data Notation) parser.

## Installation

```bash
pip install datomic-py
```

Or with uv:

```bash
uv add datomic-py
```

## Requirements

- Python 3.12+
- httpx

## Usage

### REST Client

```python
from datomic_py import Datomic

# Connect to Datomic
conn = Datomic('http://localhost:3000/', 'my-storage')

# Create a database
db = conn.create_database('my-db')

# Define a schema and transact
schema = '''[{
  :db/id #db/id[:db.part/db]
  :db/ident :person/name
  :db/valueType :db.type/string
  :db/cardinality :db.cardinality/one
  :db/doc "A person's name"
  :db.install/_attribute :db.part/db}]'''

db.transact(schema)

# Add data
db.transact('[{:db/id #db/id[:db.part/user] :person/name "Alice"}]')

# Query
results = db.query('[:find ?e ?n :where [?e :person/name ?n]]')
print(results)  # ((17592186045417, 'Alice'),)

# Get entity by ID
entity = db.entity(17592186045417)
```

### EDN Parser

```python
from datomic_py import edn_loads

# Parse EDN strings
edn_loads('42')           # 42
edn_loads('"hello"')      # 'hello'
edn_loads(':keyword')     # ':keyword'
edn_loads('[1 2 3]')      # (1, 2, 3)
edn_loads('{:a 1 :b 2}')  # {':a': 1, ':b': 2}
edn_loads('#{1 2 3}')     # frozenset({1, 2, 3})
edn_loads('true')         # True
edn_loads('nil')          # None

# Special types
edn_loads('#inst "2023-01-15T10:30:00.000-00:00"')  # datetime
edn_loads('#uuid "550e8400-e29b-41d4-a716-446655440000"')  # UUID
```

### Schema Helpers

```python
from datomic_py import Attribute, Schema, STRING, BOOLEAN, ONE, IDENTITY

schema = Schema(
    Attribute(':user/email', STRING, unique=IDENTITY, index=True),
    Attribute(':user/name', STRING, cardinality=ONE),
    Attribute(':user/active', BOOLEAN),
)
```

## Development

This project uses [uv](https://github.com/astral-sh/uv) for dependency management.

```bash
# Create virtual environment
uv venv --python 3.12

# Activate
source .venv/bin/activate

# Install dependencies
uv sync --all-extras

# Run tests
pytest

# Run tests with coverage
pytest --cov=datomic_py --cov-report=term-missing

# Run linting
ruff check .
```

## License

MIT License - see LICENSE file for details.

## Credits

Migrated to Python 3.12 with modern tooling (uv, pytest, pyproject.toml).
Based on the original `pydatomic` project: https://github.com/gns24/pydatomic