# Alembic Migrations

This project uses Alembic for database migrations with two independent migration chains:

- **auth** — for the auth-database (`users` table)
- **shard** — for shard databases (`chats`, `chat_members`, `messages` tables)

## Prerequisites

```bash
pip install -r alembic/requirements.txt
```

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `AUTH_DATABASE_URL` | `postgresql+asyncpg://nexuschat:nexuschat@localhost:5432/nexuschat` | Auth DB connection string |
| `SHARD_DATABASE_URL` | `postgresql+asyncpg://nexuschat:nexuschat@localhost:5433/nexuschat` | Shard DB connection string |

## Running Migrations

```bash
# Run all pending auth-db migrations
alembic -c alembic.ini -n auth upgrade head

# Run all pending shard migrations
alembic -c alembic.ini -n shard upgrade head
```

## Creating New Migrations

```bash
# Create a new auth migration
alembic -c alembic.ini -n auth revision -m "description"

# Create a new shard migration
alembic -c alembic.ini -n shard revision -m "description"
```

## Rollback

```bash
# Rollback one step on auth-db
alembic -c alembic.ini -n auth downgrade -1

# Rollback one step on shard
alembic -c alembic.ini -n shard downgrade -1

# Rollback to a specific revision
alembic -c alembic.ini -n auth downgrade <revision_id>

# Rollback all (undo everything)
alembic -c alembic.ini -n auth downgrade base
alembic -c alembic.ini -n shard downgrade base
```

## Viewing Migration History

```bash
alembic -c alembic.ini -n auth history
alembic -c alembic.ini -n shard history
```

## Checking Current State

```bash
alembic -c alembic.ini -n auth current
alembic -c alembic.ini -n shard current
```
