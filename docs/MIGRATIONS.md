# Migration Guide

This guide explains how Alembic migrations work in NexusChat.

## Architecture

NexusChat uses two separate Alembic migration chains, one per database:

1. **auth** — manages the `users` table in the auth-db (PostgreSQL on port 5432)
2. **shard** — manages `chats`, `chat_members`, and `messages` tables in shard databases (shard0:5433, shard1:5435)

Each chain lives in its own directory:

```
alembic/
├── auth/
│   ├── env.py                 # ASYNC engine config for auth-db
│   ├── script.py.mako         # Migration template
│   └── versions/              # Auth migration revisions
│       └── 0001_*.py          # Initial: create users table
├── shard/
│   ├── env.py                 # ASYNC engine config for shard-db
│   ├── script.py.mako         # Migration template
│   └── versions/              # Shard migration revisions
│       └── 0001_*.py          # Initial: create chats, chat_members, messages tables
└── requirements.txt           # Dependencies (alembic, asyncpg, sqlalchemy)
```

## Workflow

### 1. Setting Up for the First Time

```bash
# Install dependencies
pip install -r alembic/requirements.txt

# Set database URLs (adjust as needed)
set AUTH_DATABASE_URL=postgresql+asyncpg://nexuschat:nexuschat@localhost:5432/nexuschat
set SHARD_DATABASE_URL=postgresql+asyncpg://nexuschat:nexuschat@localhost:5433/nexuschat

# Apply all migrations
alembic -c alembic.ini -n auth upgrade head
alembic -c alembic.ini -n shard upgrade head
```

### 2. Making Schema Changes

After modifying a model (e.g., adding a column to `users`):

```bash
# Auto-generate a migration (review carefully before applying!)
alembic -c alembic.ini -n auth revision --autogenerate -m "add avatar_url to users"

# Review the generated file in alembic/auth/versions/
# Then apply it:
alembic -c alembic.ini -n auth upgrade head
```

> **Note:** For `--autogenerate` to work, you need to set `target_metadata` in `env.py` to your SQLAlchemy `Base.metadata`. Currently it is set to `None` for raw migration operations.

### 3. Manual Migrations

For complex changes (e.g., data migrations), create an empty revision and write raw SQL:

```bash
alembic -c alembic.ini -n auth revision -m "backfill user display names"
```

Then edit the generated file using `op.execute()`:

```python
def upgrade():
    op.execute("UPDATE users SET display_name = username WHERE display_name IS NULL")
```

## Docker Integration

When running inside Docker Compose, the services use the init SQL scripts in `infra/postgres/` to bootstrap the schema. The Alembic migrations are meant to:

- Replace these init scripts over time
- Track incremental schema changes
- Provide rollback capability

To run migrations inside Docker:

```bash
# Exec into a container that has access to the DB
docker exec -it <auth-service-container> bash

# Install alembic if needed
pip install alembic asyncpg sqlalchemy

# Run migrations (DB hosts resolve to Docker service names)
AUTH_DATABASE_URL="postgresql+asyncpg://nexuschat:nexuschat@postgres-auth:5432/nexuschat" \
  alembic -c alembic.ini -n auth upgrade head
```

## Best Practices

1. **Always review auto-generated migrations** — `--autogenerate` is not perfect and may miss some changes
2. **One logical change per migration** — makes rollback easier to reason about
3. **Test migrations on a copy of production data** before running in production
4. **Never edit existing migrations** once they've been applied to any environment
5. **Use `downgrade()`** to make rollback safe for every migration
6. **Commit migration files** alongside the model changes that require them
