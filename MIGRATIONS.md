## Alembic migrations

This repo uses Alembic with async SQLAlchemy engine.

### 1) Install dependencies

```bash
python3 -m pip install -r requirements.txt
```

### 2) Run migrations

```bash
python3 -m alembic -c alembic.ini upgrade head
```

### 3) Create new migration (optional)

```bash
python3 -m alembic -c alembic.ini revision --autogenerate -m "your message"
```

