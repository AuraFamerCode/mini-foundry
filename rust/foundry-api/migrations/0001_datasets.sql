CREATE TABLE datasets (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL UNIQUE CHECK (btrim(name) <> ''),
    schema_json JSONB NOT NULL CHECK (jsonb_typeof(schema_json) = 'array'),
    primary_key TEXT NOT NULL CHECK (btrim(primary_key) <> ''),
    version BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
    row_count BIGINT NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE dataset_rows (
    dataset_id UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    row_key TEXT NOT NULL,
    row_json JSONB NOT NULL CHECK (jsonb_typeof(row_json) = 'object'),
    PRIMARY KEY (dataset_id, row_key)
);

CREATE TABLE dataset_transactions (
    dataset_id UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    version BIGINT NOT NULL CHECK (version > 0),
    operation TEXT NOT NULL CHECK (operation IN ('snapshot', 'append', 'delete')),
    delta_json JSONB NOT NULL,
    rows_affected BIGINT NOT NULL CHECK (rows_affected >= 0),
    committed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (dataset_id, version)
);
