CREATE TABLE IF NOT EXISTS "client" (
    id         bigserial PRIMARY KEY,
    name       varchar(255)                NOT NULL,
    is_deleted boolean                     NOT NULL DEFAULT FALSE,
    created_at timestamp without time zone NOT NULL DEFAULT NOW(),
    updated_at timestamp without time zone NOT NULL DEFAULT NOW()
);