CREATE OR REPLACE FUNCTION bottledwater_schema_json(
        relname text,
        relnamespace text DEFAULT NULL
    ) RETURNS text
    AS 'bottledwater', 'bottledwater_schema_json' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION bottledwater_export_json(
        relname text,
        relnamespace text DEFAULT NULL,
        nochildren boolean DEFAULT FALSE
    ) RETURNS setof text
    AS 'bottledwater', 'bottledwater_export_json' LANGUAGE C VOLATILE;

