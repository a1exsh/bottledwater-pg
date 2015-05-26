CREATE OR REPLACE FUNCTION bottledwater_key_schema(name) RETURNS text
    AS 'bottledwater', 'bottledwater_key_schema' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION bottledwater_row_schema(name) RETURNS text
    AS 'bottledwater', 'bottledwater_row_schema' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION bottledwater_frame_schema() RETURNS text
    AS 'bottledwater', 'bottledwater_frame_schema' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION bottledwater_export(
        table_pattern text    DEFAULT '%',
        allow_unkeyed boolean DEFAULT false
    ) RETURNS setof bytea
    AS 'bottledwater', 'bottledwater_export' LANGUAGE C VOLATILE STRICT;

