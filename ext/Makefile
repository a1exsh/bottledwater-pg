MODULE_big = samza_postgres
EXTENSION = samza_postgres

AVRO_CFLAGS = $(shell pkg-config --cflags avro-c)
AVRO_LDFLAGS = $(shell pkg-config --libs avro-c)

PG_CPPFLAGS += $(AVRO_CFLAGS)
SHLIB_LINK += $(AVRO_LDFLAGS)

OBJS = io_util.o logdecoder.o oid2avro.o protocol.o snapshot.o
DATA = samza_postgres--0.1.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)