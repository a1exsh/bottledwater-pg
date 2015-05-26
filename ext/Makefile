MODULE_big = bottledwater
EXTENSION = bottledwater

OBJS = logdecoder.o snapshot.o
DATA = bottledwater--0.1.sql

ifdef AVRO
OBJS += format-avro.o oid2avro.o io_util.o protocol_server.o protocol.o snapshot-avro.o

PG_CPPFLAGS += $(shell pkg-config --cflags avro-c)
SHLIB_LINK += $(shell pkg-config --libs avro-c)
endif

ifdef JSON
OBJS += format-json.o snapshot-json.o
endif

PG_CPPFLAGS += -std=c99

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

$(OBJS): make-config-h

make-config-h:
ifdef AVRO
	  echo "#define AVRO" >config.h
else
	  echo "#undef AVRO" >config.h
endif
ifdef JSON
	  echo "#define JSON" >>config.h
else
	  echo "#undef JSON" >>config.h
endif
