MODULE_big = bottledwater
EXTENSION = bottledwater

OBJS = logdecoder.o
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

$(OBJS): config.h

config.h:
ifdef AVRO
	  echo "#define AVRO" >$@
else
	  echo "#undef AVRO" >$@
endif
ifdef JSON
	  echo "#define JSON" >>$@
else
	  echo "#undef JSON" >>$@
endif

install: bottledwater--0.1.sql

bottledwater--0.1.sql:
	cat bottledwater-common--0.1.sql >$@
ifdef AVRO
	cat bottledwater-avro--0.1.sql >>$@
endif
ifdef JSON
	cat bottledwater-json--0.1.sql >>$@
endif

clean: clean-generated-files

clean-generated-files:
	rm -f config.h bottledwater--0.1.sql
