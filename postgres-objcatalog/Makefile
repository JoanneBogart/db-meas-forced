EXTENSION = objcatalog
DATA_built = sql/$(EXTENSION)--1.0.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

$(DATA_built): sql/$(EXTENSION).sql.in
	cat $^ > $@
