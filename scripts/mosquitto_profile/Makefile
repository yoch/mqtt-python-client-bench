# Ceiling probe helpers (not used by `report build`).
#
#   make -C scripts/mosquitto_profile
#   # see docs/MOSQUITTO_PROFILING.md
CC ?= gcc
CFLAGS ?= -O2 -pthread -Wall
BIN ?= mqtt_hammer

.PHONY: all clean

all: $(BIN)

$(BIN): mqtt_hammer.c
	$(CC) $(CFLAGS) -o $@ $<

clean:
	rm -f $(BIN)
