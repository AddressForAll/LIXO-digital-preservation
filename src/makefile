##
## Installing Project Digital Preservation
##

## -- -- VARS -- --

pg_io  =/tmp/pg_io
pg_uri ="postgres://postgres@localhost/dl03t_main"

## -- -- RULES -- --

ini_all: ini_sql

ini_sql:
	@echo "run SQL spcripts"
	mkdir -p $(pg_io)
	psql $(pg_uri) < pubLib.sql
	psql $(pg_uri) < eclusa-step1-ini.sql
	# sh eclusa-step2a-ins.sh
	psql $(pg_uri) < eclusa-step2b-ins.sql
	touch /opt/lock-mkStatus/digitalPreservation-makeSQL.~feito

## ECLUSA, commands to execute by root at crontab:

ecl_run: /opt/lock-mkStatus/digitalPreservation-makeSQL.~feito
	whoami
	pwd
	@echo "Run with sudo! (idempotent)"
	mkdir -p $(pg_io)
	chown postgres:postgres -R $(pg_io)
	# make -C /opt/gits/WS/src ini_tmprefresh
	psql $(pg_uri) -c "SELECT optim.fdw_wgets_script('refresh')"
	sh $(pg_io)/run_wgets-refresh.sh
	psql $(pg_uri) -c "SELECT optim.fdw_wgets_refresh(false)" # (donor e donatedpack) true on master
	sh /opt/gits/digital-preservartion/src/eclusa-mkCpHashFiles.sh
	psql $(pg_uri) -c "SELECT * FROM eclusa.vw03alldft_cityfolder_ins" # insert into origin

ecl_out: ecl_run
	# testar se $$(pg_io) funciona
	psql $(pg_uri) -c "COPY (SELECT * FROM optim.vwdump_origin) TO '$$(pg_io)/br-origin.csv' CSV HEADER"
	# tem API nao precisa disso?

# core inserts
ini_ins:
	@echo "run SQL spcripts"
	mkdir -p $(pg_io)
	psql $(pg_uri) -c "SELECT optim.fdw_wgets_script()"
	sh $(pg_io)/run_wgets-all.sh
  # falta rodar resto


## testing ... correct is to wget
ins_step1: # LIXO
	mkdir -p $(pg_io)/digital-preservartion-XX
	rm -f $(pg_io)/digital-preservartion-XX/*.csv
	for gitdir in /opt/gits/digital-preservartion-*; do \
	   echo "Refreshing $$gitdir and copying its CSVs..."; \
	   git -C "$$gitdir" pull; \
	   cp "$$gitdir/data/"*.csv $(pg_io)/digital-preservartion-XX; \
	done
