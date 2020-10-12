##
## Installing Project Digital Preservation
##

## -- -- VARS -- --

## -- -- RULES -- --

ini_all: ini_sql

ini_sql:
	@echo "run SQL spcripts"
	mkdir -p /tmp/pg_io
	psql dl03t_main < pubLib.sql
	psql dl03t_main < eclusa-step1-ini.sql
	# sh eclusa-step2a-ins.sh
	psql dl03t_main < eclusa-step2b-ins.sql
	touch /opt/lock-mkStatus/digitalPreservation-makeSQL.~feito

## ECLUSA, commands to execute by root at crontab:

ecl_run: /opt/lock-mkStatus/digitalPreservation-makeSQL.~feito
	whoami
	@echo "Run with sudo! (idempotent)"
	# make -C /opt/gits/WS/src ini_tmprefresh
	psql dl03t_main -c "SELECT optim.fdw_wgets_script('refresh')"
	sh /tmp/pg_io/run_wgets-refresh.sh
	psql dl03t_main -c "SELECT optim.fdw_wgets_refresh(false)" # (donor e donatedpack) true on master
	sh eclusa-mkCpHashFiles.sh
	psql dl03t_main -c "SELECT * FROM eclusa.vw03alldft_cityfolder_ins" # insert into origin

ecl_out: ecl_run
	psql dl03t_main -c "COPY (SELECT * FROM optim.vwdump_origin) TO '/tmp/pg_io/br-origin.csv' CSV HEADER"
	# ??

#falta ecl_enioFiles_step3_insert: conclue o proceso de insert dos casos válidos de pré-preservação

# core inserts
ini_ins:
	@echo "run SQL spcripts"
	mkdir -p /tmp/pg_io
	psql dl03t_main -c "SELECT optim.fdw_wgets_script()"
	sh /tmp/pg_io/run_wgets-all.sh
  # falta rodar resto


## testing ... correct is to wget
ins_step1: # LIXO
	mkdir -p /tmp/pg_io/digital-preservartion-XX
	rm -f /tmp/pg_io/digital-preservartion-XX/*.csv
	for gitdir in /opt/gits/digital-preservartion-*; do \
	   echo "Refreshing $$gitdir and copying its CSVs..."; \
	   git -C "$$gitdir" pull; \
	   cp "$$gitdir/data/"*.csv /tmp/pg_io/digital-preservartion-XX; \
	done
