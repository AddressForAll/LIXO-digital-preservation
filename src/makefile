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
	sh eclusa-step2a-ins.sh
	psql dl03t_main < eclusa-step2b-ins.sql
	touch /opt/lock-mkStatus/digitalPreservation-makeSQL.~feito

## ECLUSA, commands to execute by root at crontab:

ecl_run: /opt/lock-mkStatus/digitalPreservation-makeSQL.~feito
	sh eclusa-mkCpHashFiles.sh

#falta ecl_enioFiles_step3_insert: conclue o proceso de insert dos casos válidos de pré-preservação
