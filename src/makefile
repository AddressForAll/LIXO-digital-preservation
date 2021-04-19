##
## Installing Project Digital Preservation
##

## -- -- VARS -- --

pg_io  =/tmp/pg_io

pg_uri_root ="postgres://postgres@localhost"
pg_db       ="dl03t_main"
pg_dbingest ="ingest2"
pg_uri      ="$(pg_uri_root)/$(pg_db)"
pg_uringest ="$(pg_uri_root)/$(pg_dbingest)"

countries = "'BR','CL','CO','EC','PE','VE'"  # conforme jurisdiction/get.sh
# pendente: incluir em info a propriedade Wikidata dos geocódigos numericos locais. Aí poderemos dispensar a lista de contries e automatizar get.sh pelo SQL.

## -- -- RULES -- --

ini_all: ini_sql

ini_sql:
	@echo "run SQL spcripts"
	mkdir -p $(pg_io)
	psql $(pg_uri) < /opt/gits/_a4a/WS/src/pubLib.sql
	psql $(pg_uri) < eclusa/step1-ini.sql
	# sh eclusa-step2a-ins.sh
	#/opt/gits/WS/src? psql $(pg_uri) < eclusa/step2b-ins.sql
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
	sh /opt/gits/digital-preservartion/src/eclusa/mkCpHashFiles.sh
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

##############

# Tests "load and check" of first-level local jurisdictions, maintained by Wikidata and OSM:
jurisdiction_iso_prepare:  # depends on prepared databases
	@echo "--- Jurisdiction prepare ingestion database ---"
	psql $(pg_uringest) -c "CREATE SCHEMA IF NOT EXISTS optim; DROP TABLE optim.jurisdiction;"
	pg_dump --format plain --table optim.jurisdiction $(pg_uri)  | psql $(pg_uringest)

jurisdiction_iso_run: jurisdiction_iso_prepare jurisdiction/get.sh jurisdiction/get.sql run_mustache.py jurisdiction/getWikidata.mustache
	@echo "--- Jurisdiction prepare bash file and LOAD ---"
	sh jurisdiction/get.sh
	ls -l /tmp/pg_io/wdquery-*.csv
	psql $(pg_uringest) < jurisdiction/get.sql

# edit SQL and run:
jurisdiction_iso_run2:  get-Latam5.sql get-Latam5.sh run_mustache.py getWikidata.mustache
	@echo "--- Jurisdiction prepare2 bash file and LOAD ---"
	sh jurisdiction/get.sh
	ls -l /tmp/pg_io/wdquery-*.csv
	psql $(pg_uringest) < jurisdiction/get.sql

jurisdiction_iso_test:
	psql $(pg_uringest) -c "\
	WITH RECURSIVE rec_ids AS (\
	   SELECT osm_id FROM optim.jurisdiction\
	     WHERE admin_level=2 AND abbrev IN ($(countries))\
	   UNION ALL\
	   SELECT sa.osm_id\
	    FROM optim.jurisdiction AS sa\
	    JOIN rec_ids ON rec_ids.osm_id = sa. parent_id\
	)\
	 SELECT isolabel_ext,  osm_id, abbrev, name, name_en, parent_id, lexlabel, wikidata_id, jurisd_base_id, jurisd_local_id, admin_level\
	 FROM optim.jurisdiction\
	 WHERE osm_id IN (SELECT osm_id FROM rec_ids) order by 1\
	"
