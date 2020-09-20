----
-- Public library for dl03t_main and other databases.

CREATE or replace FUNCTION text_to_boolean(x text, as_null boolean DEFAULT NULL) RETURNS boolean AS $f$
  SELECT CASE
    WHEN x IS NULL OR x=''  THEN as_null -- NULL or false
    WHEN x='0' OR x='false' THEN false
    ELSE true
  END
$f$ language SQL immutable;

CREATE or replace FUNCTION pg_read_file(f text, missing_ok boolean) RETURNS text AS $$
  SELECT pg_read_file(f,0,922337203,missing_ok) -- max. of ~800 Mb or 880 MiB = 0.86 GiB
   -- GAMBI, ver https://stackoverflow.com/q/63299550/287948
   -- ou usar jsonb_read_stat_file()
$$ LANGUAGE SQL IMMUTABLE;

CREATE or replace FUNCTION jsonb_read_stat_file(
  f text,
  missing_ok boolean DEFAULT false
) RETURNS JSONb AS $f$
  SELECT j || jsonb_build_object( 'file',f,  'content',pg_read_file(f) )
  FROM to_jsonb( pg_stat_file(f,missing_ok) ) t(j)
  WHERE j IS NOT NULL
$f$ LANGUAGE SQL IMMUTABLE;

CREATE or replace FUNCTION lexname_to_unix(p_lexname text) RETURNS text AS $$
  SELECT string_agg(initcap(p),'') FROM regexp_split_to_table($1,'\.') t(p)
$$ LANGUAGE SQL IMMUTABLE;


CREATE or replace FUNCTION pg_tablestruct_dump_totext(p_tabname text) RETURNS text[]  AS $f$
  SELECT array_agg(col||' '||datatype)
  FROM (
    SELECT -- attrelid::regclass AS tbl,
           attname            AS col
         , atttypid::regtype  AS datatype
    FROM   pg_attribute
    WHERE  attrelid = p_tabname::regclass  -- table name, optionally schema-qualified
    AND    attnum > 0
    AND    NOT attisdropped
    ORDER  BY attnum
  ) t
$f$ language SQL IMMUTABLE;
COMMENT ON FUNCTION pg_tablestruct_dump_totext
  IS 'Extraxcts column descriptors of a table. Used in ingest.fdw_generate_getclone() function.'
;