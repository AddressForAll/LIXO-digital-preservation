----
-- Public library for dl03t_main and other databases.

CREATE or replace FUNCTION text_to_boolean(x text) RETURNS boolean AS $f$
  SELECT CASE
    WHEN x IS NULL OR x=''  THEN NULL
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
