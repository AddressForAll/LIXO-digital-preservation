--
-- Eclusa de dados: trata os dados do upload e os transfere para a ingestão.
-- (Data Canal-lock)
-- O dado só entra depois que sha256 é confirmado fora (upload correto) e dentro da base (nao-duplicação)
-- A ficha completa de metadados é gerada pela eclusa: contagens no filesystem e no PostGIS.
-- PS: ainda assim o relatório de relevância, indicando se trouche informação nova e aparentemente consistente, só vem depois.
--
-- Efetua o scan, validação, geração de comandos e ingestão de dados já padronizados.
--

CREATE extension IF NOT EXISTS postgis;
CREATE extension IF NOT EXISTS adminpack;

CREATE EXTENSION IF NOT EXISTS file_fdw;
CREATE SERVER    IF NOT EXISTS files FOREIGN DATA WRAPPER file_fdw;

CREATE schema    IF NOT EXISTS ingest;
CREATE schema    IF NOT EXISTS tmp_orig;

----
-- Public lib:

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


-- -- -- -- -- -- -- -- --
-- inicializações DATASETS:

CREATE TABLE ingest.city ( -- only current city
  ibge_id int   NOT NULL PRIMARY KEY, -- BR official id, control
  name    text  NOT NULL CHECK(length(name)<60), -- admin-level3
  state   text  NOT NULL CHECK(length(state)=2), -- UF, admin-level2
  abbrev3 text  CHECK(length(abbrev3)=3),
  wikidata_id  bigint,  --  from '^Q\d+'
  lexlabel     text NOT NULL,  -- cache from name. 'sao.paulo'
  isolabel_ext text NOT NULL,  -- cache from name and state. BR-SP-SaoPaulo
  ddd          integer,
  info JSONb, -- postalCode_ranges, notes,   creation, extinction
  UNIQUE(state,name),
  UNIQUE(state,lexlabel)
);

CREATE TABLE ingest.donor (
  id serial NOT NULL primary key,
  scope text, -- city code or country code
  vat_id text,
  shortname text,
  legalName text NOT NULL,
  wikidata_id bigint,
  url text,
  info JSONb,
  UNIQUE(vat_id),
  UNIQUE(scope,legalName)
);
-- -- -- -- -- -- -- -- --
-- inicializações INGEST:

CREATE TABLE IF NOT EXISTS ingest.donatedPack(
  pack_id int NOT NULL PRIMARY KEY,
  donor_id int NOT NULL REFERENCES ingest.donor(id),
  accepted_date date,
  about text,
  info jsonb,
  UNIQUE(pack_id)
); -- pack é só intermediário de agregação
INSERT INTO ingest.donatedPack (pack_id,donor_id,accepted_date,about,info)
  SELECT pack_id,donor_id,accepted_date::date,about,
         jsonb_build_object(
             'label_ref',label_ref,'creator',creator,
             'licensedExp',licensedExp,'license_main',license_main,
             'url_objType',url_objType, 'uri',uri
         )
  FROM tmp_orig.fdw_br_donatedPack
  WHERE length(accepted_date)>=4 AND donor_id IN (select id from ingest.donor)
ON CONFLICT DO NOTHING
;
-- verifica por hora com: select scope,vat_ID,shortName,legalName, p.pack_id, p.donor_id from ingest.donor d inner join (select pack_id, donor_id, info->>'label_ref' labref from ingest.donatedPack) p ON p.labref=d.shortname;
-- assim podemos trocaar o antigo

CREATE TABLE IF NOT EXISTS ingest.origin(
   id serial     NOT NULL PRIMARY KEY,
   city_id int NOT NULL REFERENCES ingest.city(ibge_id), -- escopo dos dados, desmembrando se possível.
   --donor_id int REFERENCES ingest.donor(id),
   pack_id int NOT NULL REFERENCES ingest.donatedPack(pack_id), -- um ou mais origins no mesmo paxck.
   fhash text    NOT NULL, -- sha256 is a finger print
   cityname text NOT NULL, -- city name
   fname text    NOT NULL,  -- filename
   fversion smallint NOT NULL DEFAULT 1, -- version counter
   ctype text, -- content type
   config jsonb, -- example '{"staging_db":"ingest1"}' ou tmplixo1 para nao poluir std.
   cmds text[],  -- conforme config; uso posterior para guardar sequencia de comandos.
   is_valid boolean NOT NULL DEFAULT false,
   is_open boolean NOT NULL DEFAULT true,
   fmeta jsonb,
   ingest_instant timestamp DEFAULT now(),
   UNIQUE(fhash),
   UNIQUE(cityname,fname,fversion) -- ,kx_ingest_date=ingest_instant::date
);

CREATE VIEW ingest.vw01_origin AS
  SELECT o.*,
         c.name as city_name, c.state as city_state,
         c.abbrev3 AS city_abbrev3, c.isolabel_ext AS city_isolabel_ext,
         d.vat_id AS donor_vat_id, d.shortname AS donor_shortname,
         d.legalName AS donor_legalName, d.url AS donor_url
  FROM ingest.origin o
       INNER JOIN ingest.city c ON o.city_id=c.ibge_id
       LEFT JOIN ingest.donor d ON o.donor_id=d.id
;

-- -- -- -- -- -- -- -- --
-- inicializações ECLUSA:

CREATE SCHEMA IF NOT EXISTS eclusa; -- módulo complementar do Schema ingest.

CREATE or replace FUNCTION eclusa.cityfolder_input_files(
  p_fpath text DEFAULT '/home/igor',
  p_excludefiles text[] DEFAULT array['sha256sum.txt','README.md']
) RETURNS TABLE (
  fid int, cityname text, fname text, ctype text, is_valid boolean, fmeta jsonb
) AS $f$

  WITH t0 AS ( SELECT rtrim(p_fpath,'/') AS fpath )
  , t1 AS (
    SELECT f as cityname,
           t0.fpath ||'/'|| f as f
    FROM pg_ls_dir((SELECT fpath FROM t0)) t(f), t0
    WHERE    f ~ '^BR\-[A-Z]{2,2}\-[A-Za-z]+$'
  )
  ,tres AS (
    SELECT cityname, fname, 'std' AS ctype,
         to_jsonb( pg_stat_file(fpath||'/'||fname) ) || jsonb_build_object('fpath',fpath) fmeta
    FROM (  -- t2:
      SELECT cityname,
             f||'/'||'input' as fpath,
             pg_ls_dir(f||'/'||'input') as fname
      FROM t1
      ORDER BY 1,3
    ) t2
  ), tres2 AS ( -- main query:

  SELECT (row_number() OVER ())::int id,
          cityname , fname, ctype,
          fname ~* '\.(zip|gz|rar|geojson|csv|dwg|pdf)$' AS is_validext,
          fmeta || jsonb_build_object( 'ext', (regexp_match(fname,'\.([^/\.]+)$'))[1]  ) AS fmeta
  FROM ( -- t3:

    SELECT * FROM tres WHERE not((fmeta->'isdir')::boolean)

    UNION

    SELECT cityname, fname,ctype,
           to_jsonb( pg_stat_file(fpath||'/'||fname) ) || jsonb_build_object('fpath',fpath) AS fmeta
    FROM ( -- t4:
      select cityname, fname as ctype,
             (fmeta->>'fpath')||'/'|| fname AS fpath,
             pg_ls_dir((fmeta->>'fpath')||'/'||fname) AS fname
      from tres
      where (fmeta->'isdir')::boolean
    ) t4

  ) t3
  WHERE NOT( fname=ANY(p_excludefiles) )
  ) -- \tres2
  SELECT id, cityname, fname, ctype, is_validext,
         fmeta || CASE WHEN is_validext THEN '{}'::jsonb ELSE jsonb_build_object('is_valid_err','#ER01: file extension unknown; ') END
  FROM tres2
$f$ language SQL immutable;


CREATE or replace FUNCTION eclusa.read_hashsum(
  p_file text -- for example '/tmp/pg_io/sha256sum.txt'
) RETURNS TABLE (hash text, hashtype text, file text, refpath text) AS $f$
  SELECT x[1] as hash,  hashtype, x[2] as file,
         regexp_replace(p_file, '/?([^/]+)\.txt$', '') -- refpath
  FROM (
    SELECT regexp_split_to_array(line,'\s+\*?') AS x,
           (regexp_match(p_file, '([^/]+)\.txt$'))[1]
           || '-'
           || CASE WHEN (regexp_match(line, '\s+(\*)'))[1]='*' THEN 'bin' ELSE 'text' END
           AS hashtype
    FROM regexp_split_to_table(  pg_read_file(p_file,true),  E'\n'  ) t(line)
  ) t2
  WHERE x is not null AND x[1]>''
$f$ language SQL immutable;

CREATE or replace FUNCTION eclusa.cityfolder_input( -- joined
  p_fpath text DEFAULT '/tmp/pg_io/',
  checksum_file text DEFAULT 'sha256sum.txt'
) RETURNS TABLE (fid int, cityname text, fname text, ctype text, is_valid boolean, fmeta jsonb) AS $f$
   WITH t AS (
      SELECT cf.*, y.ibge_id, fmeta->>'fpath' AS fpath
      FROM eclusa.cityfolder_input_files(p_fpath) cf
           LEFT JOIN ingest.city y ON cf.cityname=y.isolabel_ext
   )
   SELECT t.fid, t.cityname, t.fname, t.ctype,
          t.is_valid AND k2.hash is not null AS is_valid,
          CASE WHEN t.ibge_id IS NULL THEN t.fmeta    || jsonb_build_object('is_valid_err', '#ER02: cityname unknown')
               WHEN k2.hash is not null THEN  t.fmeta || jsonb_build_object('ibge_id',t.ibge_id, 'hash',k2.hash, 'hashtype', k2.hashtype)
               ELSE t.fmeta || jsonb_build_object('ibge_id',t.ibge_id, 'is_valid_err', COALESCE(t.fmeta->>'is_valid_err','')||'#ER03: hash not generated; ')  END
   FROM t LEFT JOIN (
           SELECT k.*
           FROM (SELECT DISTINCT fmeta->>'fpath' as fpath FROM t) t2,
                LATERAL eclusa.read_hashsum( t2.fpath||'/'||checksum_file) k
           WHERE t2.fpath=k.refpath -- and k.refpath is not null
   ) k2
   ON k2.refpath = t.fpath AND t.fname=k2.file
   WHERE t.fname!=checksum_file -- nome reservado!
   ORDER BY t.cityname, t.fname
$f$ language SQL immutable;

-- -- -- -- --
-- Carga da origem e geração de comandos para a ingestão final:

CREATE or replace FUNCTION ingest.cityfolder_insert(
  p_in_path     text DEFAULT '/tmp/pg_io/CITY',
  p_eclusa_path text DEFAULT '/tmp/pg_io/eclusa',
  p_db          text DEFAULT 'ingest1', -- ou tmplixo
  p_especifico  text DEFAULT ''
) RETURNS text AS $f$
  -- idempotente no estado: chamamos a função n vezes com os mesmos parâmetros e o resultado é sempre o mesmo.
-- SELECT datname FROM pg_stat_activity where client_port=-1 LIMIT 1
  INSERT INTO ingest.origin(city_id,fhash,cityname, fname,ctype,is_valid, fmeta, config)
   SELECT (fmeta->'ibge_id')::int, fmeta->>'hash', cityname,
          fname, ctype, is_valid,
          (fmeta - 'hash') || jsonb_build_object( 'user_resp', regexp_match(p_in_path,'^/home/([^/]+)') ),
          jsonb_build_object('staging_db',t1.datname)
   FROM eclusa.cityfolder_input( rtrim(p_in_path,'/') ),
        (SELECT datname FROM pg_stat_activity where client_port=-1 LIMIT 1) t1
   WHERE is_valid
  ON CONFLICT DO NOTHING;
  -- Comandos de uso geral:
  UPDATE ingest.origin SET cmds = array[
             concat('mkdir -p ', rtrim(p_eclusa_path,'/'), '/orig', id),
             concat('cd ', rtrim(p_eclusa_path,'/'), '/orig', id),
             CASE WHEN fmeta->>'ext'='zip' THEN concat('unzip -j ',fmeta->>'fpath','/',fname) ELSE '' END
          ]
  WHERE is_open AND is_valid
        AND cmds IS NULL -- novos
  ;
  -- Comandos p_especifico='shp_sampa1':
  UPDATE ingest.origin SET cmds = cmds ||  concat(
       -- SRID 31983 precisa estar nos metadados.
       'shp2pgsql -s 31983 ', regexp_replace(fname,'\.([^\.\/]+)$',''),'.shp tmp_orig.t',id,'_01 | psql '|| p_db
     )
  WHERE p_especifico='shp_sampa1' AND is_open AND is_valid
        AND array_length(cmds,1)=3 -- novos
        AND ctype='lotes'          -- só neste caso
  ;
  SELECT '... insert/tentativa realizado, comandos: '
     ||E'\n* '|| (SELECT COUNT(*) FROM ingest.origin WHERE is_open AND is_valid) ||' origens em aberto.'
     ||E'\n* '|| (SELECT COUNT(*) FROM ingest.origin WHERE is_open AND NOT(is_valid)) ||E' origens com defeito.\n'
$f$ language SQL VOLATILE;


CREATE or replace FUNCTION ingest.cityfolder_generate_views_tpl1(
  p_vwnane text DEFAULT 'vw0_union1'
) RETURNS text  AS $f$
BEGIN
 EXECUTE
    'CREATE or replace VIEW  tmp_orig.'|| p_vwnane ||' AS '
    || (
      SELECT string_agg( 'SELECT '|| id ||' gid_prefix, * FROM tmp_orig.t'||id||'_01 ', E'\n  UNION \n' )
      FROM ingest.origin WHERE is_open AND is_valid AND ctype='lotes' AND array_length(cmds,1)=4
    );
    return 'VIEW tmp_orig.'|| p_vwnane || ' was created!';
END;
$f$ language PLpgSQL;

CREATE or replace FUNCTION ingest.fdw_generate( -- ainda nao esta em uso, revisar!!
  -- foreign-data wrapper generator
  p_source_id int,
  p_subsource_id int,      -- default 1
  p_field_desc text[],      -- pairs
  p_path text DEFAULT NULL  -- default based on ids
) RETURNS text  AS $f$
DECLARE
 fdwname text;
BEGIN
 fdwname := 'tmp_orig.fdw'|| p_source_id ||'_'|| p_subsource_id;
 EXECUTE
    'CREATE FOREIGN TABLE '|| fdwname ||'('
    || (
      SELECT array_to_string( concat(p_field_desc[i*2+1], p_field_desc[i*2+2]), ', ' )
      FROM (SELECT generate_series(0,array_length(p_field_desc,1)/2 - 1)) g(i)
    )
    ||') SERVER files OPTIONS '
    || format(
       "(filename %L, format %L, header %L, delimiter %L)",
       p_path||'/x'||p_source_id, 'csv', 'true', ','
    );
    -- .. FROM ingest.origin WHERE is_open AND is_valid AND ctype='lotes' AND array_length(cmds,1)=4
    return 'VIEW tmp_orig.'|| fdwname || ' was created!';
END;
$f$ language PLpgSQL;

-- mudar para esquema eclusa?
CREATE or replace FUNCTION ingest.cityfolder_cmds_to_run(
  p_staging_db text DEFAULT 'ingest1',
  p_output_shfile text DEFAULT '/tmp/pg_io/run.sh'
) RETURNS text AS $f$
  SELECT pg_catalog.pg_file_unlink(p_output_shfile);
  SELECT E'\nGravados '|| pg_catalog.pg_file_write(
     p_output_shfile,
     string_agg( cmd_blk, E'\n' )
     || E'\n\n psql '|| p_staging_db || E' -c "SELECT ingest.cityfolder_generate_views_tpl1()"\n',
     false
   )::text || ' bytes em '|| p_output_shfile ||E' \n'
  FROM (
    SELECT E'\n\n# orig'|| id ||E'\n'|| array_to_string(cmds,E'\n')
    FROM ingest.origin WHERE is_open AND is_valid
    ORDER BY id
  ) t(cmd_blk)
$f$ language SQL immutable;

----- GERADORES DE Comando

CREATE or replace FUNCTION eclusa.cityfolder_runhashes(
  p_path text, -- ex. /home/igor
  p_output_shfile text DEFAULT '/tmp/pg_io/runHashes.sh'
) RETURNS text AS $f$
  SELECT pg_catalog.pg_file_unlink(p_output_shfile);
  SELECT E'\nGravados '|| pg_catalog.pg_file_write(
    p_output_shfile,
    string_agg( cmd, E'\n' ),
    false
  )::text ||' bytes em '|| p_output_shfile ||E' \n' as fim
  FROM (
    SELECT distinct concat(
        'cd ', fmeta->>'fpath', '; sha256sum -b *.* > sha256sum.txt; chmod 666 sha256sum.txt'
      ) as cmd
    FROM eclusa.cityfolder_input_files(p_path)
    ORDER BY 1
  ) t
$f$ language SQL immutable;

CREATE or replace FUNCTION eclusa.cityfolder_run_cpfiles(
  p_user text, -- ex. igor
  p_output_shfile text DEFAULT '/tmp/pg_io/runCpFiles.sh'
) RETURNS text AS $f$
  SELECT pg_catalog.pg_file_unlink(p_output_shfile);
  SELECT E'\nGravados '|| pg_catalog.pg_file_write(
    p_output_shfile,
    string_agg( cmd, E'\n' ),
    false
  )::text ||' bytes em '|| p_output_shfile ||E' \n' as fim
  FROM (
    SELECT concat(
      'cp "', fmeta->>'fpath', '/', fname, '" ',
      '/var/www/preserv.addressforall.org/download/', fmeta->>'hash','.', fmeta->>'ext'
    )
    FROM eclusa.cityfolder_input('/home/'||p_user) where is_valid
) t(cmd)
$f$ language SQL immutable;

----- expondo na API!

CREATE or replace FUNCTION API.cityfolder_input_files_user(
    p_user text DEFAULT 'igor',
    p_is_valid text DEFAULT NULL
  ) RETURNS TABLE (cityname text, ctype text, fid int, is_valid boolean, fname text, err_msg text) AS $wrap$
  SELECT cityname,ctype,fid,is_valid,fname, (fmeta->>'is_valid_err') AS err_msg
  FROM eclusa.cityfolder_input_files('/home/'||p_user)
  WHERE COALESCE( is_valid=text_to_boolean(p_is_valid), true)
  ORDER BY 1,2
$wrap$ language SQL immutable;
-- ver API.AddressForAll/eclusa/checkUserFiles-step1/{user}

CREATE or replace FUNCTION API.cityfolder_input_user(
    p_user text DEFAULT 'igor',
    p_is_valid text DEFAULT NULL
  ) RETURNS TABLE (cityname text, ctype text, fid int, is_valid boolean, fname text, hash text, err_msg text) AS $wrap$
  SELECT cityname,ctype,fid,is_valid,fname,(fmeta->>'hash') as hash, (fmeta->>'is_valid_err') AS err_msg
  FROM eclusa.cityfolder_input('/home/'||p_user)
  WHERE COALESCE( is_valid=text_to_boolean(p_is_valid), true)
  ORDER BY 1,2
$wrap$ language SQL immutable;
-- ver API.AddressForAll/eclusa/checkUserFiles-step2/{user}

-- certo é criar função que retorna JSON das tabelas acima e processa strings nos parametros
-- API.eclusa_checkUserFiles_step(step,user,flag)

----
----
