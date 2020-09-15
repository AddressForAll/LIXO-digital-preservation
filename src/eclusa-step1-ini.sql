
-- ECLUSA STEP1
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
s
CREATE EXTENSION IF NOT EXISTS file_fdw;
CREATE SERVER    IF NOT EXISTS files FOREIGN DATA WRAPPER file_fdw;

CREATE schema    IF NOT EXISTS api;
CREATE schema    IF NOT EXISTS ingest;
CREATE schema    IF NOT EXISTS optim;
CREATE schema    IF NOT EXISTS tmp_orig;

-- -- -- -- -- -- -- -- --
-- inicializações OPTIM:

CREATE TABLE IF NOT EXISTS optim.jurisdiction ( -- only current
  -- need a view vw01current_jurisdiction to avoid the lost of non-current.
  -- https://schema.org/AdministrativeArea or https://schema.org/jurisdiction ?
  -- OSM use AdminLevel, etc. but LexML uses Jurisdiction.
  osm_id bigint PRIMARY KEY, -- official or adapted geometry. AdministrativeArea.
  jurisd_base_id int NOT NULL,  -- ISO numeric COUNTRY ID or negative for non-iso (ex. oceans)
  -- ISO3166-1-numeric for Brazil is 076,
  jurisd_local_id int   NOT NULL, -- numeric official ID like IBGE_ID of BR jurisdiction.
  -- for example BR's ACRE is 12 and its cities are {1200013, 1200054,etc}.
  name    text  NOT NULL CHECK(length(name)<60), -- city name for admin-level3 (OSM level=?)
  parent_abbrev   text  NOT NULL CHECK(length(parent_abbrev)=2), -- state is admin-level2, country level1
  abbrev text  CHECK(length(abbrev)>=2 AND length(abbrev)<=5), -- ISO and other abbreviations
  wikidata_id  bigint,  --  from '^Q\d+'
  lexlabel     text NOT NULL,  -- cache from name. 'sao.paulo'
  isolabel_ext text NOT NULL,  -- cache from name and state. BR-SP-SaoPaulo
  ddd          integer, -- Direct distance dialing
  info JSONb -- postalCode_ranges, notes,   creation, extinction, etc.
  ,UNIQUE(jurisd_base_id,jurisd_local_id)
  ,UNIQUE(jurisd_base_id,parent_abbrev,name)
  ,UNIQUE(jurisd_base_id,parent_abbrev,lexlabel)
  ,UNIQUE(jurisd_base_id,parent_abbrev,lexlabel)
  ,UNIQUE(jurisd_base_id,parent_abbrev,abbrev)
);
CREATE TABLE optim.donor (
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
CREATE TABLE optim.donatedPack(
  pack_id int NOT NULL PRIMARY KEY,
  donor_id int NOT NULL REFERENCES optim.donor(id),
  accepted_date date,
  about text,
  config_commom jsonb, -- parte da config de ingestão comum a todos os arquivos (ver caso Sampa)
  info jsonb,
  UNIQUE(pack_id)
); -- pack é só intermediário de agregação

CREATE TABLE optim.origin_content_type(
  id int PRIMARY KEY,
  label text,
  model_geo text,      -- tipo de geometria e seus atributos
  model_septable text, -- atributos da geometria em tabela separada
  is_useful text,      -- valido se true
  score text           -- avaliação de preferência
  ,UNIQUE(label)
);
INSERT INTO optim.origin_content_type VALUES
  (1,'PL1','Point Lot via_name housenumber','',TRUE,'perfect'),
  (2,'PL1s','Point Lot id','id via_name housenumber',TRUE,'perfect'), -- separated
  (3,'P1','Point via_name housenumber','',TRUE,'perfect'),
  (4,'P2','Point housenumber','',TRUE,'good'),
  (5,'P1s','Point id ','id via_name housenumber',TRUE,'perfect'),    -- separated
  (6,'P2s','Point id','id housenumber',TRUE,'good'),
  (7,'P3e','Point','',FALSE,'bad'),                                  -- empty
  (8,'L1','Lot via_name housenumber','',TRUE,'perfect'),
  (9,'L2','Lot housenumber','',TRUE,'good'),
  (10,'L2s','Lot id','id housenumber',TRUE,'good'),
  (11,'L1s','Lot id','id via_name housenumber',TRUE,'perfect'),
  (12,'L3','Lot','',FALSE,'bad'),
  (13,'N1s','(null)','via_name housenumber region',FALSE,'bad'),
  (14,'V1','Via name','',TRUE,'perfect'),
  (15,'V1s','Via id','id name',TRUE,'good')
;

CREATE TABLE IF NOT EXISTS optim.origin(
   id serial           NOT NULL PRIMARY KEY,
   jurisd_osm_id int   NOT NULL REFERENCES optim.jurisdiction(osm_id), -- scope of data, desmembrando arquivos se possível.
   ctype text          NOT NULL REFERENCES optim.origin_content_type(label),  -- .. tipo de entrada que amarra com config!
   pack_id int         NOT NULL REFERENCES optim.donatedPack(pack_id), -- um ou mais origins no mesmo paxck.
   fhash text          NOT NULL, -- sha256 is a finger print
   fname text          NOT NULL,  -- filename
   fversion smallint   NOT NULL DEFAULT 1, -- fname version (counter for same old filename+ctype).
   -- PS: pack version or file intention version? Or only control over changes?...  ou versão relativa às conig.
   kx_cmds text[],  -- conforme config; uso posterior para guardar sequencia de comandos.
   is_valid boolean   NOT NULL DEFAULT false,
   is_open boolean    NOT NULL DEFAULT true,
   fmeta jsonb,  -- file metadata
   config jsonb, -- complementado por pack com (config||config_commom) AS config
   ingest_instant timestamp DEFAULT now()
   ,UNIQUE(fhash)
   ,UNIQUE(jurisd_osm_id,fname,fversion,ctype) -- ,kx_ingest_date=ingest_instant::date
);

CREATE or replace FUNCTION ingest.pg_attribute_dump_text(p_tabname text) RETURNS text[]  AS $f$
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


-- -- --
CREATE or replace FUNCTION ingest.fdw_generate_getclone(
  -- foreign-data wrapper generator
  p_tablename text,
  p_jurisd_abbrev text DEFAULT 'br',
  p_schemaname text DEFAULT 'optim',
  p_path text DEFAULT NULL  -- default based on ids
) RETURNS text  AS $f$
DECLARE
 fdwname text;
 fpath text;
 f text;
BEGIN
 fdwname := 'tmp_orig.fdw_'|| p_tablename ||'_'|| p_jurisd_abbrev;
 -- poderia otimizar por chamada (alter table option filename), porém não é paralelizável.
 fpath := COALESCE(p_path,'/tmp/pg_io');
 f := concat(fpath,'/',p_tablename,'-',p_jurisd_abbrev,'.csv');
 EXECUTE
    format(
      'DROP FOREIGN TABLE IF EXISTS %s; CREATE FOREIGN TABLE %s (%s)',
       fdwname, array_to_string(ingest.pg_attribute_dump_text(p_schemaname||'.'||p_tablename),',')
     ) || format(
       'SERVER files OPTIONS (filename %L, format %L, header %L, delimiter %L)',
       f, 'csv', 'true', ','
    );
    return ' '|| fdwname || E' was created!\n source: '||f|| ' ';
END;
$f$ language PLpgSQL;

-- -- -- -- -- -- -- -- --
-- inicializações INGEST:

CREATE VIEW ingest.vw01_origin AS
  SELECT o.*,
         c.name as city_name,          c.parent_abbrev as city_state,
         c.abbrev AS city_abbrev3,    c.isolabel_ext AS city_isolabel_ext,
         d.vat_id AS donor_vat_id,     d.shortname AS donor_shortname,
         d.legalName AS donor_legalName, d.url AS donor_url,
         p.accepted_date,              p.config_commom
  FROM (optim.origin o
       INNER JOIN optim.jurisdiction c ON o.jurisd_osm_id=c.osm_id
       LEFT JOIN optim.donatedPack p ON o.pack_id=p.pack_id
     ) LEFT JOIN optim.donor d ON p.donor_id = d.id
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
      SELECT cf.*, y.jurisd_local_id, fmeta->>'fpath' AS fpath
      FROM eclusa.cityfolder_input_files(p_fpath) cf
           LEFT JOIN optim.jurisdiction y ON cf.cityname=y.isolabel_ext
   )
   SELECT t.fid, t.cityname, t.fname, t.ctype,
          t.is_valid AND k2.hash is not null AS is_valid,
          CASE WHEN t.jurisd_local_id IS NULL THEN t.fmeta    || jsonb_build_object('is_valid_err', '#ER02: cityname unknown')
               WHEN k2.hash is not null THEN  t.fmeta || jsonb_build_object('jurisd_local_id',t.jurisd_local_id, 'hash',k2.hash, 'hashtype', k2.hashtype)
               ELSE t.fmeta || jsonb_build_object('jurisd_local_id',t.jurisd_local_id, 'is_valid_err', COALESCE(t.fmeta->>'is_valid_err','')||'#ER03: hash not generated; ')  END
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
  -- idempotente no estado da base: chamamos a função n vezes com os mesmos parâmetros e o resultado é sempre o mesmo.

  INSERT INTO optim.origin(jurisd_osm_id,fhash, fname,ctype,is_valid, fmeta, config)
   SELECT j.osm_id, e.fmeta->>'hash',
          e.fname, e.ctype, e.is_valid,
          (e.fmeta - 'hash') || jsonb_build_object( 'user_resp', regexp_match(p_in_path,'^/home/([^/]+)') ),
          jsonb_build_object('staging_db',t1.datname)  -- teste
   FROM eclusa.cityfolder_input( rtrim(p_in_path,'/') ) e
       INNER JOIN optim.jurisdiction j ON j.isolabel_ext=e.cityname,
        (SELECT COALESCE( (SELECT datname FROM pg_stat_activity ORDER BY 1 LIMIT 1), NULL) ) t1(datname)
   WHERE is_valid
  ON CONFLICT DO NOTHING;
  -- Comandos de uso geral:
  UPDATE optim.origin SET kx_cmds = array[
             concat('mkdir -p ', rtrim(p_eclusa_path,'/'), '/orig', id),
             concat('cd ', rtrim(p_eclusa_path,'/'), '/orig', id),
             CASE WHEN fmeta->>'ext'='zip' THEN concat('unzip -j ',fmeta->>'fpath','/',fname) ELSE '' END
          ]
  WHERE is_open AND is_valid
        AND kx_cmds IS NULL -- novos
  ;
  -- Comandos p_especifico='shp_sampa1':
  UPDATE optim.origin SET kx_cmds = kx_cmds ||  concat(
       -- SRID 31983 precisa estar nos metadados.
       'shp2pgsql -s 31983 ', regexp_replace(fname,'\.([^\.\/]+)$',''),'.shp tmp_orig.t',id,'_01 | psql '|| p_db
     )
  WHERE p_especifico='shp_sampa1' AND is_open AND is_valid
        AND array_length(kx_cmds,1)=3 -- novos
        AND ctype='lotes'          -- só neste caso
  ;
  SELECT '... insert/tentativa realizado, comandos: '
     ||E'\n* '|| (SELECT COUNT(*) FROM optim.origin WHERE is_open AND is_valid) ||' origens em aberto.'
     ||E'\n* '|| (SELECT COUNT(*) FROM optim.origin WHERE is_open AND NOT(is_valid)) ||E' origens com defeito.\n'
$f$ language SQL VOLATILE;


CREATE or replace FUNCTION ingest.cityfolder_generate_views_tpl1(
  p_vwnane text DEFAULT 'vw0_union1'
) RETURNS text  AS $f$
BEGIN
 EXECUTE
    'CREATE or replace VIEW  tmp_orig.'|| p_vwnane ||' AS '
    || (
      SELECT string_agg( 'SELECT '|| id ||' gid_prefix, * FROM tmp_orig.t'||id||'_01 ', E'\n  UNION \n' )
      FROM optim.origin WHERE is_open AND is_valid AND ctype='lotes' AND array_length(kx_cmds,1)=4
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
    -- .. FROM optim.origin WHERE is_open AND is_valid AND ctype='lotes' AND array_length(kx_cmds,1)=4
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
    SELECT E'\n\n# orig'|| id ||E'\n'|| array_to_string(kx_cmds,E'\n')
    FROM optim.origin WHERE is_open AND is_valid
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

\echo E'\n --- FDW para ingestão de dados do git ---'
PREPARE fdw_gen(text) AS SELECT ingest.fdw_generate_getclone($1, 'br', 'optim');
EXECUTE fdw_gen('jurisdiction');  -- cria tmp_orig.fdw_jurisdiction_br
EXECUTE fdw_gen('donor');         -- cria tmp_orig.fdw_donor_br
EXECUTE fdw_gen('donatedPack');   --  ...
EXECUTE fdw_gen('origin_content_type');
EXECUTE fdw_gen('origin');
