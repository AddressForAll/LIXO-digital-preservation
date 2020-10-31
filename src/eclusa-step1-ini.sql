-- -- --
-- OPTIM E ECLUSA STEP1  (depends on optim-step1)
--
-- Eclusa de dados: trata os dados do upload e os transfere para a ingestão.
-- (Data Canal-lock)
-- O dado só entra depois que sha256 é confirmado fora (upload correto) e dentro da base (nao-duplicação)
-- A ficha completa de metadados é gerada pela eclusa: contagens no filesystem e no PostGIS.
-- PS: ainda assim o relatório de relevância, indicando se trouche informação nova e aparentemente consistente, só vem depois.
--
-- Efetua o scan, validação, geração de comandos e ingestão de dados já padronizados.
--

-- -- -- -- -- -- -- -- --
-- inicializações ECLUSA:

CREATE SCHEMA IF NOT EXISTS eclusa; -- módulo complementar do Schema ingest.


CREATE or replace FUNCTION eclusa.cityfolder_input_packdir(
  p_user text, -- e.g. 'igor'
  p_fpath text DEFAULT '/home'
) RETURNS TABLE (LIKE api.ttpl_eclusa01_packdir) AS $f$
  WITH
  t0 AS (
    SELECT username, CASE -- define base path conforme existência ou não da pasta da eclusa:
      WHEN (pg_stat_file(p0||'/_eclusa',true)).isdir is null THEN p0
      ELSE p0 ||'/_eclusa'
      END AS fpath
    FROM (SELECT rtrim(p_fpath,'/')||'/'||lower(p_user), lower(p_user)) t0pre(p0,username)
  )
  , t1 AS ( -- obriga subdivisã por jurisdições na eclusa:
    SELECT t0.username, j.isolabel_ext as jurisdiction_label, j.osm_id as jurisdiction_osmid,
           t0.fpath ||'/'|| t.f as jurisdiction_path
    FROM t0, pg_ls_dir( (SELECT fpath FROM t0) ) t(f)
	         INNER JOIN optim.jurisdiction j ON t.f=j.isolabel_ext
  )
  SELECT username, jurisdiction_label,jurisdiction_osmid,
	       jurisdiction_path||'/'||pack_fname as pack_path,
	       replace(pack_fname,'_pk','')::int as pack_id, NULL::jsonb
  FROM (  -- t2a:
      SELECT *, pg_ls_dir(jurisdiction_path) as pack_fname
      FROM t1 ORDER BY 1,3
  ) t2a
	WHERE pack_fname ~ '^_pk\d+'
$f$ language SQL immutable;

CREATE or replace FUNCTION eclusa.cityfolder_input_files(
  p_user text, --  e.g. 'igor',
  p_excludefiles text[] DEFAULT array['sha256sum.txt','README.md','README.txt']
) RETURNS TABLE (LIKE api.ttpl_eclusa02_cityfile1) AS $f$
  -- falta conferir se user é válido e se pack foi registrado.
  WITH
  tres_pack AS (
    SELECT pack_id, fname, 'std' AS ctype,
           jsonb_build_object(
             'fpath',pack_path,
             'username',username,
             'jurisdiction_label',jurisdiction_label,
             'jurisdiction_osmid',jurisdiction_osmid,
             'pack_id',pack_id
           ) || to_jsonb( pg_stat_file(pack_path||'/'||fname) ) AS fmeta
           -- or sonb_build_object AS packinfo for:
            -- 'donor_id',p.donor_id, 'user_resp',p.user_resp, 'accepted_date',p.accepted_date
    FROM (  -- t1:
      SELECT *, pg_ls_dir(pack_path) as fname -- dir or file
      FROM eclusa.cityfolder_input_packdir(p_user) t2  -- need user_resp! use API.uridisp_eclusa_checkuserdir()
      -- INNER JOIN optim.donatedPack p ON p.pack_id=t2.pack_id
      ORDER BY jurisdiction_label, fname -- t2.pack_id
    ) t1
  ),
  tres2 AS ( -- main query:
    SELECT pack_id, ctype, (row_number() OVER ())::int id,
           fname,
           fname ~* '\.(zip|gz|rar|geojson|json|csv|dwg|pdf|pbf|kmz)$' AS is_validext,
           fmeta || jsonb_build_object( 'ext', (regexp_match(fname,'\.([^/\.]+)$'))[1]  ) AS fmeta
    FROM ( -- t3:
      SELECT pack_id, ctype, fname, fmeta FROM tres_pack WHERE NOT((fmeta->'isdir')::boolean)
      UNION
      SELECT pack_id, ctype, fname,
             fmeta_pack || to_jsonb( pg_stat_file(fpath||'/'||fname) )
             || jsonb_build_object('fpath',fpath) -- fmeta
      FROM ( -- t4:
        SELECT pack_id, fname as ctype,
               (fmeta->>'fpath')||'/'|| fname AS fpath,
               pg_ls_dir((fmeta->>'fpath')||'/'||fname) AS fname,
               fmeta as fmeta_pack
        FROM tres_pack
        WHERE (fmeta->'isdir')::boolean
      ) t4
    ) t3
    WHERE NOT( fname=ANY(p_excludefiles) )
  ) -- \tres2
  SELECT pack_id, ctype, id, fname, is_validext,
         fmeta || CASE
          WHEN is_validext THEN '{}'::jsonb
          ELSE jsonb_build_object('is_valid_err','#ER01: file extension unknown; ')
          END AS fmeta
  FROM tres2
  ORDER BY 1,2,4
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
  p_user text, --  e.g. 'igor'
  checksum_file text DEFAULT 'sha256sum.txt'
) RETURNS TABLE (LIKE api.ttpl_eclusa02_cityfile1) AS $f$
   WITH t AS (
      SELECT *, fmeta->>'fpath' AS fpath,
             u.username IS NULL OR  u.username!=pack_user! ???
      FROM eclusa.cityfolder_input_files(p_user) --  pack_id,ctype,fid,fname, is_valid, fmeta
           --LEFT JOIN optim.jurisdiction y ON (cf.cmeta->'jurisdiction_osmid')::bigint=y.osm_id
           LEFT JOIN optim.auth_user u ON lower(p_user)=u.username
   )
   SELECT t.pack_id, t.ctype, t.fid, t.fname,
        t.is_valid AND k2.hash is not null AND u.username IS NOT NULL AND  AS is_valid,
        CASE -- WHEN t.jurisd_local_id is null THEN t.fmeta || jsonb_build_object('is_valid_err', '#ER02: cityname unknown')
           WHEN u.username IS NULL THEN t.fmeta || jsonb_build_object('is_valid_err', COALESCE(t.fmeta->>'is_valid_err','')||'#ER03: user not authorized or not select for this pack; ')
           WHEN k2.hash is not null THEN  t.fmeta || jsonb_build_object('hash',k2.hash, 'hashtype', k2.hashtype)
           ELSE t.fmeta || jsonb_build_object('is_valid_err', COALESCE(t.fmeta->>'is_valid_err','')||'#ER04: hash not generated; ')
        END
   FROM t LEFT JOIN (
           SELECT k.* -- hash, hashtype, file, refpath
           FROM (SELECT DISTINCT fmeta->>'fpath' as fpath FROM t) t2,
                LATERAL eclusa.read_hashsum( t2.fpath||'/'||checksum_file) k
           WHERE t2.fpath=k.refpath -- and k.refpath is not null
   ) k2
   ON k2.refpath = t.fpath AND t.fname=k2.file
   WHERE t.fname!=checksum_file -- reserved name
   ORDER BY t.pack_id, t.fname
$f$ language SQL immutable;

-- -- -- -- --
-- Carga da origem e geração de comandos para a ingestão final:

CREATE or replace FUNCTION eclusa.cityfolder_insert(
  p_user     text, -- e.g. 'igor'
  p_eclusa_path text DEFAULT '/tmp/pg_io/eclusa',
  p_db_ingest   text DEFAULT 'ingest1', -- ou tmplixo
  p_especifico  text DEFAULT ''
) RETURNS text AS $f$
  -- idempotente no estado da base: chamamos a função n vezes com os mesmos parâmetros e o resultado é sempre o mesmo.
  -- primeiro rodar make ecl_run
  INSERT INTO optim.origin(jurisd_osm_id,pack_id,fhash, fname,ctype,is_valid, is_open, fmeta, config)
   -- ignores fversion,kx_cmds
   SELECT (e.fmeta->'jurisdiction_osmid')::bigint, e.pack_id, e.fmeta->>'hash',
          e.fname, e.ctype, e.is_valid, true,
          (e.fmeta - 'hash' - 'jurisdiction_osmid'),
          jsonb_build_object('staging_db',t1.datname)  -- teste
   FROM eclusa.cityfolder_input(p_user) e,
        (SELECT COALESCE( (SELECT datname FROM pg_stat_activity ORDER BY 1 LIMIT 1), NULL) ) t1(datname)
   WHERE is_valid
  ON CONFLICT DO NOTHING;
  -- Comandos de uso geral:
  UPDATE optim.origin SET kx_cmds = array[
             concat('mkdir -p ', rtrim(p_eclusa_path,'/'), '/orig', id),
             concat('cd ', rtrim(p_eclusa_path,'/'), '/orig', id),
             CASE WHEN fmeta->>'ext'='zip' THEN concat('unzip -j ',fmeta->>'fpath','/',fname) ELSE '' END
          ]
  WHERE is_open AND is_valid AND kx_cmds IS NULL -- novos
  ;
  -- Comandos p_especifico='shp_sampa1':
  UPDATE optim.origin SET kx_cmds = kx_cmds ||  concat(
       -- SRID 31983 precisa estar nos metadados.
       'shp2pgsql -s 31983 ', regexp_replace(fname,'\.([^\.\/]+)$',''),'.shp tmp_orig.t',id,'_01 | psql '|| p_db_ingest
     )
  WHERE p_especifico='shp_sampa1' AND is_open AND is_valid
        AND array_length(kx_cmds,1)=3 -- novos
        AND ctype='lotes'          -- só neste caso
  ;
  SELECT '... insert/tentativa realizado, comandos: '
     ||E'\n* '|| (SELECT COUNT(*) FROM optim.origin WHERE is_open AND is_valid) ||' origens em aberto.'
     ||E'\n* '|| (SELECT COUNT(*) FROM optim.origin WHERE is_open AND NOT(is_valid)) ||E' origens com defeito.\n'
$f$ language SQL VOLATILE;

CREATE VIEW eclusa.vw03alldft_cityfolder_ins AS
  SELECT string_agg(eclusa.cityfolder_insert(username),E'\n')
  FROM eclusa.vw01_cityfolder_validUsers
;

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

CREATE or replace FUNCTION ingest.cityfolder_cmds_to_run(
  -- REVISAR! em uso?
  -- mudar para esquema eclusa?
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

----- Shell-script generators:

CREATE or replace FUNCTION eclusa.cityfolder_validUsers(
  p_path text DEFAULT '/home'
) RETURNS TABLE (LIKE api.ttpl_general01_namecheck) AS $f$
  WITH t AS (
    SELECT t.dirname, a.users, t.dirname=ANY(a.users) AS is_valid,
           rtrim(p_path,'/')||'/'||t.dirname as upath
    FROM pg_ls_dir(p_path) t(dirname),
         (SELECT array_agg(username) users FROM optim.auth_user) a
  )
   SELECT true,  dirname, '' FROM t WHERE is_valid
   UNION
   SELECT false, dirname, 'no auth_user for filesys_user '||upath FROM t WHERE NOT(is_valid)
   UNION
   ( SELECT false, u.username, 'no filesys_user for auth_user '||u.username
     FROM optim.auth_user u, (SELECT array_agg(dirname) dnames FROM t) d
     WHERE NOT(username=ANY(dnames))
   )
   ORDER BY 1 DESC,2
$f$ language SQL immutable;

CREATE VIEW eclusa.vw01_cityfolder_validUsers AS
  SELECT name as username FROM eclusa.cityfolder_validUsers() WHERE is_valid
; -- list all default valid users

CREATE or replace FUNCTION eclusa.cityfolder_runhashes(
  p_user text, -- ex. igor
  p_output_shfile text DEFAULT '/tmp/pg_io/runHashes',
  p_path text DEFAULT '/home'
) RETURNS text AS $f$
  WITH
  t0 AS (SELECT p_output_shfile ||'-'|| p_user ||'.sh' AS sh_file),
  t1 AS (SELECT *, pg_catalog.pg_file_unlink(sh_file) as rm_ret FROM t0)
   SELECT
    MAX('Arquivo anterior '||CASE WHEN t1.rm_ret THEN 'removido' ELSE 'ausente' END)
    ||E'\nGravados '
    || pg_catalog.pg_file_write(
      MAX(sh_file),
      string_agg( cmd, E'\n' ),
      false
     )::text ||' bytes em '|| MAX(sh_file) ||E' \n' as fim
   FROM t1, (
     SELECT distinct concat(
        'cd ', fmeta->>'fpath', '; sha256sum -b *.* > sha256sum.txt; chmod 666 sha256sum.txt'
      ) as cmd
     FROM eclusa.cityfolder_input_files(p_user) -- não mais p_path||'/'||
     ORDER BY 1
   ) t
$f$ language SQL immutable;

CREATE VIEW eclusa.vw01alldft_cityfolder_runhashes AS
  SELECT string_agg(eclusa.cityfolder_runhashes(username),E'\n')
  FROM eclusa.vw01_cityfolder_validUsers
; -- execute all as default.

CREATE or replace FUNCTION eclusa.cityfolder_run_cpfiles(
  p_user          text, -- ex. igor
  p_output_shfile text DEFAULT '/tmp/pg_io/runCpFiles',
  p_path          text DEFAULT '/home',
  p_target_path   text DEFAULT '/var/www/preserv.addressforall.org/download/'
) RETURNS text AS $f$
  WITH
  t0 AS (SELECT p_output_shfile ||'-'|| p_user ||'.sh' AS sh_file),
  t1 AS (SELECT *, pg_catalog.pg_file_unlink(sh_file) as rm_ret FROM t0),
  t2 AS (
   SELECT
    MAX('Arquivo anterior '||CASE WHEN t1.rm_ret THEN 'removido' ELSE 'ausente' END)
    ||E'\nGravados '
    || pg_catalog.pg_file_write(
      MAX(sh_file),
      string_agg( cmd, E'\n' ),
      false
     )::text ||' bytes em '|| MAX(sh_file) ||E' \n' as fim
   FROM t1, (
     SELECT DISTINCT concat(
         'cp "', fmeta->>'fpath', '/', fname, '" ',
         p_target_path, fmeta->>'hash','.', fmeta->>'ext'
       ) AS cmd
     FROM eclusa.cityfolder_input(p_user)
     WHERE is_valid AND fmeta->>'hash' NOT IN ( -- exclude when exists
       SELECT fhash FROM optim.origin
     ) -- FALTA conferir se foi mesmo copiado nos backups de hashes.
     ORDER BY 1
   ) t
 ) -- \t2
 SELECT COALESCE( t2.fim, volat_file_write('Nada a copiar com '||t0.sh_file, t0.sh_file, '# vazio',false) )
 FROM t0 LEFT JOIN t2 ON true
$f$ language SQL immutable;

CREATE or replace VIEW eclusa.vw01alldft_cityfolder_run_cpfiles AS   -- mudar vw02all!
  SELECT string_agg(eclusa.cityfolder_run_cpfiles(username),E'\n')
  FROM eclusa.vw01_cityfolder_validUsers
; -- execute all as default.

-- -- -- --
-- API exposing of filesystem or shell-script results

-- CREATE or replace FUNCTION eclusa.cityfolder_input_files_user(
-- p_user text DEFAULT 'igor',
-- p_is_valid text DEFAULT NULL
-- RETURNS TABLE (LIKE api.ttpl_eclusa02_cityfile1) AS $f$
--  IS 'Lists all user-eclusa valid files in Step1. See endpoint /eclusa/checkUserFiles-step1/{user}.'
/*
lixo ver API.uri_dispatch_tab_eclusa1
CREATE or replace FUNCTION API.cityfolder_input_user(
    p_user text DEFAULT 'igor',
    p_is_valid text DEFAULT NULL
  ) RETURNS TABLE (LIKE api.ttpl_eclusa02_cityfile1) AS $f$
  SELECT cityname,ctype,fid,is_valid,fname,fmeta
         -- see fmeta->>'hash' and  fmeta->>'is_valid_err'
  FROM eclusa.cityfolder_input('/home/'||p_user)
  WHERE COALESCE( is_valid=text_to_boolean(p_is_valid), true)
  ORDER BY 1,2
$f$ language SQL immutable;
COMMENT ON FUNCTION api.cityfolder_input_files_user
  IS 'Lists all user-eclusa valid files in Step2. See endpoint /eclusa/checkUserFiles-step2/{user}.'
;
*/

-- -- -- -- -- -- -- --
-- API Eclusa dispatchers:

-- revisar propagacao do usuario para validar no final!
-- Ou decidir ja transmitir erro por aqui! nao pode criar pack se nao for o dono designado!

CREATE or replace FUNCTION API.uridisp_eclusa_checkuserdir(
    p_uri text DEFAULT '',
    p_args text DEFAULT NULL
) RETURNS TABLE (LIKE api.ttpl_eclusa01_packdir) AS $f$
    SELECT t2.username, t2.jurisdiction_label, t2.jurisdiction_osmid,
           t2.pack_path, t2.pack_id, jsonb_build_object(
      'donor_id',p.donor_id, 'user_resp',p.user_resp, 'accepted_date',p.accepted_date
    ) as packinfo
    FROM API.uri_dispatch_parser(p_uri) t1(p), -- rev ,'{eclusa,checkuserfiles_step1}'
         LATERAL eclusa.cityfolder_input_packdir(t1.p[1]) t2 -- username
         INNER JOIN optim.donatedPack p ON p.pack_id=t2.pack_id
    ORDER BY pack_id
$f$ language SQL immutable;
COMMENT ON FUNCTION API.uridisp_eclusa_checkuserdir
  IS 'List the user dir packages. A uri_dispatcher that runs eclusa.cityfolder_input_packdir() returning ttpl_eclusa01_packdir.'
;

-- http://api-test.addressforall.org/v1/eclusa/checkuserfiles_step1/igor
CREATE or replace FUNCTION API.uridisp_eclusa_checkuserfiles_step1(
    p_uri text DEFAULT '', -- /eclusa/checkUserFiles-step1/{user}/{is_valid?}
    p_args text DEFAULT NULL
) RETURNS TABLE (LIKE api.ttpl_eclusa02_cityfile1) AS $f$
        SELECT pack_id, ctype, fid, fname, is_valid, fmeta
        FROM API.uri_dispatch_parser(p_uri) t1(p), -- rev ,'{eclusa,checkuserfiles_step1}'
             LATERAL eclusa.cityfolder_input_files(t1.p[1]) t2 -- nao mais '/home/'||
        WHERE  COALESCE( t2.is_valid=text_to_boolean(t1.p[2]), true)
        ORDER BY 1,2
$f$ language SQL immutable;
COMMENT ON FUNCTION API.uridisp_eclusa_checkuserfiles_step1
  IS 'A uri_dispatcher that runs eclusa.cityfolder_input_files() returning ttpl_eclusa01_cityfile1.'
;

CREATE or replace FUNCTION API.uridisp_eclusa_checkuserfiles_step2(
    p_uri text DEFAULT '', -- /eclusa/checkuserFiles_step2/{user}/{is_valid?}
    p_args text DEFAULT NULL
) RETURNS TABLE (LIKE api.ttpl_eclusa02_cityfile1) AS $f$
        SELECT pack_id, ctype, fid, fname, is_valid, fmeta
        FROM API.uri_dispatch_parser(p_uri) t1(p), -- rev ,'{eclusa,checkuserfiles_step2}'
             LATERAL eclusa.cityfolder_input(t1.p[1]) t2
        WHERE  COALESCE( t2.is_valid=text_to_boolean(t1.p[2]), true)
        ORDER BY 1,2
$f$ language SQL immutable;
COMMENT ON FUNCTION API.uridisp_eclusa_checkuserfiles_step2
  IS 'A uri_dispatcher that runs eclusa.cityfolder_input() returning ttpl_eclusa01_cityfile1.'
;
