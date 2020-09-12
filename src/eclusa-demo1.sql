
-- caso sao paulo como exemplo
/*
CREATE table addesses_sp_spa AS -- depois de tudo....
  SELECT u.*, e.streetname, e.housenum,
         CASE WHEN trim(e.cep)>'' and e.cep!='-' THEN replace(trim(e.cep),'-','')::int else NULL END as cep
  FROM tmp_orig.mvw0_union3_idx u INNER JOIN tmp_orig.vw1159_01a_enxuto e
    ON NOT(u.is_dup) AND u.obj_id=e.obj_id AND u.cond_num=e.cond_num
; -- 1586786

*/
-------------------------------------------
-- EXEMPLO DE CARGA E PREPARO DOS COMANDOS:
-- DELETE FROM ingest.origin;
SELECT ingest.cityfolder_insert('/home/igor', '/tmp/pg_io/eclusa', 'tmplixo', 'shp_sampa1');
-- idempotente

UPDATE ingest.origin
SET cmds = array_cat(cmds, ARRAY['recode WINDOWS-1252..UTF-8 IPTU_2020.csv', 'dos2unix IPTU_2020.csv'])
WHERE fhash='99d4b808f96562bba371188209618c1fde319dc499780a48256c2037e8c8d1f9'
      AND is_open AND is_valid AND array_length(cmds,1)=3
; -- idempotente

SELECT ingest.cityfolder_cmds_to_run('tmplixo'); -- mesma base e cria view
  -- cria tmp_orig.vw0_union1

CREATE FOREIGN TABLE tmp_orig.fdw1159_01(
 "NUMERO DO CONTRIBUINTE" text,
 "ANO DO EXERCICIO" text,
 "NUMERO DA NL" text,
 "DATA DO CADASTRAMENTO" text,
 "TIPO DE CONTRIBUINTE 1" text,
 "CPF/CNPJ DO CONTRIBUINTE 1" text,
 "NOME DO CONTRIBUINTE 1" text,
 "TIPO DE CONTRIBUINTE 2" text,
 "CPF/CNPJ DO CONTRIBUINTE 2" text,
 "NOME DO CONTRIBUINTE 2" text,
 "NUMERO DO CONDOMINIO" text,
 "CODLOG DO IMOVEL" text,
 "NOME DE LOGRADOURO DO IMOVEL" text,
 "NUMERO DO IMOVEL" text,
 "COMPLEMENTO DO IMOVEL" text,
 "BAIRRO DO IMOVEL" text,
 "REFERENCIA DO IMOVEL" text,
 "CEP DO IMOVEL" text,
 "QUANTIDADE DE ESQUINAS/FRENTES" text,
 "FRACAO IDEAL" text,
 "AREA DO TERRENO" text,
 "AREA CONSTRUIDA" text,
 "AREA OCUPADA" text,
 "VALOR DO M2 DO TERRENO" text,
 "VALOR DO M2 DE CONSTRUCAO" text,
 "ANO DA CONSTRUCAO CORRIGIDO" text,
 "QUANTIDADE DE PAVIMENTOS" text,
 "TESTADA PARA CALCULO" text,
 "TIPO DE USO DO IMOVEL" text,
 "TIPO DE PADRAO DA CONSTRUCAO" text,
 "TIPO DE TERRENO" text,
 "FATOR DE OBSOLESCENCIA" text,
 "ANO DE INICIO DA VIDA DO CONTRIBUINTE" text,
 "MES DE INICIO DA VIDA DO CONTRIBUINTE" text,
 "FASE DO CONTRIBUINTE" text
) SERVER files OPTIONS (
        filename '/tmp/pg_io/eclusa/orig1159/IPTU_2020.csv',
        format 'csv',
        header 'true',
        delimiter ';'
); -- ID = (SELECT id from ingest.origin WHERE fname='IPTU_2020.zip')
-- amarrar origem com city_id

/*
SELECT  "TIPO DE TERRENO", COUNT(*) n
FROM tmp_orig.fdw1159_01 group by 1 order by 2 desc;
 TIPO DE TERRENO  |    n
------------------+---------
 Normal           | 2345803
 De esquina       |  685534
 De duas ou mais  |  417672
 Terreno interno  |   23761
 Lote de esquina  |   13499
 Lote de fundos   |    9051
 Lote encravado   |    3324
*/
-- teste de volumetria:
SELECT count(*) from tmp_orig.fdw1159_01; -- 3498644

---------

CREATE TABLE tmp_orig.t1159_01a AS
  -- t1159_01, t1159_01a, t1159_01b conforme transformações.
  SELECT DISTINCT num_imovel,
         CASE WHEN cond_num='00' THEN obj_id_item ELSE substr(obj_id_item,1,6)||'0000' END::bigint obj_id,
         obj_id_item, digito, streetname, housenum, cep, cond_num, false AS is_dup
  FROM (
    SELECT "NUMERO DO IMOVEL" num_imovel,
       numcont[1] obj_id_item,
       numcont[2] digito,
       "NOME DE LOGRADOURO DO IMOVEL" as streetname,
       "NUMERO DO IMOVEL" as housenum,
       "CEP DO IMOVEL" as cep,
       substr("NUMERO DO CONDOMINIO",1,2) as cond_num -- sem digito verificador
       -- , "BAIRRO DO IMOVEL" --col [15]
    FROM (
      SELECT *, regexp_match("NUMERO DO CONTRIBUINTE", '^(\d+)(?:\-(\d+))?$') as numcont
      FROM tmp_orig.fdw1159_01
    ) t
  ) tt
  ORDER BY 1,2,3,4
; -- 3498644
CREATE INDEX t1159_01a_obj_id_idx ON tmp_orig.t1159_01a(obj_id);

WITH dup AS (
  SELECT obj_id, cond_num
         ,count(*) n
         ,array_agg(obj_id_item) as dup_obj_id_items
  FROM tmp_orig.t1159_01a
  GROUP BY 1,2
  HAVING count(*)>1
) UPDATE tmp_orig.t1159_01a
  SET is_dup=true
  WHERE obj_id_item IN (SELECT DISTINCT unnest(dup_obj_id_items) i FROM dup)
; -- 1871857 rows; sobram 3498644-1871857=1626787

CREATE VIEW tmp_orig.vw1159_01a_enxuto AS
 SELECT DISTINCT obj_id, cond_num, streetname, housenum, cep
 FROM tmp_orig.t1159_01a
 WHERE NOT(is_dup)
; --  1626787 in SELECT COUNT(*) FROM tmp_orig.vw1159_01a_enxuto;

CREATE or replace VIEW tmp_orig.vw0_union2 AS
  SELECT (gid_prefix-pmin)::bigint + gid::bigint*(1+pmax-pmin)::bigint AS gid,
         gid_prefix AS origin_id, gid AS old_gid,
         (lo_setor || lo_quadra || lo_lote)::bigint AS obj_id,
         lo_condomi AS cond_num,
         lo_tp_quad || lo_tp_lote AS tipo,
         geom
  FROM tmp_orig.vw0_union1 t,
       LATERAL (select max(gid_prefix) as pmax, min(gid_prefix) as pmin from tmp_orig.vw0_union1) ag
;

SELECT count(*), count(distinct gid),
       count(distinct obj_id||'.'||cond_num ),
       count(distinct obj_id||'.'||cond_num||'.'||tipo)
FROM tmp_orig.vw0_union2; -- 1665805 | 1665805 | 1655689 | 1665707
-- duplocados portanto 1665805-1655689=10116, mas envolvidos na duplicação seriam mais.

CREATE TABLE tmp_orig.mvw0_union3_idx AS
  -- old MATERIALIZED VIEW
  SELECT gid, obj_id, cond_num, tipo
       ,false AS is_dup
       ,origin_id, old_gid
       ,st_area(geom) as area_srid31983
       ,st_transform(geom,4326) AS geom
  FROM tmp_orig.vw0_union2 t -- vw1159_01a
  ORDER BY 1,2,3
;
CREATE INDEX tmp_orig_mvw0_union2_obj_idx ON tmp_orig.mvw0_union3_idx(obj_id);

WITH dup AS (
  SELECT obj_id, cond_num
         ,count(*) n
         ,array_agg(gid) as dup_gids
  FROM tmp_orig.mvw0_union3_idx
  GROUP BY 1,2
  HAVING count(*)>1
) UPDATE tmp_orig.mvw0_union3_idx
  SET is_dup=true
  WHERE gid IN (SELECT DISTINCT unnest(dup_gids) AS dup_gid FROM dup)
; -- 19833 rows
WITH dup AS (
  SELECT geom,count(*) n,array_agg(gid) as dup_gids
  FROM tmp_orig.mvw0_union3_idx
  GROUP BY 1
  HAVING count(*)>1
) UPDATE tmp_orig.mvw0_union3_idx
  SET is_dup=true
  WHERE NOT(is_dup) AND gid IN (SELECT DISTINCT unnest(dup_gids) AS dup_gid FROM dup)
; -- 22 rows porém já is_dup


CREATE table addesses_sp_spa AS
  SELECT u.*, e.streetname, e.housenum,
         CASE WHEN e.cep>'' THEN replace(e.cep,'-','0')::int else NULL END as cep
  FROM tmp_orig.mvw0_union3_idx u INNER JOIN tmp_orig.vw1159_01a_enxuto e
    ON NOT(u.is_dup) AND u.obj_id=e.obj_id AND u.cond_num=e.cond_num
; -- 1586786


SELECT COUNT(*) n,
       COUNT(distinct geom) n_geometrias,
       COUNT(distinct lo_setor || lo_quadra || lo_lote) n_obj_ids, -- as str
       COUNT(distinct lo_setor || lo_quadra || lo_lote ||'.'||lo_condomi) n_obj_ids_condo
FROM tmp_orig.vw0_union1
;
--   n     | n_geometrias | n_obj_ids | n_obj_ids_condo
--  -------+--------------+-----------+-----------------
-- 1665805 |      1665794 |   1634327 |         1655689

SELECT COUNT(*) n,
       COUNT(distinct obj_id) n_obj_ids, -- as bigint
       COUNT(distinct obj_id::text ||'.'||cond_num) n_obj_ids_condo
FROM tmp_orig.mvw0_union3_idx -- effect of distinct?
;
--     n    | n_obj_ids | n_obj_ids_condo
--  -------+-----------+-----------------
-- 1665794 |   1634327 |         1655689

-- REPORT: join da geometria com o cadastro de IPTU
SELECT count(*) n, -- left
       count(c.obj_id) as n_join,
       count(distinct g.obj_id) n_obj_dist,  -- left
       count(distinct c.obj_id) n_obj_join_dist,
       count(distinct g.obj_id::text||g.cond_num) n_objcd_dist, -- left
       count(distinct c.obj_id::text||c.cond_num) n_objcd_join_dist
FROM tmp_orig.mvw0_union3_idx  g LEFT JOIN tmp_orig.vw1159_01a_enxuto c
    ON c.obj_id =g.obj_id AND c.cond_num=g.cond_num
;
--    n    | n_join  | n_obj_dist | n_obj_join_dist | n_objcd_dist | n_objcd_join_dist
--   ------+---------+------------+-----------------+--------------+-------------------
-- 1686181 | 1659168 |    1634327 |         1607962 |      1655689 |           1629299

-- Duplicados na tabela das geometrias:
SELECT obj_id, cond_num, count(*) n
FROM tmp_orig.mvw0_union3_idx
GROUP BY 1,2  HAVING COUNT(*)>1
order by 3 desc, 1,2; -- ~ duplicados
/*
obj_id   | cond_num | n
------------+----------+---
1670130001 | 00       | 5
130010001 | 00       | 4
380030001 | 00       | 4
440200001 | 00       | 4
760050001 | 00       | 4
810250001 | 00       | 4
20010001 | 00       | 3
30190001 | 00       | 3
40040001 | 00       | 3
...
1661660001 | 00       | 2
1661670001 | 00       | 2
1661880001 | 00       | 2
1661910001 | 00       | 2
1661910002 | 00       | 2
*/

-- Duplicados na tabela enxuta do IPTU:
SELECT obj_id, cond_num, count(*) n
FROM tmp_orig.vw1159_01a_enxuto
GROUP BY 1,2  HAVING COUNT(*)>1
order by 3 desc, 1,2; -- ~ duplicados
/*
obj_id   | cond_num |  n
------------+----------+-----
1682560000 | 01       | 168
714310000 | 01       |  96
1920170000 | 01       |  53
1121440000 | 01       |  42
311160000 | 01       |  38
1122090000 | 01       |  35
...
*/












---------------------------
SELECT obj_id, cond_num, area_m2,
       round(d::numeric,1) as diff,
       round(dp::numeric,3)::text||'%' as diff_perc
FROM (
  SELECT obj_id, cond_num, round(area) as area_m2,
         area2-area as d,
         200.0*abs(area2-area)/(area2+area) as dp
  FROM  tmp_orig.mvw0_union3_idx
  WHERE ABS(area-area2)>1 -- risco de imprecisão
  order by 4 desc
) t LIMIT 100
; /*
obj_id   | cond_num | area_m2 | diff | diff_perc
------------+----------+---------+------+-----------
2290650096 | 00       |    5642 |  1.8 | 0.032%
2290520001 | 00       |    6453 |  2.1 | 0.032%
2290030001 | 00       |    4426 |  1.4 | 0.032%
2290010003 | 00       |   14672 |  4.6 | 0.032%
2290010001 | 00       |    4601 |  1.5 | 0.032%
...
obj_id   | cond_num | area_m2 | diff  | diff_perc
------------+----------+---------+-------+-----------
1290010002 | 00       | 2639589 | 543.7 | 0.021%
732720002 | 00       | 2119548 | 229.1 | 0.011%
2710150001 | 00       | 2314283 | 196.0 | 0.008%
1580010001 | 00       | 1525173 | 192.4 | 0.013%
1110010002 | 00       |  793112 | 186.5 | 0.024%
2350100001 | 00       |  628038 | 183.4 | 0.029%
...
*/

SELECT obj_id, count(*) n, count(distinct cond_num) n_cond
FROM tmp_orig.mvw0_union3_idx
GROUP BY 1 order by 2 desc
; /*
obj_id   | n  | n_cond
------------+----+--------
100480000 | 36 |     36
360030000 | 34 |     34
160650000 | 32 |     32
1430720000 | 30 |     30
380440000 | 30 |     30
2150030000 | 30 |     30
...  */

-- LEFT vs RIGHT da geometria vs cadastro:
SELECT g.obj_id,
       max((c.cond_num is not null)::int)=1 as is_condo,
       max(c.cond_num::int) max_condo_num,
       count(*) n
FROM tmp_orig.mvw0_union3_idx g LEFT JOIN tmp_orig.vw1159_01a_enxuto  c
    ON c.obj_id =g.obj_id
GROUP BY 1  HAVING COUNT(*)>1
order by 4 desc, 1; -- ~17901 duplicados
/*
obj_id   | is_condo | max_condo_num |  n
------------+----------+---------------+------
360030000 | t        |            38 | 1870
100480000 | t        |            38 | 1728
70120000 | t        |            29 | 1403
160650000 | t        |            32 | 1376
100240000 | t        |            30 | 1276
280200000 | t        |            29 | 1274
50760000 | t        |            24 | 1265
380440000 | t        |            33 | 1230
100120000 | t        |            24 | 1150
...
*/
SELECT c.obj_id,
       max((g.cond_num is not null)::int)=1 as is_condo,
       max(g.cond_num::int) max_condo_num,
       count(*) n
FROM tmp_orig.mvw0_union3_idx g RIGHT JOIN tmp_orig.vw1159_01a_enxuto  c
    ON c.obj_id =g.obj_id
GROUP BY 1  HAVING COUNT(*)>1
order by 4 desc, 1; -- ~17374 duplicados
/*
obj_id   | is_condo | max_condo_num |  n
------------+----------+---------------+------
360030000 | t        |            38 | 1870
100480000 | t        |            38 | 1728
70120000 | t        |            29 | 1403
160650000 | t        |            32 | 1376
100240000 | t        |            30 | 1276
280200000 | t        |            29 | 1274
50760000 | t        |            24 | 1265
380440000 | t        |            33 | 1230
100120000 | t        |            24 | 1150
...
*/

-- (INNER) Duplicados com cond_num amarrado:
SELECT c.obj_id,c.cond_num,
       count(*) n
FROM tmp_orig.mvw0_union3_idx g INNER JOIN tmp_orig.vw1159_01a_enxuto  c
    ON c.obj_id =g.obj_id AND g.cond_num=c.cond_num
GROUP BY 1,2  HAVING COUNT(*)>1
order by 3 desc, 1,2; -- ~18195 linhas (casos duplicados)
/*
   obj_id   | cond_num | n
------------+----------+----
  714310000 | 01       | 96
 1121440000 | 01       | 42
 1322400000 | 01       | 34
  714360000 | 01       | 31
 1871990000 | 01       | 31
  370360000 | 04       | 28
 1960120000 | 01       | 28
 1301600000 | 01       | 27
...
220490000 | 05       |  2
220490001 | 00       |  2
220500001 | 00       |  2
220510000 | 02       |  2
220520001 | 00       |  2
...
*/
