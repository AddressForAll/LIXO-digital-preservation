
DROP TABLE tmp1_jur;
CREATE TABLE tmp1_jur (
   code text,item text,label_en text,label_es text, local_id text, osmId_min bigint, ldc text
);

PREPARE tmp1_ins_select (text) AS
  INSERT INTO optim.jurisdiction (
   osm_id, jurisd_base_id, jurisd_local_id, name, parent_abbrev, abbrev, wikidata_id, lexlabel,
   isolabel_ext, ddd, info,admin_level,parent_id, name_en
 )
 SELECT COALESCE(osmId_min,-wikidata_id),
        t2.jurisd_base_id,        
        iif(local_id is not null,local_id::bigint, wikidata_id) jurisd_local_id,
        COALESCE(label_es,label_en) as name,
        t2.pabbrev as parent_abbrev,
        abbrev_code as abbrev, 
        wikidata_id,
        lower(abbrev_code) as lexlabel, -- replace(lower(code,'-',';'))
        code as isolabel_ext, 
        replace(ldc,'+','')::int as ddd,
        NULL::jsonb as info,
        4 as admin_level,
        t2.parentid as parent_id,
        label_en as name_en
  FROM (
    SELECT *,  regexp_replace(code,'^[A-Z][A-Z]\-','') as abbrev_code,
           replace(item,'http://www.wikidata.org/entity/Q','')::bigint wikidata_id
    FROM tmp1_jur
  ) t1, (
    SELECT osm_id as parentid, jurisd_base_id, abbrev as pabbrev FROM optim.jurisdiction WHERE admin_level=2 AND abbrev=$1
  ) t2;
-----
DELETE FROM tmp1_jur; COPY tmp1_jur FROM '/tmp/pg_io/wdquery-CL.csv' CSV HEADER;
 EXECUTE tmp1_ins_select('CL');
DELETE FROM tmp1_jur; COPY tmp1_jur FROM '/tmp/pg_io/wdquery-CO.csv' CSV HEADER;
 EXECUTE tmp1_ins_select('CO');
DELETE FROM tmp1_jur; COPY tmp1_jur FROM '/tmp/pg_io/wdquery-EC.csv' CSV HEADER;
 EXECUTE tmp1_ins_select('EC');
DELETE FROM tmp1_jur; COPY tmp1_jur FROM '/tmp/pg_io/wdquery-PE.csv' CSV HEADER;
 EXECUTE tmp1_ins_select('PE');
DELETE FROM tmp1_jur; COPY tmp1_jur FROM '/tmp/pg_io/wdquery-VE.csv' CSV HEADER;
 EXECUTE tmp1_ins_select('VE');
