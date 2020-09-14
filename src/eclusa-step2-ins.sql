-- ECLUSA STEP2 INSERTS

CREATE FOREIGN TABLE tmp_orig.fdw_br_donor (
  scope text,
  "vatID" text,
  "shortName" text,
  "legalName" text,
  "wikidataQID" text,
  url text
) SERVER files OPTIONS (
   filename '/tmp/pg_io/br-donor.csv'
   ,format 'csv'
   ,delimiter ','
   ,header 'true'
);
INSERT INTO optim.donor (scope,vat_ID,shortName,legalName,wikidata_ID,url)
  SELECT scope, "vatID", "shortName", "legalName",
         substr("wikidataQID",2)::bigint,
         url
  FROM tmp_orig.fdw_br_donor
  ON CONFLICT DO NOTHING
;

CREATE FOREIGN TABLE tmp_orig.fdw_br_city_codes (
  name text,
  state text,
  "wdId" text,
  "idIBGE" int,
  "lexLabel" text,
  creation integer,
  extinction integer,
  "postalCode_ranges" text,
  ddd integer,
  abbrev3 text,
  notes text
) SERVER files OPTIONS (
   filename '/tmp/pg_io/br-city-codes.csv'
   ,format 'csv'
   ,delimiter ','
   ,header 'true'
);

INSERT INTO optim.jurisdiction(local_jurisd_id,name,state,abbrev3,wikidata_id,lexlabel,isolabel_ext,ddd,info)
  SELECT   "idIBGE"::int, name,
            state, abbrev3, -- upper
            substr("wdId",2)::bigint,
            "lexLabel",
            'BR-'||state||'-'||lexname_to_unix("lexLabel"),
            ddd,
            jsonb_build_object(
              'postalCode_ranges',"postalCode_ranges",
              'notes',notes,
              'creation',creation,
              'extinction',extinction
            ) AS  info
  FROM tmp_orig.fdw_br_city_codes
ON CONFLICT DO NOTHING
; --

CREATE FOREIGN TABLE tmp_orig.fdw_br_donatedPack (
  donor_id int,
  pack_id int,
  accepted_date text,
  label_ref text,
  about text,
  contentReferenceTime text,
  creator text,
  licensedExp text,
  license_main text,
  url_objType text,
  uri text
) SERVER files OPTIONS (
   filename '/tmp/pg_io/donatedPack.csv'
   ,format 'csv'
   ,delimiter ','
   ,header 'true'
);


INSERT INTO optim.donatedPack (pack_id,donor_id,accepted_date,about,info)
  SELECT pack_id,donor_id,accepted_date::date,about,
         jsonb_build_object(
             'label_ref',label_ref,'creator',creator,
             'licensedExp',licensedExp,'license_main',license_main,
             'url_objType',url_objType, 'uri',uri
         )
  FROM tmp_orig.fdw_br_donatedPack
  WHERE length(accepted_date)>=4 AND donor_id IN (select id from optim.donor)
ON CONFLICT DO NOTHING
;
-- verifica por hora com: select scope,vat_ID,shortName,legalName, p.pack_id, p.donor_id from optim.donor d inner join (select pack_id, donor_id, info->>'label_ref' labref from optim.donatedPack) p ON p.labref=d.shortname;
-- assim podemos trocaar o antigo
