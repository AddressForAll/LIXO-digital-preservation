python3 run_mustache.py --json_inline='{"code":"CL","country_wd_id":"Q298","name":"Chile","local_prop":"P6929"}' \
   -t getWikidata.mustache > getWikidata.sh
python3 run_mustache.py  --json_inline='{"code":"CO","country_wd_id":"Q739","name":"Colombia"}' \
   -t getWikidata.mustache >> getWikidata.sh
python3 run_mustache.py  --json_inline='{"code":"EC","country_wd_id":"Q736","name":"Ecuador"}' \
   -t getWikidata.mustache >> getWikidata.sh
python3 run_mustache.py  --json_inline='{"code":"PE","country_wd_id":"Q419","name":"Peru","local_prop":"P844"}' \
   -t getWikidata.mustache >> getWikidata.sh
python3 run_mustache.py  --json_inline='{"code":"VE","country_wd_id":"Q717","name":"Venezuela"}' \
   -t getWikidata.mustache >> getWikidata.sh

mv getWikidata.sh /tmp/pg_io/
cd /tmp/pg_io/
sh getWikidata.sh   # gera cada um dos arquivos "wdquery-{{code}}.csv"
