
## STEP1
cd /tmp/pg_io/
# rm -f runHashes-*.sh
psql "postgres://postgres@localhost/dl03t_main" -c "SELECT * FROM eclusa.vw01alldft_cityfolder_runhashes"
for sh_file in /tmp/pg_io/runHashes-*.sh; do
    echo "Counting lines and executing $sh_file"
    wc -l "$sh_file"
    sh "$sh_file"
done

## STEP2
cd /tmp/pg_io/
# rm -f runCpFiles-*.sh
# faz INSERTS no ORIGIN
psql "postgres://postgres@localhost/dl03t_main" -c "SELECT * FROM eclusa.vw01alldft_cityfolder_run_cpfiles"
for sh_file in /tmp/pg_io/runCpFiles-*.sh; do
  echo "Counting lines and executing $sh_file"
  wc -l "$sh_file"
  sh "$sh_file"
done
