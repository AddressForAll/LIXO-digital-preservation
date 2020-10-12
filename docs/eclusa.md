# ECLUSA DE DADOS

<img src="assets/eclusa123-ico.200x.png" align="right">

A entrega de dados brutos pode ser realizada arquivo por arquivo na API ou "em lote", ambas pelo *técnico responsável* devidamente autenticado. A "entrega em lote" é realizada por protocolo SFTP, no ambiente apelidado de Eclusa, garantindo-se o seguinte workflow:

1. O técnico responsável recebe os dados originais, os revisa e os organiza.
2. O técnico responsável efetua na sua home SFTP (`/home/{user}`) dos arquivos desejados nas pastas padronizadas.
3. O software da Eclusa cria hashes e leva os metadados para a base de dados, depois de oferecer recursos de confirmação na API. Os dados ficam preservados em área de quarentena (períorodo de embargo e testes) e depois em repositório definitivo.

Todo o workflow e garantia de geração de *hash* é efetuado pela Eclusa.

## Códigos-fonte da Eclusa e demais softwares

Ver [digital-preservation/src](http://git.AddressForAll.org/digital-preservartion/tree/master/src).

## Reinicializacao depois do boot
Com o boot do servidor todos os conteudos da pasta `/tmp` sao perdidos, devemos portanto reinicializa-la com as devidas permissoes.

```sh
sudo make -C /opt/gits/WS/src  ini_tmprefresh    # important
make -C /opt/gits/WS/src  ini_database_list_tmp  # only a test
sudo make -C /opt/gits/digital-preservartion/src  ecl_run    # important
```
