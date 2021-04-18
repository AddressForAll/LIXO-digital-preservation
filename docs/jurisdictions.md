# Inclusão de novas jurisdições

As jurisdições são organizadas em níveis, seguindo-se a convenção da tag [`admin_level` do OpenStreetMap](https://wiki.openstreetmap.org/wiki/Key:admin_level). Os países correspondem ao [nível administrativo](https://wiki.openstreetmap.org/wiki/Tag:boundary%3Dadministrative) mais importante, em função da sua soberania quanto às demais convenções, por exemplo relativas ao endereço das casas dos seus habitantes ou às subdivisões do país.

Para evitar controvérsias em torno do conceito de "país", seguindo as diretivas OpenStreetMap e Wikidata, apenas entidades políticas listados no padrão [ISO 3166](https://en.wikipedia.org/wiki/List_of_ISO_3166_country_codes) devem ser considerados países. São portanto 3 identificadores reforçando a caracterização do país, seus metadados e seus limites geográficos:

* Indetificador OpenStreetMap (`osm_id`), por exemplo o Ecuador é delimitado pela [*relation* 108089](https://www.openstreetmap.org/relation/108089).

* Identificadores numérico (`jurisd_base_id`) e de duass letras (`isolabel_ext`) da [ISO_3166-2](https://en.wikipedia.org/wiki/ISO_3166-2). Por rexemplo o Ecuador tem `jurisd_base_id=218` e [`isolabel_ext=EC`], e as suas subdivisões de primeiro nível em conformidade com [ISO&nbsp;3166&#8209;2:EC](https://en.wikipedia.org/wiki/ISO_3166-2:EC), tais como Santa Elena (`EC-SE`) e Tungurahua (`EC-T`).

* Identificador Wikidata (`wikidata_id`), por exemplo  Ecuador tem [`wikidata_id=736`](http://wikidata.org/entity/Q736), Santa Elena [`wikidata_id=1124125`](http://wikidata.org/entity/Q1124125) e Tungurahua [`wikidata_id=504252`](http://wikidata.org/entity/Q504252).

Cabe ao administrador do projeto AddressForAll garantir que todos esses elementos definidores de jurisdição estejam consistentes entre si e tenham sua definição baseada em documentos oficiais, com cópias digitais devidamente preservadas. A homologação das subdivisões mais específicas cabe ao representante AddressForAll de cada país.

