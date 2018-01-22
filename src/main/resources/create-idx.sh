#!/usr/bin/env bash
curl -XDELETE http://develastic-vh.gbif.org:9200/_all
curl -XPUT http://develastic-vh.gbif.org:9200/literature -d @mendeley_mapping.json


curl -XPUT http://localhost:9200/literature/_alias/literature_idx

curl -XDELETE http://devcmssearch-vh.gbif.org:9200/.monitoring-es-2-2017.05.1
