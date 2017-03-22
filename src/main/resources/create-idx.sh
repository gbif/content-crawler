#!/usr/bin/env bash
curl -XDELETE http://localhost:9200/_all
curl -XPUT http://develastic-vh.gbif.org:9200/literature -d @mendeley_mapping.json
