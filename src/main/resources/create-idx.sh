#!/usr/bin/env bash
curl -XDELETE http://develastic-vh.gbif.org:9200/_all
curl -XPUT http://develastic-vh.gbif.org:9200/literature -d @mendeley_mapping.json
