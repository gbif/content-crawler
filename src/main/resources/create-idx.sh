#!/usr/bin/env bash
curl -XDELETE http://localhost:9200/literature
curl -XPUT http://localhost:9200/literature -d @mendeley_mapping.json
