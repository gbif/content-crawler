#!/usr/bin/env bash
#exit on any failure
set -e

P=$1
TOKEN=$2
COMMAND=$3

echo "Get configuration from github"
curl -s -H "Authorization: token $TOKEN" -H 'Accept: application/vnd.github.v3.raw' -O -L https://api.github.com/repos/gbif/gbif-configuration/contents/content-crawler/$P.yml
echo "Downloading latest snapshot"
wget -O content-crawler.jar "http://repository.gbif.org/service/local/artifact/maven/redirect?g=org.gbif.content&a=content-crawler&v=LATEST&r=snapshots&e=jar"
echo "Running crawler"
java -jar content-crawler.jar $COMMAND --conf $P.yml
rm -rf content-crawler.jar $P.yml
echo "Crawl has finished"



