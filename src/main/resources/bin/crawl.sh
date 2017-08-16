#!/usr/bin/env bash

#Downloads latest snapshot.
function download {
  echo "Downloading content crawler"
  wget -O content-crawler.jar "http://repository.gbif.org/service/local/artifact/maven/redirect?g=org.gbif.content&a=content-crawler&v=LATEST&r=$1&e=jar"
  mv latest.sha1 jar.sha1
}

#Downloads latest sha1 checksum.
function downloadJarSha1 {
  curl -s -L "http://repository.gbif.org/service/local/artifact/maven/resolve?g=org.gbif.content&a=content-crawler&v=LATEST&r=$1&e=jar" | sed -n 's/<sha1>\(.*\)<\/sha1>/\1/p' > latest.sha1
}

#Download configuration settings from GitHub.
#Receives as parameter the environment/file name to be downloaded.
function downloadConfig {
  echo "Get configuration from github"
  curl -s -H "Authorization: token $1" -H 'Accept: application/vnd.github.v3.raw' -O -L https://api.github.com/repos/gbif/gbif-configuration/contents/content-crawler/$2.yml
}

#exit on any failure
set -e

P=$1
TOKEN=$2
COMMAND=$3
REPOSITORY=${4:-snapshots}

downloadConfig $TOKEN $P
downloadJarSha1 $REPOSITORY
if [ -f jar.sha1 ] && [ -f content-crawler.jar ]; then
  if ! cmp -s latest.sha1 jar.sha1; then
    download $REPOSITORY
  else
    echo "Using previously download jar file since content is the same"
  fi
else
 download $REPOSITORY
fi
rm -f latest.sha1
echo "Running crawler"
java -jar content-crawler.jar $COMMAND --conf $P.yml
rm -f $P.yml
echo "Crawl has finished"


