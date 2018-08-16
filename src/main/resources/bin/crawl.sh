#!/usr/bin/env bash

#Downloads latest snapshot.
function download {
  echo "Downloading content crawler"
  URL=`getArtifactUrl -s http://repository.gbif.org -g org.gbif.content -a content-crawler -v LATEST -r $1`
  echo Download ${URL}
  wget --progress=dot:mega -O content-crawler.jar "${URL}"
  mv latest.sha1 jar.sha1
}

#Downloads latest sha1 checksum.
function downloadJarSha1 {
  URL=`getArtifactUrl -s http://repository.gbif.org -g org.gbif.content -a content-crawler -v LATEST -r $1`
  echo Download jar sha1 ${URL}.sha1
  curl -s -L "${URL}.sha1" | sed -n 's/<sha1>\(.*\)<\/sha1>/\1/p' > latest.sha1
}

#Download configuration settings from GitHub.
#Receives as parameter the environment/file name to be downloaded.
function downloadConfig {
  echo "Get configuration from github"
  curl -s -H "Authorization: token $1" -H 'Accept: application/vnd.github.v3.raw' -O -L https://api.github.com/repos/gbif/gbif-configuration/contents/content-crawler/$2.yml
}

function getArtifactUrl {

  OPTIND=1         # Reset in case getopts has been used previously in the shell.

  PACKAGING=jar
  CLASSIFIER=

  while getopts "s:g:a:v:r:c:e:" OPTION
  do
       case ${OPTION} in
           s)
               SONAR_REDIRECT_URL=${OPTARG}
               ;;
           g)
               GROUP_ID=${OPTARG}
               ;;
           a)
               ARTIFACT_ID=${OPTARG}
               ;;
           v)
               VERSION=${OPTARG}
               ;;
           r)
               REPOSITORY=${OPTARG}
               ;;
           c)
               CLASSIFIER=${OPTARG}
               ;;
           e)
               PACKAGING=${OPTARG}
               ;;
       esac
  done

  GROUP_ID_URL=${GROUP_ID//.//}
  SONAR_REDIRECT_URL=${SONAR_REDIRECT_URL}/repository/${REPOSITORY}/${GROUP_ID_URL}/${ARTIFACT_ID}

  if [ -n "$CLASSIFIER" ]; then
    CLASSIFIER=-${CLASSIFIER}
  fi

  if [[ ${VERSION} == "LATEST" || ${VERSION} == *SNAPSHOT* ]] ; then

    if [[ "${VERSION}" == "LATEST" ]] ; then
      VERSION=$(xmllint --xpath "//metadata/versioning/versions/version[last()]/text()" <(curl -s "${SONAR_REDIRECT_URL}/maven-metadata.xml"))
    fi

    if [[ "${VERSION}" == *SNAPSHOT* ]] ; then
      META_PATH=${SONAR_REDIRECT_URL}/${VERSION}/maven-metadata.xml
      TIMESTAMP=`curl -s "${META_PATH}" | xmllint --xpath "string(//timestamp)" -`
      BUILD_NUMBER=`curl -s "${META_PATH}" | xmllint --xpath "string(//buildNumber)" -`

      FILENAME=${ARTIFACT_ID}-${VERSION%-SNAPSHOT}-${TIMESTAMP}-${BUILD_NUMBER}${CLASSIFIER}.${PACKAGING}
    else
      FILENAME=${ARTIFACT_ID}-${VERSION}${CLASSIFIER}.${PACKAGING}
    fi

  else

    if [[ ${VERSION} == "RELEASE" ]] ; then
      VERSION=$(xmllint --xpath "string(//release)" <(curl -s "${SONAR_REDIRECT_URL}/maven-metadata.xml"))
    fi

    FILENAME=${ARTIFACT_ID}-${VERSION}${CLASSIFIER}.${PACKAGING}
  fi

  SONAR_DOWNLOAD_URL=${SONAR_REDIRECT_URL}/${VERSION}/${FILENAME}

  echo ${SONAR_DOWNLOAD_URL}
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
