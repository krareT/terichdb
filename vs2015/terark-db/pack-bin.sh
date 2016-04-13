#!/bin/sh

set -x

rm -rf terark-db-win-x64
mkdir -p terark-db-win-x64

cp x64/Release/*.dll terark-db-win-x64
git log -n 1 > terark-db-win-x64/package.version.txt

tar cjf terark-db-win-x64.tar.bz2 terark-db-win-x64
scp terark-db-win-x64.tar.bz2 root@nark.cc:/var/www/html/download
