#!/bin/sh

set -x

if [ -z "$build" ]; then
	build=Release
fi

TarBall=terark-db-win-x64-$build
rm -rf ${TarBall}
mkdir -p ${TarBall}/api
mkdir -p ${TarBall}/bin
mkdir -p ${TarBall}/lib
mkdir -p ${TarBall}/include/terark/db
mkdir -p ${TarBall}/include/terark/io
mkdir -p ${TarBall}/include/terark/thread
mkdir -p ${TarBall}/include/terark/util

cp x64/$build/terark*.dll                   ${TarBall}/lib
cp x64/$build/terark*.lib                   ${TarBall}/lib
cp x64/$build/terark*.pdb                   ${TarBall}/lib
#cp ../../../terark/vs2015/terark-fsa/x64/$build/*.dll  ${TarBall}/lib
#cp ../../../terark/vs2015/terark-fsa/x64/$build/*.lib  ${TarBall}/lib
cp x64/$build/terark-db-schema-compile.exe  ${TarBall}/bin
cp -r ../../api/leveldb/leveldb/include      ${TarBall}/api
cp ../../src/terark/db/db_conf.hpp           ${TarBall}/include/terark/db
cp ../../src/terark/db/db_context.hpp        ${TarBall}/include/terark/db
cp ../../src/terark/db/db_index.hpp          ${TarBall}/include/terark/db
cp ../../src/terark/db/db_store.hpp          ${TarBall}/include/terark/db
cp ../../src/terark/db/db_segment.hpp        ${TarBall}/include/terark/db
cp ../../src/terark/db/db_table.hpp          ${TarBall}/include/terark/db
cp ../../src/terark/db/db_dll_decl.hpp       ${TarBall}/include/terark/db
cp ../../terark-base/src/terark/*.hpp        ${TarBall}/include/terark
cp ../../terark-base/src/terark/io/*.hpp     ${TarBall}/include/terark/io
cp ../../terark-base/src/terark/thread/*.hpp ${TarBall}/include/terark/thread
cp ../../terark-base/src/terark/util/*.hpp   ${TarBall}/include/terark/util
git log -n 1 > ${TarBall}/package.version.txt

tar cjf ${TarBall}.tar.bz2 ${TarBall}
chmod a-x ${TarBall}.tar.bz2
scp ${TarBall}.tar.bz2 root@nark.cc:/var/www/html/download
