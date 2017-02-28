#!/bin/sh

set -x

if [ -z "$build" ]; then
	build=Release
fi

TarBall=terichdb-win-x64-$build
rm -rf ${TarBall}
mkdir -p ${TarBall}/api
mkdir -p ${TarBall}/bin
mkdir -p ${TarBall}/lib
mkdir -p ${TarBall}/include/terark/terichdb
mkdir -p ${TarBall}/include/terark/io
mkdir -p ${TarBall}/include/terark/thread
mkdir -p ${TarBall}/include/terark/util

cp x64/$build/terark*.dll                   ${TarBall}/lib
cp x64/$build/terark*.lib                   ${TarBall}/lib
cp x64/$build/terark*.pdb                   ${TarBall}/lib
#cp ../../../terark/vs2015/terark-fsa/x64/$build/*.dll  ${TarBall}/lib
#cp ../../../terark/vs2015/terark-fsa/x64/$build/*.lib  ${TarBall}/lib
cp x64/$build/terichdb-schema-compile.exe  ${TarBall}/bin
cp -r ../../api/leveldb/leveldb/include      ${TarBall}/api
cp ../../src/terark/terichdb/db_conf.hpp           ${TarBall}/include/terark/terichdb
cp ../../src/terark/terichdb/db_context.hpp        ${TarBall}/include/terark/terichdb
cp ../../src/terark/terichdb/db_index.hpp          ${TarBall}/include/terark/terichdb
cp ../../src/terark/terichdb/db_store.hpp          ${TarBall}/include/terark/terichdb
cp ../../src/terark/terichdb/db_segment.hpp        ${TarBall}/include/terark/terichdb
cp ../../src/terark/terichdb/db_table.hpp          ${TarBall}/include/terark/terichdb
cp ../../src/terark/terichdb/db_dll_decl.hpp       ${TarBall}/include/terark/terichdb
cp ../../terark-base/src/terark/*.hpp        ${TarBall}/include/terark
cp ../../terark-base/src/terark/io/*.hpp     ${TarBall}/include/terark/io
cp ../../terark-base/src/terark/thread/*.hpp ${TarBall}/include/terark/thread
cp ../../terark-base/src/terark/util/*.hpp   ${TarBall}/include/terark/util
git log -n 1 > ${TarBall}/package.version.txt

tar cjf ${TarBall}.tar.bz2 ${TarBall}
chmod a-x ${TarBall}.tar.bz2
scp ${TarBall}.tar.bz2 root@nark.cc:/var/www/html/download
