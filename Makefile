DBG_FLAGS ?= -g3 -D_DEBUG
RLS_FLAGS ?= -O3 -DNDEBUG -g3
#WITH_BMI2 ?= $(shell bash ./cpu_has_bmi2.sh)
WITH_BMI2 ?= 0

ifeq "$(origin LD)" "default"
  LD := ${CXX}
endif

#COMPILER := $(shell ${CXX} --version | head -1 | awk '{split($$3, Ver, "."); printf("%s-%d.%d", $$1, Ver[1], Ver[2]);}')
# Makefile is stupid to parsing $(shell echo ')')
#COMPILER := $(shell ${CXX} --version | head -1 | sed 's/\(\S\+\)\s\+([^()]*)\s\+\([0-9]\+.[0-9]\+\).*/\1-\2/')
tmpfile := $(shell mktemp compiler-XXXXXX)
COMPILER := $(shell ${CXX} tools/configure/compiler.cpp -o ${tmpfile}.exe && ./${tmpfile}.exe && rm -f ${tmpfile}*)
#$(error COMPILER=${COMPILER})
UNAME_MachineSystem := $(shell uname -m -s | sed 's:[ /]:-:g')
BUILD_NAME := ${UNAME_MachineSystem}-${COMPILER}-bmi2-${WITH_BMI2}
BUILD_ROOT := build/${BUILD_NAME}
ddir:=${BUILD_ROOT}/dbg
rdir:=${BUILD_ROOT}/rls

gen_sh := $(dir $(lastword ${MAKEFILE_LIST}))gen_env_conf.sh

err := $(shell env BOOST_INC=${BOOST_INC} bash ${gen_sh} "${CXX}" ${COMPILER} ${BUILD_ROOT}/env.mk; echo $$?)
ifneq "${err}" "0"
   $(error err = ${err} MAKEFILE_LIST = ${MAKEFILE_LIST}, PWD = ${PWD}, gen_sh = ${gen_sh} "${CXX}" ${COMPILER} ${BUILD_ROOT}/env.mk)
endif

TERARK_INC := -Isrc

include ${BUILD_ROOT}/env.mk

UNAME_System := $(shell uname | sed 's/^\([0-9a-zA-Z]*\).*/\1/')
ifeq (CYGWIN, ${UNAME_System})
  FPIC =
  # lazy expansion
  CYGWIN_LDFLAGS = -Wl,--out-implib=$@ \
				   -Wl,--export-all-symbols \
				   -Wl,--enable-auto-import
  DLL_SUFFIX = .dll.a
  CYG_DLL_FILE = $(shell echo $@ | sed 's:\(.*\)/lib\([^/]*\)\.a$$:\1/cyg\2:')
else
  ifeq (Darwin,${UNAME_System})
    DLL_SUFFIX = .dylib
  else
    DLL_SUFFIX = .so
  endif
  FPIC = -fPIC
  CYG_DLL_FILE = $@
endif
override CFLAGS += ${FPIC}
override CXXFLAGS += ${FPIC}
override LDFLAGS += ${FPIC}

ifeq "$(shell a=${COMPILER};echo $${a:0:3})" "g++"
  ifeq (Linux, ${UNAME_System})
    override LDFLAGS += -rdynamic
  endif
  ifeq (${UNAME_System},Darwin)
    COMMON_C_FLAGS += -Wa,-q
  endif
  override CXXFLAGS += -time
  ifeq "$(shell echo ${COMPILER} | awk -F- '{if ($$2 >= 4.8) print 1;}')" "1"
    CXX_STD := -std=gnu++1y
  endif
endif

ifeq "${CXX_STD}" ""
  CXX_STD := -std=gnu++11
endif

# icc or icpc
ifeq "$(shell a=${COMPILER};echo $${a:0:2})" "ic"
  override CXXFLAGS += -xHost -fasm-blocks
  CPU = -xHost
else
  CPU = -march=native
  COMMON_C_FLAGS  += -Wno-deprecated-declarations
  ifeq "$(shell a=${COMPILER};echo $${a:0:5})" "clang"
    COMMON_C_FLAGS  += -fstrict-aliasing
  else
    COMMON_C_FLAGS  += -Wstrict-aliasing=3
  endif
endif

ifeq (${WITH_BMI2},1)
  CPU += -mbmi -mbmi2
else
  CPU += -mno-bmi -mno-bmi2
endif

COMMON_C_FLAGS  += -Wformat=2 -Wcomment
COMMON_C_FLAGS  += -Wall -Wextra
COMMON_C_FLAGS  += -Wno-unused-parameter
COMMON_C_FLAGS  += -D_GNU_SOURCE # For cygwin

#-v #-Wall -Wparentheses
#COMMON_C_FLAGS  += ${COMMON_C_FLAGS} -Wpacked -Wpadded -v
#COMMON_C_FLAGS	 += ${COMMON_C_FLAGS} -Winvalid-pch
#COMMON_C_FLAGS  += ${COMMON_C_FLAGS} -fmem-report

ifeq "$(shell a=${COMPILER};echo $${a:0:5})" "clang"
  COMMON_C_FLAGS += -fcolor-diagnostics
endif

#CXXFLAGS +=
#CXXFLAGS += -fpermissive
#CXXFLAGS += -fexceptions
#CXXFLAGS += -fdump-translation-unit -fdump-class-hierarchy

override CFLAGS += ${COMMON_C_FLAGS}
override CXXFLAGS += ${COMMON_C_FLAGS}
#$(error ${CXXFLAGS} "----" ${COMMON_C_FLAGS})

DEFS := -D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE

override INCS := ${TERARK_INC} ${INCS}

ifeq (, $(findstring ${BOOST_INC}, ${INCS} /usr/include /usr/local/include))
  override INCS += -I${BOOST_INC}
endif

ifeq (, $(findstring ${BOOST_LIB}, /usr/lib64 /usr/lib /usr/local/lib))
  override LIBS += -L${BOOST_LIB}
endif

LIBBOOST ?= \
	  -lboost_filesystem${BOOST_SUFFIX} \
	  -lboost_system${BOOST_SUFFIX}


ifeq "1" "0"
ifeq ($(shell test -d /usr/local/lib64 && echo 1),1)
  override LIBS += -L/usr/local/lib64
endif
ifeq ($(shell test -d /usr/local/lib && echo 1),1)
  override LIBS += -L/usr/local/lib
endif
ifeq ($(shell test -d /usr/lib64 && echo 1),1)
  override LIBS += -L/usr/lib64
endif
ifeq ($(shell test -d /usr/lib && echo 1),1)
  override LIBS += -L/usr/lib
endif
endif

#LIBS += -ldl
#LIBS += -lpthread
override LIBS += ${LIBBOOST}

#extf = -pie
extf = -fno-stack-protector
#extf+=-fno-stack-protector-all
override CFLAGS += ${extf}
#override CFLAGS += -g3
override CXXFLAGS += ${extf}
#override CXXFLAGS += -g3
#CXXFLAGS += -fnothrow-opt

override INCS += -Iapi/leveldb/leveldb/include
override INCS += -Iapi/leveldb/leveldb
override INCS += -Iapi/leveldb
override INCS += -I/opt/include
override LIBS += -L/opt/lib

ifeq (, ${prefix})
	ifeq (root, ${USER})
		prefix := /usr
	else
		prefix := /home/${USER}
	endif
endif

TerichDB_src := $(wildcard src/terark/terichdb/*.cpp)

DfaDB_src := $(wildcard src/terark/terichdb/dfadb/*.cpp)
TrbDB_src := $(wildcard src/terark/terichdb/trbdb/*.cpp)
Tiger_src := $(wildcard src/terark/terichdb/wiredtiger/*.cpp)

LeveldbApi_src =
LeveldbApi_src += $(wildcard api/leveldb/leveldb_terark.cc)
LeveldbApi_src += $(wildcard api/leveldb/leveldb/db/*.cc)
LeveldbApi_src += api/leveldb/leveldb/util/coding.cc
LeveldbApi_src += api/leveldb/leveldb/util/comparator.cc
LeveldbApi_src += api/leveldb/leveldb/util/env.cc
LeveldbApi_src += api/leveldb/leveldb/util/env_posix.cc
LeveldbApi_src += api/leveldb/leveldb/util/logging.cc
LeveldbApi_src += api/leveldb/leveldb/util/options.cc
LeveldbApi_src += api/leveldb/leveldb/util/status.cc

TerichDB_lib := terichdb
DfaDB_lib := terichdb-dfadb
TrbDB_lib := terichdb-trbdb
Tiger_lib := terichdb-wiredtiger

ifeq (${WITH_BMI2},1)
  CORE_HOME := ../terark
else
  CORE_HOME := terark-base
endif
override INCS := -I${CORE_HOME}/src ${INCS}
LIB_TERARK_D := -L${CORE_HOME}/${BUILD_ROOT}/lib -lterark-core-${COMPILER}-d
LIB_TERARK_R := -L${CORE_HOME}/${BUILD_ROOT}/lib -lterark-core-${COMPILER}-r

#function definition
#@param:${1} -- targets var prefix, such as bdb_util | core
#@param:${2} -- build type: d | r
objs = $(addprefix ${${2}dir}/, $(addsuffix .o, $(basename ${${1}_src})))

TerichDB_d_o := $(call objs,TerichDB,d)
TerichDB_r_o := $(call objs,TerichDB,r)
TerichDB_d := ${BUILD_ROOT}/lib/lib${TerichDB_lib}-${COMPILER}-d${DLL_SUFFIX}
TerichDB_r := ${BUILD_ROOT}/lib/lib${TerichDB_lib}-${COMPILER}-r${DLL_SUFFIX}
static_TerichDB_d := ${BUILD_ROOT}/lib/lib${TerichDB_lib}-${COMPILER}-d.a
static_TerichDB_r := ${BUILD_ROOT}/lib/lib${TerichDB_lib}-${COMPILER}-r.a

DfaDB_d_o := $(call objs,DfaDB,d)
DfaDB_r_o := $(call objs,DfaDB,r)
DfaDB_d := ${BUILD_ROOT}/lib/lib${DfaDB_lib}-${COMPILER}-d${DLL_SUFFIX}
DfaDB_r := ${BUILD_ROOT}/lib/lib${DfaDB_lib}-${COMPILER}-r${DLL_SUFFIX}
static_DfaDB_d := ${BUILD_ROOT}/lib/lib${DfaDB_lib}-${COMPILER}-d.a
static_DfaDB_r := ${BUILD_ROOT}/lib/lib${DfaDB_lib}-${COMPILER}-r.a

TrbDB_d_o := $(call objs,TrbDB,d)
TrbDB_r_o := $(call objs,TrbDB,r)
TrbDB_d := ${BUILD_ROOT}/lib/lib${TrbDB_lib}-${COMPILER}-d${DLL_SUFFIX}
TrbDB_r := ${BUILD_ROOT}/lib/lib${TrbDB_lib}-${COMPILER}-r${DLL_SUFFIX}
static_TrbDB_d := ${BUILD_ROOT}/lib/lib${TrbDB_lib}-${COMPILER}-d.a
static_TrbDB_r := ${BUILD_ROOT}/lib/lib${TrbDB_lib}-${COMPILER}-r.a

Tiger_d_o := $(call objs,Tiger,d)
Tiger_r_o := $(call objs,Tiger,r)
Tiger_d := ${BUILD_ROOT}/lib/lib${Tiger_lib}-${COMPILER}-d${DLL_SUFFIX}
Tiger_r := ${BUILD_ROOT}/lib/lib${Tiger_lib}-${COMPILER}-r${DLL_SUFFIX}
static_Tiger_d := ${BUILD_ROOT}/lib/lib${Tiger_lib}-${COMPILER}-d.a
static_Tiger_r := ${BUILD_ROOT}/lib/lib${Tiger_lib}-${COMPILER}-r.a

LeveldbApi_lib := terichdb-leveldb-api
LeveldbApi_d_o := $(call objs,LeveldbApi,d)
LeveldbApi_r_o := $(call objs,LeveldbApi,r)
LeveldbApi_d := ${BUILD_ROOT}/lib/lib${LeveldbApi_lib}-${COMPILER}-d${DLL_SUFFIX}
LeveldbApi_r := ${BUILD_ROOT}/lib/lib${LeveldbApi_lib}-${COMPILER}-r${DLL_SUFFIX}
static_LeveldbApi_d := ${BUILD_ROOT}/lib/lib${LeveldbApi_lib}-${COMPILER}-d.a
static_LeveldbApi_r := ${BUILD_ROOT}/lib/lib${LeveldbApi_lib}-${COMPILER}-r.a

ALL_TARGETS = ${MAYBE_DBB_DBG} ${MAYBE_DBB_RLS} TerichDB LeveldbApi
DBG_TARGETS = ${MAYBE_DBB_DBG} ${TerichDB_d}
RLS_TARGETS = ${MAYBE_DBB_RLS} ${TerichDB_r}

override CFLAGS   += ${DEFS}
override CXXFLAGS += ${DEFS}

.PHONY : default all TerichDB LeveldbApi DfaDB TrbDB Tiger

default : TerichDB LeveldbApi TrbDB Tiger ${DFADB_TARGET}
all : ${ALL_TARGETS}
TerichDB: ${TerichDB_d} ${TerichDB_r} ${static_TerichDB_d} ${static_TerichDB_r}
DfaDB: ${DfaDB_d} ${DfaDB_r} ${static_DfaDB_d} ${static_DfaDB_r}
TrbDB: ${TrbDB_d} ${TrbDB_r} ${static_TrbDB_d} ${static_TrbDB_r}
Tiger: ${Tiger_d} ${Tiger_r} ${static_Tiger_d} ${static_Tiger_r}
LeveldbApi: ${LeveldbApi_d} ${LeveldbApi_r} ${static_LeveldbApi_d} ${static_LeveldbApi_r}
${LeveldbApi_d}: ${TerichDB_d}
${LeveldbApi_r}: ${TerichDB_r}
${DfaDB_d}: ${TerichDB_d}
${DfaDB_r}: ${TerichDB_r}
${TrbDB_d}: ${TerichDB_d}
${TrbDB_r}: ${TerichDB_r}
${Tiger_d}: ${TerichDB_d}
${Tiger_r}: ${TerichDB_r}

allsrc = ${TerichDB_src} ${DfaDB_src} ${TrbDB_src} ${Tiger_src} ${LeveldbApi_src}
alldep = $(addprefix ${rdir}/, $(addsuffix .dep, $(basename ${allsrc}))) \
         $(addprefix ${ddir}/, $(addsuffix .dep, $(basename ${allsrc})))

.PHONY : dbg rls
dbg: ${DBG_TARGETS}
rls: ${RLS_TARGETS}

ifneq (${UNAME_System},Darwin)
${TerichDB_d} ${TerichDB_r} : LIBS += -lrt
endif

#${TerichDB_d} : override LIBS := ${LIB_TERARK_D} ${LIBS} -ltbb_debug
${TerichDB_d} : override LIBS := ${LIB_TERARK_D} ${LIBS} -ltbb
${TerichDB_r} : override LIBS := ${LIB_TERARK_R} ${LIBS} -ltbb

${DfaDB_d} : override INCS += -I../terark/src
${DfaDB_r} : override INCS += -I../terark/src
${DfaDB_d} : override LIBS := -L../terark/${BUILD_ROOT}/lib -lterark-zbs-${COMPILER}-d -lterark-fsa-${COMPILER}-d -L${BUILD_ROOT}/lib -lterichdb-${COMPILER}-d ${LIB_TERARK_D} ${LIBS} -ltbb
${DfaDB_r} : override LIBS := -L../terark/${BUILD_ROOT}/lib -lterark-zbs-${COMPILER}-r -lterark-fsa-${COMPILER}-r -L${BUILD_ROOT}/lib -lterichdb-${COMPILER}-r ${LIB_TERARK_R} ${LIBS} -ltbb

${TrbDB_d} : override LIBS := -L${BUILD_ROOT}/lib -lterichdb-${COMPILER}-d ${LIB_TERARK_D} ${LIBS} -ltbb
${TrbDB_r} : override LIBS := -L${BUILD_ROOT}/lib -lterichdb-${COMPILER}-r ${LIB_TERARK_R} ${LIBS} -ltbb

${Tiger_d} : override LIBS := -L${BUILD_ROOT}/lib -lterichdb-${COMPILER}-d ${LIB_TERARK_D} ${LIBS} -ltbb -lwiredtiger
${Tiger_r} : override LIBS := -L${BUILD_ROOT}/lib -lterichdb-${COMPILER}-r ${LIB_TERARK_R} ${LIBS} -ltbb -lwiredtiger

${LeveldbApi_d} : override LIBS := -L${BUILD_ROOT}/lib -lterichdb-${COMPILER}-d ${LIB_TERARK_D} ${LIBS} -ltbb
${LeveldbApi_r} : override LIBS := -L${BUILD_ROOT}/lib -lterichdb-${COMPILER}-r ${LIB_TERARK_R} ${LIBS} -ltbb
${LeveldbApi_d} : ${TerichDB_d}
${LeveldbApi_r} : ${TerichDB_r}

${TerichDB_d} ${TerichDB_r} : LIBS += -lpthread

ifeq (${WITH_BMI2},1)
${TerichDB_d} : $(call objs,TerichDB,d)
${TerichDB_r} : $(call objs,TerichDB,r)
else
${TerichDB_d} : $(call objs,TerichDB,d) ${CORE_HOME}/${BUILD_ROOT}/lib/libterark-core-${COMPILER}-d${DLL_SUFFIX}
${TerichDB_r} : $(call objs,TerichDB,r) ${CORE_HOME}/${BUILD_ROOT}/lib/libterark-core-${COMPILER}-r${DLL_SUFFIX}
endif
${static_TerichDB_d} : $(call objs,TerichDB,d)
${static_TerichDB_r} : $(call objs,TerichDB,r)

${DfaDB_d} : $(call objs,DfaDB,d)
${DfaDB_r} : $(call objs,DfaDB,r)
${static_DfaDB_d} : $(call objs,DfaDB,d)
${static_DfaDB_r} : $(call objs,DfaDB,r)

${TrbDB_d} : $(call objs,TrbDB,d)
${TrbDB_r} : $(call objs,TrbDB,r)
${static_TrbDB_d} : $(call objs,TrbDB,d)
${static_TrbDB_r} : $(call objs,TrbDB,r)

${Tiger_d} : $(call objs,Tiger,d)
${Tiger_r} : $(call objs,Tiger,r)
${static_Tiger_d} : $(call objs,Tiger,d)
${static_Tiger_r} : $(call objs,Tiger,r)

${LeveldbApi_d} : $(call objs,LeveldbApi,d)
${LeveldbApi_r} : $(call objs,LeveldbApi,r)
${static_LeveldbApi_d} : $(call objs,LeveldbApi,d)
${static_LeveldbApi_r} : $(call objs,LeveldbApi,r)

TarBallBaseName := ${TerichDB_lib}-${BUILD_NAME}
TarBall := pkg/${TerichDB_lib}-${BUILD_NAME}
.PHONY : pkg
pkg: ${TarBall}.tgz
scp: ${TarBall}.tgz.scp.done
oss: ${TarBall}.tgz.oss.done
${TarBall}.tgz.scp.done : ${TarBall}.tgz
	scp -P 22    $< root@nark.cc:/var/www/html/download/
	touch $@
${TarBall}.tgz.oss.done : ${TarBall}.tgz
ifeq (${REVISION},)
	$(error var REVISION must be defined for target oss)
endif
	git tag ${REVISION}
	ossutil cp $< oss://terark-downloads/terichdb/${REVISION}/$(notdir $<) -f
	touch $@

${TarBall}.tgz : ${TerichDB_d} ${LeveldbApi_d} ${DfaDB_d} ${TrbDB_d} ${Tiger_d} \
				 ${TerichDB_r} ${LeveldbApi_r} ${DfaDB_r} ${TrbDB_r} ${Tiger_r}
	rm -rf ${TarBall}
	mkdir -p ${TarBall}/lib
	mkdir -p ${TarBall}/bin
	mkdir -p ${TarBall}/include/terark/terichdb
	mkdir -p ${TarBall}/include/terark/io
	mkdir -p ${TarBall}/include/terark/thread
	mkdir -p ${TarBall}/include/terark/util
	mkdir -p ${TarBall}/api/leveldb
ifeq (${PKG_WITH_DBG},1)
	cp -Ppa ../terark/${BUILD_ROOT}/lib/libterark-fsa-*d${DLL_SUFFIX} ${TarBall}/lib
	cp -Ppa ../terark/${BUILD_ROOT}/lib/libterark-core-*d${DLL_SUFFIX} ${TarBall}/lib
	cp -Ppa ${BUILD_ROOT}/lib/lib${TerichDB_lib}-*d${DLL_SUFFIX} ${TarBall}/lib
	cp -Ppa ${BUILD_ROOT}/lib/lib${DfaDB_lib}-*d${DLL_SUFFIX} ${TarBall}/lib
	cp -Ppa ${BUILD_ROOT}/lib/lib${TrbDB_lib}-*d${DLL_SUFFIX} ${TarBall}/lib
	cp -Ppa ${BUILD_ROOT}/lib/lib${Tiger_lib}-*d${DLL_SUFFIX} ${TarBall}/lib
endif
	$(MAKE) -C vs2015/terichdb/terichdb-schema-compile
	cp    vs2015/terichdb/terichdb-schema-compile/rls/*.exe ${TarBall}/bin
	cp -Ppa ../terark/${BUILD_ROOT}/lib/libterark-fsa-*r${DLL_SUFFIX} ${TarBall}/lib
	cp -Ppa ../terark/${BUILD_ROOT}/lib/libterark-core-*r${DLL_SUFFIX} ${TarBall}/lib
	cp -Ppa ${BUILD_ROOT}/lib/lib${TerichDB_lib}-*r${DLL_SUFFIX} ${TarBall}/lib
	cp -Ppa ${BUILD_ROOT}/lib/lib${DfaDB_lib}-*r${DLL_SUFFIX} ${TarBall}/lib
	cp -Ppa ${BUILD_ROOT}/lib/lib${TrbDB_lib}-*r${DLL_SUFFIX} ${TarBall}/lib
	cp -Ppa ${BUILD_ROOT}/lib/lib${Tiger_lib}-*r${DLL_SUFFIX} ${TarBall}/lib
	cp    src/terark/terichdb/db_conf.hpp           ${TarBall}/include/terark/terichdb
	cp    src/terark/terichdb/db_context.hpp        ${TarBall}/include/terark/terichdb
	cp    src/terark/terichdb/db_index.hpp          ${TarBall}/include/terark/terichdb
	cp    src/terark/terichdb/db_store.hpp          ${TarBall}/include/terark/terichdb
	cp    src/terark/terichdb/db_segment.hpp        ${TarBall}/include/terark/terichdb
	cp    src/terark/terichdb/db_dll_decl.hpp       ${TarBall}/include/terark/terichdb
	cp    src/terark/terichdb/db_table.hpp          ${TarBall}/include/terark/terichdb
	cp    terark-base/src/terark/*.hpp        ${TarBall}/include/terark
	cp    terark-base/src/terark/io/*.hpp     ${TarBall}/include/terark/io
	cp    terark-base/src/terark/thread/*.hpp ${TarBall}/include/terark/thread
	cp    terark-base/src/terark/util/*.hpp   ${TarBall}/include/terark/util
	cp -r api/leveldb/leveldb/include         ${TarBall}/api/leveldb
ifeq (${PKG_WITH_DEP},1)
	cp    /opt/include/wiredtiger.h           ${TarBall}/include/
	cp -r /opt/include/tbb                    ${TarBall}/include/
	cp -r /opt/include/boost                  ${TarBall}/include/
	cp -a /opt/lib/libwiredtiger-*.so*        ${TarBall}/lib/
	cp -a /opt/lib/libwiredtiger.so*          ${TarBall}/lib/
	cp -a /opt/lib/libwiredtiger_snappy.so*   ${TarBall}/lib/
  ifeq (Darwin,${UNAME_System})
	cp -a /opt/lib/libwiredtiger*.dylib*      ${TarBall}/lib/
  endif
	cp -a /opt/${COMPILER}/lib64/libtbb*${DLL_SUFFIX}*      ${TarBall}/lib/
	cp -a /opt/${COMPILER}/lib64/libboost_filesystem*${DLL_SUFFIX}*  ${TarBall}/lib/
	#cp -a /opt/${COMPILER}/lib64/libboost_date_time*${DLL_SUFFIX}*   ${TarBall}/lib/
	cp -a /opt/${COMPILER}/lib64/libboost_system*${DLL_SUFFIX}*      ${TarBall}/lib/
endif
	echo $(shell date "+%Y-%m-%d %H:%M:%S") > ${TarBall}/package.buildtime.txt
	echo $(shell git log | head -n1) >> ${TarBall}/package.buildtime.txt
	cd pkg; tar czf ${TarBallBaseName}.tgz ${TarBallBaseName}

ifeq (${WITH_BMI2},0)
terark-base/${BUILD_ROOT}/lib/libterark-core-${COMPILER}-d${DLL_SUFFIX} \
terark-base/${BUILD_ROOT}/lib/libterark-core-${COMPILER}-r${DLL_SUFFIX}:
	$(MAKE) -C terark-base core
endif

%${DLL_SUFFIX}:
	@echo "----------------------------------------------------------------------------------"
	@echo "Creating dynamic library: $@"
	@echo BOOST_INC=${BOOST_INC} BOOST_SUFFIX=${BOOST_SUFFIX}
	@echo -e "OBJS:" $(addprefix "\n  ",$(sort $(filter %.o,$^)))
	@echo -e "LIBS:" $(addprefix "\n  ",${LIBS})
	@mkdir -p ${BUILD_ROOT}/lib
	@rm -f $@
	@${LD} -shared $(sort $(filter %.o,$^)) ${LDFLAGS} ${LIBS} -o ${CYG_DLL_FILE} ${CYGWIN_LDFLAGS}
	cd $(dir $@); ln -sf $(notdir $@) $(subst -${COMPILER},,$(notdir $@))
ifeq (CYGWIN, ${UNAME_System})
	@cp -l -f ${CYG_DLL_FILE} /usr/bin
endif

%.a:
	@echo "----------------------------------------------------------------------------------"
	@echo "Creating static library: $@"
	@echo BOOST_INC=${BOOST_INC} BOOST_SUFFIX=${BOOST_SUFFIX}
	@echo -e "OBJS:" $(addprefix "\n  ",$(sort $(filter %.o,$^)))
	@echo -e "LIBS:" $(addprefix "\n  ",${LIBS})
	@mkdir -p ${BUILD_ROOT}/lib
	@rm -f $@
	@${AR} rcs $@ $(filter %.o,$^)
	cd $(dir $@); ln -sf $(notdir $@) $(subst -${COMPILER},,$(notdir $@))

.PHONY : install
install : TerichDB
	cp ${BUILD_ROOT}/lib/* ${prefix}/lib/

.PHONY : clean
clean:
	-rm -rf ${BUILD_ROOT} ${PRECOMPILED_HEADER_GCH}

.PHONY : depends
depends : ${alldep}

.PHONY : samples
samples : TerichDB
	${MAKE} -C samples/TerichDB/abstract_api

.PHONY : leveldb_test
leveldb_test: ${ddir}/api/leveldb/leveldb_test.exe

-include ${alldep}

${ddir}/%.exe: ${ddir}/%.o
	@echo Linking ... $@
	${LD} ${LDFLAGS} -o $@ $< -Llib -lterichdb-${COMPILER}-d ${LIB_TERARK_D} ${LIBS}

${rdir}/%.exe: ${ddir}/%.o
	@echo Linking ... $@
	${LD} ${LDFLAGS} -o $@ $< -Llib -lterichdb-${COMPILER}-r ${LIB_TERARK_R} ${LIBS}

${ddir}/%.o: %.cpp
	@echo file: $< "->" $@
	@echo TERARK_INC=${TERARK_INC}
	@echo BOOST_INC=${BOOST_INC} BOOST_SUFFIX=${BOOST_SUFFIX}
	mkdir -p $(dir $@)
	${CXX} ${CXX_STD} ${CPU} -c ${DBG_FLAGS} ${CXXFLAGS} ${INCS} $< -o $@

${rdir}/%.o: %.cpp
	@echo file: $< "->" $@
	@echo TERARK_INC=${TERARK_INC}
	@echo BOOST_INC=${BOOST_INC} BOOST_SUFFIX=${BOOST_SUFFIX}
	mkdir -p $(dir $@)
	${CXX} ${CXX_STD} ${CPU} -c ${RLS_FLAGS} ${CXXFLAGS} ${INCS} $< -o $@

${ddir}/%.o: %.cc
	@echo file: $< "->" $@
	@echo TERARK_INC=${TERARK_INC}
	@echo BOOST_INC=${BOOST_INC} BOOST_SUFFIX=${BOOST_SUFFIX}
	mkdir -p $(dir $@)
	${CXX} ${CXX_STD} ${CPU} -c ${DBG_FLAGS} ${CXXFLAGS} ${INCS} $< -o $@

${rdir}/%.o: %.cc
	@echo file: $< "->" $@
	@echo TERARK_INC=${TERARK_INC}
	@echo BOOST_INC=${BOOST_INC} BOOST_SUFFIX=${BOOST_SUFFIX}
	mkdir -p $(dir $@)
	${CXX} ${CXX_STD} ${CPU} -c ${RLS_FLAGS} ${CXXFLAGS} ${INCS} $< -o $@

${ddir}/%.o : %.c
	@echo file: $< "->" $@
	mkdir -p $(dir $@)
	${CC} -c ${CPU} ${DBG_FLAGS} ${CFLAGS} ${INCS} $< -o $@

${rdir}/%.o : %.c
	@echo file: $< "->" $@
	mkdir -p $(dir $@)
	${CC} -c ${CPU} ${RLS_FLAGS} ${CFLAGS} ${INCS} $< -o $@

${ddir}/%.s : %.cpp ${PRECOMPILED_HEADER_GCH}
	@echo file: $< "->" $@
	${CXX} -S ${CPU} ${DBG_FLAGS} ${CXXFLAGS} ${INCS} $< -o $@

${rdir}/%.s : %.cpp ${PRECOMPILED_HEADER_GCH}
	@echo file: $< "->" $@
	${CXX} -S ${CPU} ${RLS_FLAGS} ${CXXFLAGS} ${INCS} $< -o $@

${ddir}/%.s : %.c ${PRECOMPILED_HEADER_GCH}
	@echo file: $< "->" $@
	${CC} -S ${CPU} ${DBG_FLAGS} ${CXXFLAGS} ${INCS} $< -o $@

${rdir}/%.s : %.c ${PRECOMPILED_HEADER_GCH}
	@echo file: $< "->" $@
	${CC} -S ${CPU} ${RLS_FLAGS} ${CXXFLAGS} ${INCS} $< -o $@

${rdir}/%.dep : %.c
	@echo file: $< "->" $@
	@echo INCS = ${INCS}
	mkdir -p $(dir $@)
	${CC} -M -MT $(basename $@).o ${INCS} $< > $@

${ddir}/%.dep : %.c
	@echo file: $< "->" $@
	@echo INCS = ${INCS}
	mkdir -p $(dir $@)
	${CC} -M -MT $(basename $@).o ${INCS} $< > $@

${rdir}/%.dep : %.cpp
	@echo file: $< "->" $@
	@echo INCS = ${INCS}
	mkdir -p $(dir $@)
	${CXX} ${CXX_STD} -M -MT $(basename $@).o ${INCS} $< > $@

${ddir}/%.dep : %.cpp
	@echo file: $< "->" $@
	@echo INCS = ${INCS}
	mkdir -p $(dir $@)
	${CXX} ${CXX_STD} -M -MT $(basename $@).o ${INCS} $< > $@

