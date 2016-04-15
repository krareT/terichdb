DBG_FLAGS ?= -g3 -D_DEBUG
RLS_FLAGS ?= -O3 -DNDEBUG
WITH_BMI2 ?= $(shell ./cpu_has_bmi2.sh)

ifeq "$(origin LD)" "default"
  LD := ${CXX}
endif

#COMPILER := $(shell ${CXX} --version | head -1 | awk '{split($$3, Ver, "."); printf("%s-%d.%d", $$1, Ver[1], Ver[2]);}')
# Makefile is stupid to parsing $(shell echo ')')
#COMPILER := $(shell ${CXX} --version | head -1 | sed 's/\(\S\+\)\s\+([^()]*)\s\+\([0-9]\+.[0-9]\+\).*/\1-\2/')
COMPILER := $(shell ${CXX} tools/configure/compiler.cpp -o a && ./a && rm -f a a.exe)
#$(error COMPILER=${COMPILER})
UNAME_MachineSystem := $(shell uname -m -s | sed 's:[ /]:-:g')
BUILD_ROOT := build/${COMPILER}-${UNAME_MachineSystem}-bmi2-${WITH_BMI2}
ddir:=${BUILD_ROOT}/dbg
rdir:=${BUILD_ROOT}/rls

gen_sh := $(dir $(lastword ${MAKEFILE_LIST}))gen_env_conf.sh

err := $(shell env BDB_HOME=${BDB_HOME} BOOST_INC=${BOOST_INC} bash ${gen_sh} "${CXX}" ${COMPILER} ${BUILD_ROOT}/env.mk; echo $$?)
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
  FPIC = -fPIC
  DLL_SUFFIX = .so
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
override CFLAGS   += ${DEFS}
override CXXFLAGS += ${DEFS}

override INCS := ${TERARK_INC} ${INCS}

ifeq (, $(findstring ${BOOST_INC}, ${INCS} /usr/include /usr/local/include))
  override INCS += -I${BOOST_INC}
endif

ifeq (, $(findstring ${BOOST_LIB}, /usr/lib64 /usr/lib /usr/local/lib))
  override LIBS += -L${BOOST_LIB}
endif

#override INCS += -I/usr/include

LIBBOOST ?= \
	  -lboost_filesystem${BOOST_SUFFIX} \
	  -lboost_date_time${BOOST_SUFFIX} \
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
override LIBS += -lwiredtiger

#extf = -pie
extf = -fno-stack-protector
#extf+=-fno-stack-protector-all
override CFLAGS += ${extf}
#override CFLAGS += -g3
override CXXFLAGS += ${extf}
#override CXXFLAGS += -g3
#CXXFLAGS += -fnothrow-opt

override INCS += -I${BDB_HOME}/include
override INCS += -I/opt/include
override LIBS += -L${BDB_HOME}/lib
override LIBS += -L/opt/lib

ifeq (, ${prefix})
	ifeq (root, ${USER})
		prefix := /usr
	else
		prefix := /home/${USER}
	endif
endif

TerarkDB_src := $(wildcard src/terark/db/*.cpp)
TerarkDB_src += $(wildcard src/terark/db/wiredtiger/*.cpp)

ifeq (1,${WITH_DFA_DB})
  TerarkDB_src += $(wildcard src/terark/db/dfadb/*.cpp)
  override INCS += -I../terark/src
  TerarkDB_lib := libterark-db
  LIB_TERARK_D := -L../terark/lib -lterark-fsa_all-${COMPILER}-d
  LIB_TERARK_R := -L../terark/lib -lterark-fsa_all-${COMPILER}-r
else
  override INCS += -Iterark-base/src
  zip_src := \
    terark-base/src/terark/io/BzipStream.cpp \
	terark-base/src/terark/io/GzipStream.cpp
  TerarkDB_src += $(wildcard terark-base/src/terark/*.cpp)
  TerarkDB_src += $(wildcard terark-base/src/terark/io/*.cpp)
  TerarkDB_src += $(wildcard terark-base/src/terark/util/*.cpp)
  TerarkDB_src += $(wildcard terark-base/src/terark/thread/*.cpp)
  TerarkDB_src := $(filter-out ${zip_src}, ${TerarkDB_src})
  TerarkDB_lib := libterark-db-no-dfadb
endif

#function definition
#@param:${1} -- targets var prefix, such as bdb_util | core
#@param:${2} -- build type: d | r
objs = $(addprefix ${${2}dir}/, $(addsuffix .o, $(basename ${${1}_src})))

TerarkDB_d_o := $(call objs,TerarkDB,d)
TerarkDB_r_o := $(call objs,TerarkDB,r)
TerarkDB_d := lib/${TerarkDB_lib}-${COMPILER}-d${DLL_SUFFIX}
TerarkDB_r := lib/${TerarkDB_lib}-${COMPILER}-r${DLL_SUFFIX}
static_TerarkDB_d := lib/${TerarkDB_lib}-${COMPILER}-d.a
static_TerarkDB_r := lib/${TerarkDB_lib}-${COMPILER}-r.a

ALL_TARGETS = ${MAYBE_DBB_DBG} ${MAYBE_DBB_RLS} TerarkDB
DBG_TARGETS = ${MAYBE_DBB_DBG} ${TerarkDB_d}
RLS_TARGETS = ${MAYBE_DBB_RLS} ${TerarkDB_r}

.PHONY : default all TerarkDB

default : TerarkDB
all : ${ALL_TARGETS}
TerarkDB: ${TerarkDB_d} ${TerarkDB_r} ${static_TerarkDB_d} ${static_TerarkDB_r}

allsrc = ${TerarkDB_src}
alldep = $(addprefix ${rdir}/, $(addsuffix .dep, $(basename ${allsrc}))) \
         $(addprefix ${ddir}/, $(addsuffix .dep, $(basename ${allsrc})))

.PHONY : dbg rls
dbg: ${DBG_TARGETS}
rls: ${RLS_TARGETS}

ifneq (${UNAME_System},Darwin)
${TerarkDB_d} ${TerarkDB_r} : LIBS += -lrt
endif

#${TerarkDB_d} : override LIBS += ${LIB_TERARK_D} -ltbb_debug
${TerarkDB_d} : override LIBS += ${LIB_TERARK_D} -ltbb
${TerarkDB_r} : override LIBS += ${LIB_TERARK_R} -ltbb

${TerarkDB_d} ${TerarkDB_r} : LIBS += -lpthread

${TerarkDB_d} : $(call objs,TerarkDB,d)
${TerarkDB_r} : $(call objs,TerarkDB,r)
${static_TerarkDB_d} : $(call objs,TerarkDB,d)
${static_TerarkDB_r} : $(call objs,TerarkDB,r)

TarBall := pkg/${TerarkDB_lib}-${UNAME_MachineSystem}-${COMPILER}-bmi2-${WITH_BMI2}
.PHONY : pkg
pkg: ${TerarkDB_d} ${TerarkDB_r}
	rm -rf ${TarBall}
	mkdir -p ${TarBall}/lib
	mkdir -p ${TarBall}/bin
	mkdir -p ${TarBall}/include/terark/db
	mkdir -p ${TarBall}/include/terark/io
	mkdir -p ${TarBall}/include/terark/thread
	mkdir -p ${TarBall}/include/terark/util
ifeq (${PKG_WITH_DBG},1)
	cp    ${TerarkDB_d} ${TarBall}/lib
	ln -s ${TerarkDB_lib}-${COMPILER}-d${DLL_SUFFIX} ${TarBall}/lib/${TerarkDB_lib}-d${DLL_SUFFIX}
endif
	$(MAKE) -C vs2015/terark-db/terark-db-schema-compile
	cp    vs2015/terark-db/terark-db-schema-compile/rls/*.exe ${TarBall}/bin
	cp    ${TerarkDB_r} ${TarBall}/lib
	cp    src/terark/db/db_conf.hpp           ${TarBall}/include/terark/db
	cp    src/terark/db/db_context.hpp        ${TarBall}/include/terark/db
	cp    src/terark/db/db_index.hpp          ${TarBall}/include/terark/db
	cp    src/terark/db/db_store.hpp          ${TarBall}/include/terark/db
	cp    src/terark/db/db_segment.hpp        ${TarBall}/include/terark/db
	cp    src/terark/db/db_table.hpp          ${TarBall}/include/terark/db
	cp    terark-base/src/terark/*.hpp        ${TarBall}/include/terark
	cp    terark-base/src/terark/io/*.hpp     ${TarBall}/include/terark/io
	cp    terark-base/src/terark/thread/*.hpp ${TarBall}/include/terark/thread
	cp    terark-base/src/terark/util/*.hpp   ${TarBall}/include/terark/util
	ln -s ${TerarkDB_lib}-${COMPILER}-r${DLL_SUFFIX} ${TarBall}/lib/${TerarkDB_lib}-r${DLL_SUFFIX}
	echo $(shell date "+%Y-%m-%d %H:%M:%S") > ${TarBall}/package.buildtime.txt
	echo $(shell git log | head -n1) >> ${TarBall}/package.buildtime.txt
	tar czf ${TarBall}.tgz ${TarBall}


%${DLL_SUFFIX}:
	@echo "----------------------------------------------------------------------------------"
	@echo "Creating dynamic library: $@"
	@echo BOOST_INC=${BOOST_INC} BOOST_SUFFIX=${BOOST_SUFFIX}
	@echo -e "OBJS:" $(addprefix "\n  ",$(sort $(filter %.o,$^)))
	@echo -e "LIBS:" $(addprefix "\n  ",${LIBS})
	@mkdir -p lib
	@rm -f $@
	@rm -f $(subst -${COMPILER},, $@)
	@ln -sf $(notdir $@) $(subst -${COMPILER},, $@)
	@${LD} -shared $(sort $(filter %.o,$^)) ${LDFLAGS} ${LIBS} -o ${CYG_DLL_FILE} ${CYGWIN_LDFLAGS}
ifeq (CYGWIN, ${UNAME_System})
	@cp -l -f ${CYG_DLL_FILE} /usr/bin
endif

%.a:
	@echo "----------------------------------------------------------------------------------"
	@echo "Creating static library: $@"
	@echo BOOST_INC=${BOOST_INC} BOOST_SUFFIX=${BOOST_SUFFIX}
	@echo -e "OBJS:" $(addprefix "\n  ",$(sort $(filter %.o,$^)))
	@echo -e "LIBS:" $(addprefix "\n  ",${LIBS})
	@mkdir -p lib
	@rm -f $(subst -${COMPILER},, $@)
	@ln -sf $(notdir $@) $(subst -${COMPILER},, $@)
	@${AR} rcs $@ $(filter %.o,$^)

.PHONY : install
install : TerarkDB
	cp lib/* ${prefix}/lib/

.PHONY : clean
clean:
	-rm -rf lib ${BUILD_ROOT} ${PRECOMPILED_HEADER_GCH}

.PHONY : depends
depends : ${alldep}

.PHONY : samples
samples : TerarkDB
	${MAKE} -C samples/TerarkDB/abstract_api

-include ${alldep}

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

