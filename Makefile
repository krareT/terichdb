DBG_FLAGS ?= -g3 -D_DEBUG
RLS_FLAGS ?= -O3 -DNDEBUG

ifeq "$(origin LD)" "default"
  LD := ${CXX}
endif

#COMPILER := $(shell ${CXX} --version | head -1 | awk '{split($$3, Ver, "."); printf("%s-%d.%d", $$1, Ver[1], Ver[2]);}')
# Makefile is stupid to parsing $(shell echo ')')
#COMPILER := $(shell ${CXX} --version | head -1 | sed 's/\(\S\+\)\s\+([^()]*)\s\+\([0-9]\+.[0-9]\+\).*/\1-\2/')
COMPILER := $(shell ${CXX} tools/configure/compiler.cpp -o a && ./a && rm -f a a.exe)
#$(error COMPILER=${COMPILER})
UNAME_MachineSystem := $(shell uname -m -s | sed 's:[ /]:-:g')
BUILD_ROOT := build/${COMPILER}-${UNAME_MachineSystem}
ddir:=${BUILD_ROOT}/dbg
rdir:=${BUILD_ROOT}/rls

gen_sh := $(dir $(lastword ${MAKEFILE_LIST}))gen_env_conf.sh

err := $(shell env BDB_HOME=${BDB_HOME} BOOST_INC=${BOOST_INC} bash ${gen_sh} "${CXX}" ${COMPILER} ${BUILD_ROOT}/env.mk; echo $$?)
ifneq "${err}" "0"
   $(error err = ${err} MAKEFILE_LIST = ${MAKEFILE_LIST}, PWD = ${PWD}, gen_sh = ${gen_sh} "${CXX}" ${COMPILER} ${BUILD_ROOT}/env.mk)
endif

BRAIN_DEAD_RE2_INC = -I3rdparty/re2/re2 -I3rdparty/re2/util

FEBIRD_INC := -Isrc -I3rdparty/re2 ${BRAIN_DEAD_RE2_INC} -I../nark/src

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

ifeq (WITH_BMI2,1)
	CPU += -mbmi -mbmi2
endif

COMMON_C_FLAGS  += -Wformat=2 -Wcomment
COMMON_C_FLAGS  += -Wall -Wextra
COMMON_C_FLAGS  += -Wno-unused-parameter
COMMON_C_FLAGS  += -D_GNU_SOURCE # For cygwin

COMMON_C_FLAGS  += -DNO_THREADS # Workaround re2

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

override INCS := ${FEBIRD_INC} ${INCS}

ifeq (, $(findstring ${BOOST_INC}, ${INCS} /usr/include /usr/local/include))
  override INCS += -I${BOOST_INC}
endif

ifeq (, $(findstring ${BOOST_LIB}, /usr/lib64 /usr/lib /usr/local/lib))
  override LIBS += -L${BOOST_LIB}
endif

#override INCS += -I/usr/include

LIBBOOST ?= \
	  -lboost_thread${BOOST_SUFFIX} \
	  -lboost_date_time${BOOST_SUFFIX} \
	  -lboost_system${BOOST_SUFFIX}

ifeq "1" "0"
ifeq ($(shell test -d /usr/local/lib64 && echo 1),1)
  LIBS += -L/usr/local/lib64
endif
ifeq ($(shell test -d /usr/local/lib && echo 1),1)
  LIBS += -L/usr/local/lib
endif
ifeq ($(shell test -d /usr/lib64 && echo 1),1)
  LIBS += -L/usr/lib64
endif
ifeq ($(shell test -d /usr/lib && echo 1),1)
  LIBS += -L/usr/lib
endif
endif

#LIBS += -ldl
#LIBS += -lpthread
#LIBS += ${LIBBOOST}

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
LIBS += -L${BDB_HOME}/lib
LIBS += -L/opt/lib

ifeq (, ${prefix})
	ifeq (root, ${USER})
		prefix := /usr
	else
		prefix := /home/${USER}
	endif
endif

#$(warning prefix=${prefix} BDB_HOME=${BDB_HOME}: LIBS=${LIBS})

#obsoleted_src =  \
#	$(wildcard src/obsoleted/nark/thread/*.cpp) \
#	$(wildcard src/obsoleted/nark/thread/posix/*.cpp) \
#	$(wildcard src/obsoleted/wordseg/*.cpp)
#LIBS += -liconv

NarkDB_src := $(wildcard src/nark/db/*.cpp)
NarkDB_src += $(wildcard src/nark/db/wiredtiger/*.cpp)

ifeq (1,${WITH_DFA_DB})
NarkDB_src += $(wildcard src/nark/db/dfadb/*.cpp)
endif

#function definition
#@param:${1} -- targets var prefix, such as bdb_util | core
#@param:${2} -- build type: d | r
objs = $(addprefix ${${2}dir}/, $(addsuffix .o, $(basename ${${1}_src})))

NarkDB_d_o := $(call objs,NarkDB,d)
NarkDB_r_o := $(call objs,NarkDB,r)
NarkDB_d := lib/libnark-NarkDB-${COMPILER}-d${DLL_SUFFIX}
NarkDB_r := lib/libnark-NarkDB-${COMPILER}-r${DLL_SUFFIX}
static_NarkDB_d := lib/libnark-NarkDB-${COMPILER}-d.a
static_NarkDB_r := lib/libnark-NarkDB-${COMPILER}-r.a

ALL_TARGETS = ${MAYBE_DBB_DBG} ${MAYBE_DBB_RLS} NarkDB
DBG_TARGETS = ${MAYBE_DBB_DBG} ${NarkDB_d}
RLS_TARGETS = ${MAYBE_DBB_RLS} ${NarkDB_r}

.PHONY : default all NarkDB

default : NarkDB
all : ${ALL_TARGETS}
NarkDB: ${NarkDB_d} ${NarkDB_r} ${static_NarkDB_d} ${static_NarkDB_r}

allsrc = ${NarkDB_src}
alldep = $(addprefix ${rdir}/, $(addsuffix .dep, $(basename ${allsrc}))) \
         $(addprefix ${ddir}/, $(addsuffix .dep, $(basename ${allsrc})))

.PHONY : dbg rls
dbg: ${DBG_TARGETS}
rls: ${RLS_TARGETS}

ifneq (${UNAME_System},Darwin)
${NarkDB_d} ${NarkDB_r} : LIBS += -lrt
endif

${NarkDB_d} : LIBS += -L../nark/lib -lnark-${COMPILER}-d -ltbb_debug
${NarkDB_r} : LIBS += -L../nark/lib -lnark-${COMPILER}-r -ltbb

${NarkDB_d} ${NarkDB_r} : LIBS += -lpthread

${NarkDB_d} : $(call objs,NarkDB,d)
${NarkDB_r} : $(call objs,NarkDB,r)
${static_NarkDB_d} : $(call objs,NarkDB,d)
${static_NarkDB_r} : $(call objs,NarkDB,r)

TarBall := pkg/narkdb-${UNAME_MachineSystem}-${COMPILER}
.PHONY : pkg
pkg: ${NarkDB_d} ${NarkDB_r}
	rm -rf ${TarBall}
	mkdir -p ${TarBall}/lib
ifeq (${PKG_WITH_DBG},1)
	cp    ${NarkDB_d} ${TarBall}/lib
	ln -s libnark-NarkDB-${COMPILER}-d${DLL_SUFFIX} ${TarBall}/lib/libnark-NarkDB-d${DLL_SUFFIX}
endif
	cp    ${NarkDB_r} ${TarBall}/lib
	ln -s libnark-NarkDB-${COMPILER}-r${DLL_SUFFIX} ${TarBall}/lib/libnark-NarkDB-r${DLL_SUFFIX}
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
install : NarkDB
	cp lib/* ${prefix}/lib/

.PHONY : clean
clean:
	-rm -rf lib/libnark* ${BUILD_ROOT} ${PRECOMPILED_HEADER_GCH}

.PHONY : depends
depends : ${alldep}

.PHONY : samples
samples : NarkDB
	${MAKE} -C samples/NarkDB/abstract_api

-include ${alldep}

${ddir}/%.o: %.cpp
	@echo file: $< "->" $@
	@echo FEBIRD_INC=${FEBIRD_INC}
	@echo BOOST_INC=${BOOST_INC} BOOST_SUFFIX=${BOOST_SUFFIX}
	mkdir -p $(dir $@)
	${CXX} ${CXX_STD} ${CPU} -c ${DBG_FLAGS} ${CXXFLAGS} ${INCS} $< -o $@

${rdir}/%.o: %.cpp
	@echo file: $< "->" $@
	@echo FEBIRD_INC=${FEBIRD_INC}
	@echo BOOST_INC=${BOOST_INC} BOOST_SUFFIX=${BOOST_SUFFIX}
	mkdir -p $(dir $@)
	${CXX} ${CXX_STD} ${CPU} -c ${RLS_FLAGS} ${CXXFLAGS} ${INCS} $< -o $@

${ddir}/%.o: %.cc
	@echo file: $< "->" $@
	@echo FEBIRD_INC=${FEBIRD_INC}
	@echo BOOST_INC=${BOOST_INC} BOOST_SUFFIX=${BOOST_SUFFIX}
	mkdir -p $(dir $@)
	${CXX} ${CXX_STD} ${CPU} -c ${DBG_FLAGS} ${CXXFLAGS} ${INCS} $< -o $@

${rdir}/%.o: %.cc
	@echo file: $< "->" $@
	@echo FEBIRD_INC=${FEBIRD_INC}
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
	${CXX} -M -MT $(basename $@).o ${INCS} $< > $@

${ddir}/%.dep : %.cpp
	@echo file: $< "->" $@
	@echo INCS = ${INCS}
	mkdir -p $(dir $@)
	${CXX} -M -MT $(basename $@).o ${INCS} $< > $@

