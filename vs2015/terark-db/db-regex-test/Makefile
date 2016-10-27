CHECK_TERARK_LIB_UPDATE ?= 1
DB_HOME ?= ../../..
CORE_HOME ?= ../../../terark-base
WITH_BMI2 ?= 0

ifeq "$(origin CXX)" "default"
  ifeq "$(shell test -e /opt/bin/g++ && echo 1)" "1"
    CXX := /opt/bin/g++
  else
    ifeq "$(shell test -e ${HOME}/opt/bin/g++ && echo 1)" "1"
      CXX := ${HOME}/opt/bin/g++
    endif
  endif
endif

ifeq "$(origin LD)" "default"
  LD := ${CXX}
endif

override INCS := -I${DB_HOME}/src -I${CORE_HOME}/src ${INCS}
#override CXXFLAGS += -pipe
override CXXFLAGS += -Wall -Wextra
override CXXFLAGS += -Wno-unused-parameter
override CXXFLAGS += -D_GNU_SOURCE
override CXXFLAGS += -D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE
#override CXXFLAGS += -Wno-unused-variable
#CXXFLAGS += -Wconversion -Wno-sign-conversion

#override CXXFLAGS += -Wfatal-errors

override CXXFLAGS += -DNO_THREADS # Workaround re2

override LIBS := -lboost_filesystem -lboost_system

ifeq ($(shell uname), Linux)
  override LIBS += -lrt
endif

COMPILER := $(shell ${CXX} ${DB_HOME}/tools/configure/compiler.cpp -o a.exe && ./a.exe && rm -f a.exe)
UNAME_MachineSystem := $(shell uname -m -s | sed 's:[ /]:-:g')
UNAME_System := $(shell uname | sed 's/^\([0-9a-zA-Z]*\).*/\1/')
COMPILER_LAZY = ${COMPILER}

ifeq "$(shell a=${COMPILER};echo $${a:0:5})" "clang"
  override CXXFLAGS += -fcolor-diagnostics
endif

ifeq "$(shell a=${COMPILER};echo $${a:0:3})" "g++"
  ifeq ($(shell uname), Darwin)
    override CXXFLAGS += -Wa,-q
  endif
  override CXXFLAGS += -time
#  override CXXFLAGS += -fmax-errors=5
  #override CXXFLAGS += -fmax-errors=2
endif

# icc or icpc
ifeq "$(shell a=${COMPILER};echo $${a:0:2})" "ic"
  override CXXFLAGS += -xHost -fasm-blocks
else
  override CXXFLAGS += -march=native
endif
ifeq (${WITH_BMI2},1)
  override CXXFLAGS += -mbmi -mbmi2
endif

ifeq "$(shell a=${COMPILER};echo $${a:0:3})" "g++"
  ifeq (Linux, ${UNAME_System})
    override LDFLAGS += -rdynamic
  endif
  override CXXFLAGS += -time
  ifeq "$(shell echo ${COMPILER} | awk -F- '{if ($$2 >= 4.8) print 1;}')" "1"
    CXX_STD := -std=gnu++1y
  endif
endif

ifeq "${CXX_STD}" ""
  CXX_STD := -std=gnu++11
endif

override CXXFLAGS += ${CXX_STD}

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
#override CXXFLAGS += ${FPIC}

BUILD_NAME := ${UNAME_MachineSystem}-${COMPILER}-bmi2-${WITH_BMI2}
BUILD_ROOT := build/${BUILD_NAME}
DB_LIB_DIR := ${DB_HOME}/${BUILD_ROOT}/lib
CORE_LIB_DIR := ${CORE_HOME}/${BUILD_ROOT}/lib

DBG_DIR := ${BUILD_ROOT}/dbg
RLS_DIR := ${BUILD_ROOT}/rls

SRCS ?= $(wildcard *.cpp)
OBJS_R := $(addprefix ${RLS_DIR}/, $(addsuffix .o, $(basename ${SRCS})))
OBJS_D := $(addprefix ${DBG_DIR}/, $(addsuffix .o ,$(basename ${SRCS})))
BINS_D := $(addsuffix .exe ,$(basename ${OBJS_D}))
BINS_R := $(addsuffix .exe ,$(basename ${OBJS_R}))

DLL_SRCS += $(wildcard *.cxx)
DLL_OBJS_R := $(addprefix ${RLS_DIR}/, $(addsuffix .o, $(basename ${DLL_SRCS})))
DLL_OBJS_D := $(addprefix ${DBG_DIR}/, $(addsuffix .o ,$(basename ${DLL_SRCS})))
DLL_BINS_D := $(addsuffix ${DLL_SUFFIX} ,$(basename ${DLL_OBJS_D}))
DLL_BINS_R := $(addsuffix ${DLL_SUFFIX} ,$(basename ${DLL_OBJS_R}))

ext_ldflags = $(strip $(shell sed -n 's,.*//Makefile\s*:\s*LDFLAGS\s*:\s*\(.*\),\1,p' $(subst .exe,.cpp,$(subst ${RLS_DIR}/,,$(subst ${DBG_DIR}/,,$@)))))
ext_cxxflags = $(strip $(shell sed -n 's,.*//Makefile\s*:\s*CXXFLAGS\s*:\s*\(.*\),\1,p' $<))

.PHONY : all clean link

all : ${BINS_D} ${BINS_R} ${OBJS_D} ${OBJS_R} link \
	${DLL_OBJS_D} ${DLL_OBJS_R} ${DLL_BINS_D} ${DLL_BINS_R}

link : ${BINS_D} ${BINS_R} ${DLL_BINS_D} ${DLL_BINS_R}
	mkdir -p dbg; cd dbg; \
	for f in `find ../${DBG_DIR} -name '*.exe' -o -name '*'${DLL_SUFFIX}`; do \
		ln -sf $$f .; \
	done; cd ..
	mkdir -p rls; cd rls; \
	for f in `find ../${RLS_DIR} -name '*.exe' -o -name '*'${DLL_SUFFIX}`; do \
		ln -sf $$f .; \
	done; cd ..

ifeq (${STATIC},1)
ifeq (${CHECK_TERARK_LIB_UPDATE},1)
${BINS_D} : ${DB_LIB_DIR}/libterark-db-${COMPILER}-d.a ${CORE_LIB_DIR}/libterark-core-${COMPILER}-d.a
${BINS_R} : ${DB_LIB_DIR}/libterark-db-${COMPILER}-r.a ${CORE_LIB_DIR}/libterark-core-${COMPILER}-r.a
endif
  ifeq (Darwin, ${UNAME_System})
${BINS_D} : LIBS := ${CORE_LIB_DIR}/libterark-core-${COMPILER}-d.a ${LIBS}
${BINS_R} : LIBS := ${CORE_LIB_DIR}/libterark-core-${COMPILER}-r.a ${LIBS}
  else
${BINS_D} : LIBS := -Wl,--whole-archive ${CORE_LIB_DIR}/libterark-core-${COMPILER}-d.a -Wl,--no-whole-archive -ldivsufsort-d ${LIBS}
${BINS_R} : LIBS := -Wl,--whole-archive ${CORE_LIB_DIR}/libterark-core-${COMPILER}-r.a -Wl,--no-whole-archive -ldivsufsort-r ${LIBS}
  endif
else
ifeq (${CHECK_TERARK_LIB_UPDATE},1)
${BINS_D} : ${DB_LIB_DIR}/libterark-db-${COMPILER}-d${DLL_SUFFIX} ${CORE_LIB_DIR}/libterark-core-${COMPILER}-d${DLL_SUFFIX}
${BINS_R} : ${DB_LIB_DIR}/libterark-db-${COMPILER}-r${DLL_SUFFIX} ${CORE_LIB_DIR}/libterark-core-${COMPILER}-r${DLL_SUFFIX}
endif
${BINS_D} : LIBS := -L${DB_LIB_DIR} -lterark-db-${COMPILER}-d -L${CORE_HOME}/lib -lterark-core-${COMPILER}-d ${LIBS}
${BINS_R} : LIBS := -L${DB_LIB_DIR} -lterark-db-${COMPILER}-r -L${CORE_HOME}/lib -lterark-core-${COMPILER}-r ${LIBS}
endif

clean :
	rm -rf ${BUILD_ROOT} dbg rls

${DBG_DIR}/%.o : %.cpp
	@mkdir -p $(dir $@)
	${CXX} -O0 -g3 -c ${INCS} ${CXXFLAGS} -o $@ $< $(ext_cxxflags)

#${RLS_DIR}/%.o : CXXFLAGS += -funsafe-loop-optimizations -fgcse-sm -fgcse-las -fgcse-after-reload
${RLS_DIR}/%.o : %.cpp
	@mkdir -p $(dir $@)
	${CXX} -Ofast -c ${INCS} ${CXXFLAGS} -o $@ $< $(ext_cxxflags) -DNDEBUG

${DBG_DIR}/%.o : %.cxx
	@mkdir -p $(dir $@)
	${CXX} -O0 -g3 -c ${INCS} ${CXXFLAGS} -o $@ $< $(ext_cxxflags) ${FPIC}

${RLS_DIR}/%.o : %.cxx
	@mkdir -p $(dir $@)
	${CXX} -Ofast  -c ${INCS} ${CXXFLAGS} -o $@ $< $(ext_cxxflags) ${FPIC} -DNDEBUG

%.exe : %.o
	@echo Linking ... $@
	${LD} ${LDFLAGS} -o $@ $< ${LIBS} $(ext_ldflags)

%${DLL_SUFFIX}: %.o
	@echo "----------------------------------------------------------------------------------"
	@echo "Creating dynamic library: $@"
	@echo BOOST_INC=${BOOST_INC} BOOST_SUFFIX=${BOOST_SUFFIX}
	@echo -e "OBJS:" $(addprefix "\n  ",$(sort $(filter %.o,$^)))
	@echo -e "LIBS:" $(addprefix "\n  ",${LIBS})
	@rm -f $@
	@rm -f $(subst -${COMPILER},, $@)
	@${LD} -shared $(sort $(filter %.o,$^)) ${LDFLAGS} ${LIBS} -o ${CYG_DLL_FILE} ${CYGWIN_LDFLAGS}
ifeq (CYGWIN, ${UNAME_System})
	@cp -l -f ${CYG_DLL_FILE} /usr/bin
endif
