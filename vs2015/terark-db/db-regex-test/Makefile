
TERARK_HOME := ../../../terark-base
#SRCS := $(wildcard *.cpp)
#LIBS := -L../../../lib -lterark-db-${COMPILER_LAZY}-r
LIBS = -lboost_filesystem -lboost_date_time -lboost_system
INCS = -I../../../src
CHECK_TERARK_FSA_LIB_UPDATE := 0

include $(shell dirname $(shell readlink $(lastword ${MAKEFILE_LIST})))/Makefile.common

${BINS_D} : LIBS := -L../../../lib -lterark-db-${COMPILER_LAZY}-d ${LIBS}
${BINS_R} : LIBS := -L../../../lib -lterark-db-${COMPILER_LAZY}-r ${LIBS}

