#!/bin/bash
#set -x

CXX=$1
COMPILER=$2
EnvConf=$3
echo COMPILER=$COMPILER 1>&2

#EnvConf=Make.env.conf-${COMPILER}

rm -f $EnvConf
mkdir -p `dirname $EnvConf`

hasboost=0
if [ -z "$BOOST_INC" ]; then
	boost_prefix=""
else
	boost_prefix=`dirname $BOOST_INC`
fi
for dir in "$boost_prefix" /usr /usr/local /opt $HOME $HOME/opt
do
	vf=${dir}/include/boost/version.hpp
#	echo dir=$dir >&2
	if test -s $vf; then
		if test -d ${dir}/lib32; then
			DIR_LIB32=${dir}/lib32
		else
			DIR_LIB32=${dir}/lib
		fi
		if test -d ${dir}/lib64; then
			DIR_LIB64=${dir}/lib64
		else
			DIR_LIB64=${dir}/lib
		fi
		WORD_BITS=`uname -m | sed 's/.*_\(64\|32\)/\1/'`
		DIR_LIB="DIR_LIB${WORD_BITS}"
		BOOST_VERSION=`sed -n '/define\s\+BOOST_VERSION/s/^.*BOOST_VERSION\s\+\([0-9]*\).*/\1/p' $vf`
		BOOST_LIB_VERSION=`sed -n '/define\s\+BOOST_LIB_VERSION/s/.*BOOST_LIB_VERSION[^"]*"\([0-9_.]*\)".*/\1/p' $vf`
		if test -z "$BOOST_SUFFIX"; then
			for lib in $DIR_LIB32 $DIR_LIB64; do
				for suf in \
						   -${COMPILER}-mt-d-${BOOST_LIB_VERSION}.a \
						   -${COMPILER}-mt-d-${BOOST_LIB_VERSION}.so \
						   -${COMPILER}-mt-d-${BOOST_LIB_VERSION}.so \
						   -mt.so \
						   -mt.so.5 \
						   -mt.so.6 \
						   ".a"
				do
					if test -e $lib/libboost_thread$suf -a -z "$BOOST_SUFFIX"; then
						BOOST_SUFFIX=$suf
					fi
				done
			done
			BOOST_SUFFIX=${BOOST_SUFFIX%.a*}
			BOOST_SUFFIX=${BOOST_SUFFIX%.so*}
		fi
cat >> $EnvConf <<- EOF
	WORD_BITS := ${WORD_BITS}
	DIR_LIB32 := ${DIR_LIB32}
	DIR_LIB64 := ${DIR_LIB64}
	DIR_LIB   := `eval 'echo ${'$DIR_LIB'}'`
	BOOST_LIB_VERSION := ${BOOST_LIB_VERSION}
	BOOST_INC := ${dir}/include
	BOOST_LIB := ${dir}/lib64
	BOOST_SUFFIX := ${BOOST_SUFFIX}
EOF
		if test "${COMPILER%-*}" = gcc && expr "${COMPILER#*-} < 4.7"; then
			if test ${BOOST_VERSION} -lt 104900; then
				echo BOOST_VERSION=${BOOST_VERSION} will wrongly define BOOST_DISABLE_THREADS for ${COMPILER} >&2
			fi
		fi
		hasboost=1
		break
	fi
done

if [ $hasboost -eq 0 ]; then
	echo $'\33[31m\33[1mFATAL: can not find boost\33[0m' 1>&2
	exit 1
fi

cat > is_cygwin.cpp << "EOF"
#include <stdio.h>
int main() {
  #ifdef __CYGWIN__
    printf("1");
  #else
    printf("0");
  #endif
    return 0;
}
EOF
if $CXX is_cygwin.cpp -o is_cygwin.exe; then
	IS_CYGWIN=`./is_cygwin.exe`
	echo IS_CYGWIN=$IS_CYGWIN >> $EnvConf
fi
rm -f is_cygwin.*

cat > has_inheriting_cons.cpp << "EOF"
struct A {
	A(int) {}
	A(int,int){}
};
struct B : public A {
	using A::A;
};
int main() {
	B b1(111);
	B b2(2,2);
	return 0;
}
EOF
rm -f src/terark/my_auto_config.hpp
touch src/terark/my_auto_config.hpp
if $CXX -std=c++11 has_inheriting_cons.cpp > /dev/null 2>&1; then
	echo '#define TERARK_HAS_INHERITING_CONSTRUCTORS' >> src/terark/my_auto_config.hpp
fi
rm -f has_inheriting_cons.cpp

if [ "$IS_CYGWIN" -eq 1 ]; then
	rm -f a.exe
else
	rm -f a.out
fi

