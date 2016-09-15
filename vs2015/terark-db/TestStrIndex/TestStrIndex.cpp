// TestStrIndex.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"

//#pragma pack(8)

struct A {
	int x, y;
	DATA_IO_LOAD_SAVE(A, &x&y)
};

struct B {
	short x[2], y[2];
	DATA_IO_LOAD_SAVE(B, &x&y)
};

struct C {
	A a;
	B b;
	DATA_IO_LOAD_SAVE(C, &a&b)
};

struct D {
	A a;
	B b;
	C c;
	DATA_IO_LOAD_SAVE(D, &a&b&c)
};

struct E {
	short x; // compiler will add paddings
	D d;
	DATA_IO_LOAD_SAVE(E, &x&d)
};

int main(int argc, char* argv[])
{
	using namespace terark;
	typedef NativeDataInput<MemIO> MemReader;
	typedef NativeDataOutput<AutoGrownMemIO> MemWriter;
	hash_strmap<int> baseIndex;
//	terark::db::dfadb::NestLoudsIndexBase dfaIndex;
	BOOST_STATIC_ASSERT((DataIO_is_dump<MemReader, int>::value));
	BOOST_STATIC_ASSERT((DataIO_is_dump<MemReader, std::pair<int,int> >::value));
	BOOST_STATIC_ASSERT((DataIO_is_dump<MemReader, A>::value) == true);
	BOOST_STATIC_ASSERT((DataIO_is_dump<MemReader, B>::value) == true);
	BOOST_STATIC_ASSERT((DataIO_is_dump<MemReader, C>::value) == true);
	BOOST_STATIC_ASSERT((DataIO_is_dump<MemReader, D>::value) == true);
	BOOST_STATIC_ASSERT((DataIO_is_dump<MemReader, E>::value) == false);
	BOOST_STATIC_ASSERT(DataIO_is_dump_by_sizeof(MemReader, A));
//	printf("sizeof(A+B+C) = %zd\n", sizeof(A) + sizeof(B) + sizeof(C));
//	printf("sizeof(D    ) = %zd\n", sizeof(D));
	MemWriter wr;
	wr << D();
    return 0;
}

