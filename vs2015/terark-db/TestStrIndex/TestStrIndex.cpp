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

struct BS1 {
	char x, y;
	DATA_IO_LOAD_SAVE(BS1, &x&y)
};
struct BS2 {
	BS1 x;
	BS1 y;
	DATA_IO_LOAD_SAVE(BS2, &x&y)
};
struct BS3 {
	BS2 x;
	unsigned char y[4];
	DATA_IO_LOAD_SAVE(BS3, &x&y)
};
struct ComplexObj {
	std::vector<int> x;
	DATA_IO_LOAD_SAVE(ComplexObj, &x)
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

	BOOST_STATIC_ASSERT((DataIO_is_dump<MemReader, BS1>::value) == true);
	BOOST_STATIC_ASSERT((DataIO_is_dump<MemReader, BS2>::value) == true);
	BOOST_STATIC_ASSERT((DataIO_is_dump<MemReader, BS3>::value) == true);

	BOOST_STATIC_ASSERT((DataIO_need_bswap<A>::value) == true);
	BOOST_STATIC_ASSERT((DataIO_need_bswap<B>::value) == true);
	BOOST_STATIC_ASSERT((DataIO_need_bswap<C>::value) == true);
	BOOST_STATIC_ASSERT((DataIO_need_bswap<D>::value) == true);
	BOOST_STATIC_ASSERT((DataIO_need_bswap<E>::value) == true);

	BOOST_STATIC_ASSERT((DataIO_need_bswap<BS1>::value) == false);
	BOOST_STATIC_ASSERT((DataIO_need_bswap<BS2>::value) == false);
	BOOST_STATIC_ASSERT((DataIO_need_bswap<BS3>::value) == false);

	BOOST_STATIC_ASSERT((DataIO_need_bswap<ComplexObj>::value) == true);
	BOOST_STATIC_ASSERT((DataIO_is_dump<MemReader, ComplexObj>::value) == false);

//	printf("sizeof(A+B+C) = %zd\n", sizeof(A) + sizeof(B) + sizeof(C));
//	printf("sizeof(D    ) = %zd\n", sizeof(D));
	MemWriter wr;
	wr << D();
	A a;
//	a._M_Deduce_DataIO_need_bswap().need_bswap();
    return 0;
}

