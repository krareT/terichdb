/*
 * index_key.h
 *
 *  Created on: Jul 25, 2015
 *      Author: leipeng
 */

#ifndef INDEX_KEY_H_
#define INDEX_KEY_H_

#include <mongo/bson/bsonobj.h>
#include <nark/valvec.hpp>

namespace mongo {

class NarkBsonBlob : public nark::valvec<char> {
	template<class DataIO>
	friend void DataIO_loadObject(DataIO& dio, NarkBsonBlob& x) {
		int byteNum = dio.template load_as<int>();
		if (byteNum < 5) {
			fprintf(stderr, "NarkBsonBlob::DataIO_loadObject: byteNum=%d\n", byteNum);
		}
		invariant(byteNum >= 5);
		x.resize_no_init(byteNum);
		DataView(x.data()).write<LittleEndian<int>>(byteNum);
		dio.ensureRead(x.data() + 4, byteNum - 4);
	}
	template<class DataIO>
	friend void DataIO_saveObject(DataIO& dio, const NarkBsonBlob& x) {
		invariant(x.size() >= 5);
		invariant(int(x.size()) == *(const int*)x.data());
		dio.ensureWrite(x.data(), x.size());
	}
public:
//	using nark::valvec<char>;
};

NarkBsonBlob narkEncodeIndexKey(const BSONObj& key);
void narkEncodeIndexKey(const BSONObj& key, NarkBsonBlob* encoded);

BSONObj narkDecodeIndexKey(StringData encoded, const BSONObj& fieldnames);

}


#endif /* INDEX_KEY_H_ */
