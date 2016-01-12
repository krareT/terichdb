/*
 * record_codec.h
 *
 *  Created on: Aug 27, 2015
 *      Author: leipeng
 */

#ifndef SRC_RECORD_CODEC_H_
#define SRC_RECORD_CODEC_H_

#include <mongo/bson/bsonobj.h>
#include <nark/valvec.hpp>
#include <nark/fstring.hpp>
#include <nark/db/db_conf.hpp>
#include <nark/db/db_segment.hpp>

namespace mongo { namespace narkdb {

using nark::db::Schema;
using nark::db::SchemaPtr;
using nark::db::SchemaSet;
using nark::db::SchemaSetPtr;

extern const char G_schemaLessFieldName[];

class SchemaRecordCoder {
public:
	nark::febitvec m_stored;
	typedef nark::gold_hash_set<nark::fstring,
		nark::fstring_func::hash, nark::fstring_func::equal> FieldsMap;
	FieldsMap m_fields;

	SchemaRecordCoder();
	~SchemaRecordCoder();

	static void parseToFields(const BSONObj&, FieldsMap*);
	static bool fieldsEqual(const FieldsMap&, const FieldsMap&);

	void
	encode(const Schema* schema, const Schema* exclude,	const BSONObj& key,
		   nark::valvec<unsigned char>* encoded) {
		encode(schema, exclude, key,
			reinterpret_cast<nark::valvec<char>*>(encoded));
	}

	void
	encode(const Schema* schema, const Schema* exclude,	const BSONObj& key,
		   nark::valvec<char>* encoded);

	nark::valvec<char>
	encode(const Schema* schema, const Schema* exclude, const BSONObj& key);

	SharedBuffer decode(const Schema* schema, const char* data, size_t size);
	SharedBuffer decode(const Schema* schema, const nark::valvec<char>& encoded);
	SharedBuffer decode(const Schema* schema, StringData encoded);
	SharedBuffer decode(const Schema* schema, nark::fstring encoded);
};

void encodeIndexKey(const Schema& indexSchema,
					const BSONObj& bson,
					nark::valvec<char>* encoded);
void encodeIndexKey(const Schema& indexSchema,
					const BSONObj& bson,
					nark::valvec<unsigned char>* encoded);

SharedBuffer
decodeIndexKey(const Schema& indexSchema, const char* data, size_t size);

inline SharedBuffer
decodeIndexKey(const Schema& indexSchema, nark::fstring data) {
	return decodeIndexKey(indexSchema, data.data(), data.size());
}

} } // namespace mongo::narkdb



#endif /* SRC_RECORD_CODEC_H_ */
