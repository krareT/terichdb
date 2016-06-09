/*
 *  Created on: 2015-08-27
 *      Author: leipeng, rockeet@gmail.com
 */

#ifndef SRC_RECORD_CODEC_H_
#define SRC_RECORD_CODEC_H_

#include <mongo/bson/bsonobj.h>
#include <terark/valvec.hpp>
#include <terark/fstring.hpp>
#include <terark/db/db_conf.hpp>
#include <terark/db/db_segment.hpp>

namespace mongo { namespace terarkdb {

using terark::db::Schema;
using terark::db::SchemaPtr;
using terark::db::SchemaSet;
using terark::db::SchemaSetPtr;

extern const char G_schemaLessFieldName[];

class SchemaRecordCoder {
public:
	terark::febitvec m_stored;
	typedef terark::gold_hash_set<terark::fstring,
		terark::fstring_func::hash, terark::fstring_func::equal> FieldsMap;
	FieldsMap m_fields;

	SchemaRecordCoder();
	~SchemaRecordCoder();

	static void parseToFields(const BSONObj&, FieldsMap*);
	static bool fieldsEqual(const FieldsMap&, const FieldsMap&);

	void
	encode(const Schema* schema, const Schema* exclude,	const BSONObj& key,
		   terark::valvec<unsigned char>* encoded) {
		encode(schema, exclude, key,
			reinterpret_cast<terark::valvec<char>*>(encoded));
	}

	void
	encode(const Schema* schema, const Schema* exclude,	const BSONObj& key,
		   terark::valvec<char>* encoded);

	terark::valvec<char>
	encode(const Schema* schema, const Schema* exclude, const BSONObj& key);

	SharedBuffer decode(const Schema* schema, const char* data, size_t size);
	SharedBuffer decode(const Schema* schema, const terark::valvec<char>& encoded);
	SharedBuffer decode(const Schema* schema, StringData encoded);
	SharedBuffer decode(const Schema* schema, terark::fstring encoded);
};

void encodeIndexKey(const Schema& indexSchema,
					const BSONObj& bson,
					terark::valvec<char>* encoded);
void encodeIndexKey(const Schema& indexSchema,
					const BSONObj& bson,
					terark::valvec<unsigned char>* encoded);

SharedBuffer
decodeIndexKey(const Schema& indexSchema, const char* data, size_t size);

inline SharedBuffer
decodeIndexKey(const Schema& indexSchema, terark::fstring data) {
	return decodeIndexKey(indexSchema, data.data(), data.size());
}

} } // namespace mongo::terarkdb



#endif /* SRC_RECORD_CODEC_H_ */
