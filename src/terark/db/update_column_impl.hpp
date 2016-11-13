	const Schema& rowSchema = *m_schema->m_rowSchema;
	assert(columnId < rowSchema.columnNum());
	if (columnId >= rowSchema.columnNum()) {
		THROW_STD(invalid_argument
			, "Invalid columnId=%zd, ge than columnNum=%zd"
			, columnId, rowSchema.columnNum()
			);
	}
	if (rowSchema.getColumnMeta(columnId).fixedLen == 0) {
		THROW_STD(invalid_argument
			, "Invalid column(id=%zd, name=%s) which columnType=%s"
			, columnId, rowSchema.getColumnName(columnId).c_str()
			, Schema::columnTypeStr(rowSchema.getColumnType(columnId))
			);
	}
	auto colproj = m_schema->m_colproject[columnId];
	assert(colproj.colgroupId < m_schema->getColgroupNum());
	const Schema& cgSchema = m_schema->getColgroupSchema(colproj.colgroupId);
	if (cgSchema.getFixedRowLen() == 0) {
		THROW_STD(invalid_argument
			, "Invalid column(id=%zd, name=%s) which colgroup=%s is not fixed length"
			, columnId, rowSchema.getColumnName(columnId).c_str()
			, m_schema->getColgroupSchema(colproj.colgroupId).m_name.c_str()
			);
	}
	if (!cgSchema.m_isInplaceUpdatable) {
		THROW_STD(invalid_argument
			, "Invalid column(id=%zd, name=%s) which colgroup=%s is not inplace updatable"
			, columnId, rowSchema.getColumnName(columnId).c_str()
			, m_schema->getColgroupSchema(colproj.colgroupId).m_name.c_str()
			);
	}

	MyRwLock lock(m_rwMutex, false);
	DebugCheckRowNumVecNoLock(this);
	assert(m_rowNumVec.size() == m_segments.size()+1);
	assert(recordId < m_rowNumVec.back());
	size_t upp = upper_bound_a(m_rowNumVec, recordId);
	assert(upp < m_rowNumVec.size());
	llong baseId = m_rowNumVec[upp-1];
	llong subId = recordId - baseId;
	auto seg = m_segments[upp-1].get();
	assert(subId < (llong)seg->m_isDel.size());
	//assert(seg->m_isDel.is0(subId));
	if (seg->m_isDel.is1(size_t(subId))) {
        throw ReadDeletedRecordException(seg->m_segDir.string(), baseId, subId);
	}
	assert(seg->m_colgroups.size() == m_schema->getColgroupNum());
	assert(seg->m_colgroups[colproj.colgroupId] != nullptr);
	auto store = seg->m_colgroups[colproj.colgroupId].get();
	llong physicId = seg->getPhysicId(subId);
	byte* recordsBasePtr = store->getRecordsBasePtr();
	assert(nullptr != recordsBasePtr);
	size_t cgLen   = cgSchema.getFixedRowLen();
	size_t offset  = cgSchema.getColumnMeta(colproj.subColumnId).fixedOffset;
	byte * coldata = recordsBasePtr + cgLen * physicId + offset;
