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

	MyRwLock lock(m_rwMutex, true);
	DebugCheckRowNumVecNoLock(this);
	assert(m_rowNumVec.size() == m_segments.size()+1);
	assert(recordId < m_rowNumVec.back());
	size_t upp = upper_bound_a(m_rowNumVec, recordId);
	assert(upp < m_rowNumVec.size());
	llong baseId = m_rowNumVec[upp-1];
	llong subId = recordId - baseId;
	auto seg = m_segments[upp-1].get();
	assert(seg->m_isDel.is0(subId));
	if (seg->m_isDel.is1(size_t(subId))) {
		THROW_STD(invalid_argument
			, "Row has been deleted: id=%lld seg=%zd baseId=%lld subId=%lld"
			, recordId, upp, baseId, subId);
	}
	auto store = seg->m_colgroups[colproj.colgroupId].get();
	auto updatable = store->getUpdatableStore();
	assert(NULL != updatable);
	llong physicId = seg->getPhysicId(subId);
