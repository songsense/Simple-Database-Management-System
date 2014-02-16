
#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;
SpaceManager* SpaceManager::_space_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
	RC rc;
	PagedFileManager *pfm = PagedFileManager::instance();
	rc = pfm->createFile(fileName.c_str());
	if(rc != SUCC) {
		cerr << "IndexManager::createFile: create index file error " << rc << endl;
		return rc;
	}
	FileHandle fileHandle;
	rc = pfm->openFile(fileName.c_str(), fileHandle);
	if(rc != SUCC) {
		cerr << "IndexManager::createFile: open index file error " << rc << endl;
		return rc;
	}

	setPageEmpty(Entry);
	setPageLeaf(Entry, IS_LEAF);
	rc = fileHandle.appendPage(Entry);
	if(rc != SUCC) {
		cerr << "IndexManager::createFile: append root page eror " << rc << endl;
		return rc;
	}

	rc = pfm->closeFile(fileHandle);
	if(rc != SUCC) {
		cerr << "IndexManager::createFile: close index file error " << rc << endl;
		return rc;
	}

	return rc;
}

RC IndexManager::destroyFile(const string &fileName)
{
	RC rc;
	PagedFileManager *pfm = PagedFileManager::instance();
	rc = pfm->destroyFile(fileName.c_str());
	if(rc != SUCC) {
		cerr << "IndexManager::destroyFile: destroy index file error " << rc << endl;
		return rc;
	}
	SpaceManager *sm = SpaceManager::instance();
	sm->closeIndexFileInfo(fileName);
	return SUCC;
}

RC IndexManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	RC rc;
	PagedFileManager *pfm = PagedFileManager::instance();
	rc = pfm->openFile(fileName.c_str(), fileHandle);
	if(rc != SUCC) {
		cerr << "IndexManager::openFile: open index file error " << rc << endl;
		return rc;
	}
	SpaceManager *sm = SpaceManager::instance();
	rc = sm->initIndexFile(fileName);
	if(rc != SUCC) {
		cerr << "IndexManager::openFile: init index file error " << rc << endl;
		return rc;
	}
	return SUCC;
}

RC IndexManager::closeFile(FileHandle &fileHandle)
{
	RC rc;
	PagedFileManager *pfm = PagedFileManager::instance();
	rc = pfm->closeFile(fileHandle);
	if(rc != SUCC) {
		cerr << "IndexManager::closeFile: close index file error " << rc << endl;
		return rc;
	}
	SpaceManager *sm = SpaceManager::instance();
	sm->closeIndexFileInfo(fileHandle.fileName);
	return SUCC;
}

RC IndexManager::insertEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	RC rc;
	rc = insertEntry(ROOT_PAGE, fileHandle,
			attribute, key, rid);
	return rc;
}

RC IndexManager::deleteEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	//TODO
	return -1;
}

RC IndexManager::scan(FileHandle &fileHandle,
    const Attribute &attribute,
    const void      *lowKey,
    const void      *highKey,
    bool			lowKeyInclusive,
    bool        	highKeyInclusive,
    IX_ScanIterator &ix_ScanIterator)
{
	//TODO
	return -1;
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	//TODO
	return -1;
}

RC IX_ScanIterator::close()
{
	//TODO
	return -1;
}

void IX_PrintError (RC rc)
{
	//TODO
}

/*
 * Split/merge pages
 */
RC IndexManager::splitPage(void *oldPage, void *newPage,
		const Attribute &attr,
		IsLeaf &isLeaf, void *keyCopiedUp) {
	//TODO
	return -1;
}
RC IndexManager::mergePage(void *srcPage, void *destPage,
		const Attribute &attr) {
	//TODO
	return -1;
}


void IndexManager::setPageEmpty(void *page) {
	setFreeSpaceStartPoint(page, page);
	setNextPageNum(page, 0);
	setPrevPageNum(page, 0);
	setPageLeaf(page, NOT_LEAF);
	setSlotNum(page, 0);
}

bool IndexManager::isPageEmpty(void *page) {
	Offset offset = getFreeSpaceOffset(page);
	if (offset == 0)
		return true;
	else
		return false;
}

// free space operation
void* IndexManager::getFreeSpaceStartPoint(const void *page) {
	char *data = (char *)page + PAGE_SIZE - sizeof(Offset);
	Offset freeSpaceStartLen = *((Offset *)data);
	data = (char *)page + freeSpaceStartLen;
	return (void *)data;
}
Offset IndexManager::getFreeSpaceOffset(const void *page) {
	char *data = (char *)page + PAGE_SIZE - sizeof(Offset);
	return *((Offset *)data);
}
void IndexManager::setFreeSpaceStartPoint(void *page, const void *point) {
	Address addr_len = (Address)point - (Address)page;
	Offset freeSpaceStartLen = (Offset)addr_len;
	char *data = (char *)page + PAGE_SIZE - sizeof(Offset);
	memcpy(data, &freeSpaceStartLen, sizeof(Offset));
}
int IndexManager::getFreeSpaceSize(const void *page) {
	char *data = (char *)page + PAGE_SIZE - sizeof(Offset);
	Offset freeSpaceStartLen = *((Offset *)data);
	SlotNum totalSlotNum = getSlotNum(page);
	int restSpace = PAGE_SIZE - freeSpaceStartLen
			- sizeof(Offset)
			- sizeof(PageNum)*2 - sizeof(IsLeaf)
			- sizeof(SlotNum) - totalSlotNum * sizeof(IndexDir);
	return restSpace;
}

// leaf flag operation
IsLeaf IndexManager::isPageLeaf(const void *page) {
	char *data = (char *)page + PAGE_SIZE
			- sizeof(Offset)
			- sizeof(IsLeaf);
	IsLeaf isLeaf(NOT_LEAF);
	memcpy(&isLeaf, data, sizeof(IsLeaf));
	return isLeaf;
}
void IndexManager::setPageLeaf(void *page, const IsLeaf &isLeaf) {
	char *data = (char *)page + PAGE_SIZE
			- sizeof(Offset)
			- sizeof(IsLeaf);
	memcpy(data, &isLeaf, sizeof(IsLeaf));
}


// set/get prev/next page number
PageNum IndexManager::getPrevPageNum(const void *page) {
	char *data = (char *)page + PAGE_SIZE - sizeof(Offset)
			- sizeof(IsLeaf)
			- sizeof(PageNum);
	PageNum pageNum(EOF_PAGE_NUM);
	memcpy(&pageNum, data, sizeof(PageNum));
	return pageNum;
}
PageNum IndexManager::getNextPageNum(const void *page) {
	char *data = (char *)page + PAGE_SIZE - sizeof(Offset)
			- sizeof(IsLeaf)
			- sizeof(PageNum)*2;
	PageNum pageNum(EOF_PAGE_NUM);
	memcpy(&pageNum, data, sizeof(PageNum));
	return pageNum;
}
void IndexManager::setPrevPageNum(void *page,
		const PageNum &pageNum) {
	char *data = (char *)page + PAGE_SIZE - sizeof(Offset)
			- sizeof(IsLeaf)
			- sizeof(PageNum);
	memcpy(data, &pageNum, sizeof(PageNum));
}
void IndexManager::setNextPageNum(void *page, const PageNum &pageNum) {
	char *data = (char *)page + PAGE_SIZE - sizeof(Offset)
			- sizeof(IsLeaf)
			- sizeof(PageNum)*2;
	memcpy(data, &pageNum, sizeof(PageNum));
}


SlotNum IndexManager::getSlotNum(const void *page) {
	char *data = (char *)page + PAGE_SIZE
			- sizeof(Offset)
			- sizeof(PageNum)*2 - sizeof(IsLeaf) - sizeof(SlotNum);
	SlotNum slotNum(0);
	memcpy(&slotNum, data,sizeof(slotNum));
	return slotNum;
}
void IndexManager::setSlotNum(void *page, const SlotNum &slotNum) {
	char *data = (char *)page + PAGE_SIZE
			- sizeof(Offset)
			- sizeof(PageNum)*2 - sizeof(IsLeaf) - sizeof(SlotNum);
	memcpy(data, &slotNum, sizeof(SlotNum));
}
RC IndexManager::getIndexDir(const void *page,
		IndexDir &indexDir,
		const SlotNum &slotNum) {
	SlotNum totalSlotNum = getSlotNum(page);
	if (totalSlotNum < slotNum)
		return IX_SLOT_DIR_OVERFLOW;
	char *data = (char *)page + PAGE_SIZE
				- sizeof(Offset)
				- sizeof(PageNum)*2 - sizeof(IsLeaf)
				- sizeof(SlotNum) - slotNum * sizeof(IndexDir);
	memcpy(&indexDir, data, sizeof(IndexDir));
	return SUCC;
}
RC IndexManager::setIndexDir(void *page,
		const IndexDir &indexDir,
		const SlotNum &slotNum) {
	SlotNum totalSlotNum = getSlotNum(page);
	if (totalSlotNum < slotNum)
		return IX_SLOT_DIR_OVERFLOW;
	char *data = (char *)page + PAGE_SIZE
				- sizeof(Offset)
				- sizeof(PageNum)*2 - sizeof(IsLeaf)
				- sizeof(SlotNum) - slotNum * sizeof(IndexDir);
	memcpy(data, &indexDir, sizeof(IndexDir));
	return SUCC;
}
// get a key's size
int IndexManager::getKeySize(const Attribute &attr, const void *key) {
	switch(attr.type) {
	case TypeInt:
		return sizeof(int);
		break;
	case TypeReal:
		return sizeof(float);
		break;
	case TypeVarChar:
		return sizeof(int) + *((int *)key);
		break;
	}
	return -1;
}


/*
 * Insert/delete/search an entry
 */
// Insert new index entry
RC IndexManager::insertEntry(const PageNum &pageNum,
		FileHandle &fileHandle,
		const Attribute &attribute,
		const void *key, const RID &rid) {
// TODO
	RC rc;
	SpaceManager *sm = SpaceManager::instance();

	rc = fileHandle.readPage(pageNum, block);
	if (rc != SUCC) {
		cerr << "IndexManager::insertEntry: error read page " << rc << endl;
		return rc;
	}
	IsLeaf pageType = isPageLeaf(block);
	if (pageType == IS_DUP_PAGE) {
		cerr << "IndexManager::insertEntry: read dup page " << IX_READ_DUP_PAGE << endl;
		return IX_READ_DUP_PAGE;
	}

	if (isPageEmpty(block) && pageNum == ROOT_PAGE) {
		// the very first data, just insert and return
		// set the data
		char *data = (char *)block;
		int keyLen = getKeySize(attribute, key);
		memcpy(data, key, keyLen);
		data += keyLen;
		memcpy(data, &rid, sizeof(RID));
		data += sizeof(RID);
		Dup dup = false;
		memcpy(data, &dup, sizeof(Dup));

		// set the index slot dir
		setSlotNum(block, 1);
		IndexDir indexDir;
		indexDir.recordLength = keyLen + sizeof(RID) + sizeof(Dup);
		indexDir.slotOffset = 0;
		setIndexDir(block, indexDir, 1);

		// set start point
		data = block + indexDir.recordLength;
		setFreeSpaceStartPoint(block, data);

		// set page type as leaf
		setPageLeaf(block, IS_LEAF);

		// write disk
		rc = fileHandle.writePage(pageNum, block);
		if (rc != SUCC) {
			cerr << "IndexManager::insertEntry: error write root page " << rc << endl;
			return rc;
		}
		return SUCC;
	}

	// search the key
	SlotNum slotNum;
	RC rc_search = binarySearchEntry(block, attribute, key, slotNum);


	IndexDir indexDir;
	rc = getIndexDir(block, indexDir, slotNum);
	if (rc != SUCC) {
		cerr << "IndexManager::insertEntry: get index dir error " << rc << endl;
		return rc;
	}
	char *data = block + indexDir.slotOffset;

	if (pageType == IS_LEAF) {
		if (rc_search == IX_SEARCH_HIT) {
			Dup isDup;
			memcpy(&isDup,
					data + indexDir.recordLength-sizeof(Dup),
					sizeof(Dup));
			RID dupHeadRID, dupAssignedRID;
			if (isDup == true) {
				// this is already a dup record
				memcpy(&dupHeadRID,
						data + indexDir.recordLength-sizeof(Dup)-sizeof(RID),
						sizeof(RID));
				rc = sm->insertDupRecord(fileHandle,
						dupHeadRID, rid, dupAssignedRID);
				if (rc != SUCC) {
					cerr << "IndexManager::insertEntry: insert dup record error 1 " << rc << endl;
					return rc;
				}
				memcpy(data + indexDir.recordLength-sizeof(Dup)-sizeof(RID),
						&dupAssignedRID, sizeof(RID));
			} else {
				// this is not a dup record
				// set the record to be dup
				isDup = true;
				memcpy(data + indexDir.recordLength - sizeof(Dup),
						&isDup, sizeof(Dup));
				// set the first record
				dupHeadRID.pageNum = DUP_PAGENUM_END;
				dupHeadRID.slotNum = 0;
				rc = sm->insertDupRecord(fileHandle,
						dupHeadRID, rid, dupAssignedRID);
				if (rc != SUCC) {
					cerr << "IndexManager::insertEntry: insert dup record error 2 " << rc << endl;
					return rc;
				}
				// set the second record
				RID dataRID;
				memcpy(&dataRID,
						data + indexDir.recordLength - sizeof(Dup) - sizeof(RID),
						sizeof(RID));
				dupHeadRID.pageNum = dupAssignedRID.pageNum;
				dupHeadRID.slotNum = dupAssignedRID.slotNum;
				rc = sm->insertDupRecord(fileHandle,
						dupHeadRID, dataRID, dupAssignedRID);
				if (rc != SUCC) {
					cerr << "IndexManager::insertEntry: insert dup record error 3 " << rc << endl;
					return rc;
				}
				// set the rid of the record at the page
				memcpy(data + indexDir.recordLength - sizeof(Dup) - sizeof(RID),
						&dupAssignedRID, sizeof(RID));
			}
			// write to the page
			rc = fileHandle.writePage(pageNum, block);
			if (rc != SUCC) {
				cerr << "IndexManager::insertEntry: write page error 1 " << rc << endl;
				return rc;
			}
		} else {
			// not hit, need to insert
			int keyLen = getKeySize(attribute, key);
			rc = insertEntryAtPos(block, slotNum, attribute,
					key, keyLen, rid, false, 0, 0);
			if (rc != SUCC) {
				cerr << "IndexManager::insertEntry: insert entry error 1 " << rc << endl;
				return rc;
			}
			// TODO split
			// write to the page
			rc = fileHandle.writePage(pageNum, block);
			if (rc != SUCC) {
				cerr << "IndexManager::insertEntry: write page error 2 " << rc << endl;
				return rc;
			}
		}
	} else {
		// continue search until reach a leaf
		// |p|key|p|key|p|key|p|
		if (rc_search == IX_SEARCH_LOWER_BOUND ||
				rc_search == IX_SEARCH_ABOVE) {
			// look at left
			data -= sizeof(PageNum);

		} else if (rc_search == IX_SEARCH_UPPER_BOUND ||
				rc_search == IX_SEARCH_HIT ||
				rc_search == IX_SEARCH_BELOW) {
			// look at right
			data += (indexDir.recordLength - sizeof(PageNum));
		}
		PageNum nextPageNum;
		memcpy(&nextPageNum, data, sizeof(PageNum));
		rc = insertEntry(nextPageNum,
				fileHandle,
				attribute, key, rid);
		if (rc != SUCC) {
			cerr << "IndexManager::insertEntry: next insert index entry error " << rc << endl;
			return rc;
		}
	}

	return SUCC;
}
// Delete index entry
RC IndexManager::deleteEntry(const PageNum &pageNum,
		FileHandle &fileHandle,
		const Attribute &attribute,
		const void *key, const RID &rid) {
// TODO
	return -1;
}
// binary search an entry
RC IndexManager::binarySearchEntry(const void *page,
		const Attribute &attr,
		const void *key,
		SlotNum &slotNum) {
	RC rc;
	// get the Total Slot number
	SlotNum totalSlotNum = getSlotNum(page);
	SlotNum beg(1), end(totalSlotNum);
	SlotNum med(beg);

	IndexDir indexDir;
	while (beg <= end) {
		med = beg + (end-beg)/2;
		rc = getIndexDir(page, indexDir, med);
		if (rc != SUCC) {
			cerr << "binarySearchEntry: error set slot dir " << rc << endl;
			return rc;
		}
		char *indexKey = (char *)page + indexDir.slotOffset;
		int compResult = compareKey(attr, key, indexKey);
		if (compResult == 0) {
			slotNum = med;
			return IX_SEARCH_HIT;
		} else if (compResult > 0) {
			beg = med+1;
		} else {
			end = med-1;
		}
	}
	if (beg > totalSlotNum) {
		slotNum = totalSlotNum+1;
		return IX_SEARCH_UPPER_BOUND;
	} else if (end < 1) {
		slotNum = 1;
		return IX_SEARCH_LOWER_BOUND;
	} else {
		med = beg + (end-beg)/2;
		rc = getIndexDir(page, indexDir, med);
		if (rc != SUCC) {
			cerr << "binarySearchEntry: error get slot dir " << rc << endl;
			return rc;
		}
		char *indexKey = (char *)page + indexDir.slotOffset;
		int compResult = compareKey(attr, key, indexKey);
		if (compResult == 0) {
			slotNum = med;
			return IX_SEARCH_HIT;
		} else if (compResult > 0) {
			slotNum = med+1;
			return IX_SEARCH_ABOVE;
		} else {
			slotNum = med;
			return IX_SEARCH_BELOW;
		}
	}
	return SUCC;
}
// insert an entry into page at pos n
RC IndexManager::insertEntryAtPos(void *page, const SlotNum &slotNum,
		  const Attribute &attr,
		  const void *key, const unsigned &keyLen,
		  const RID &rid, const Dup &dup,
		  const PageNum &prevPageNum,
		  const PageNum &nextPageNum) {
	// do not allow slotNum <= 0
	if (slotNum <= 0) {
		cerr << "insertEntryAtPos: slot num is less than 1 "
				<< IX_SLOT_DIR_LESS_ZERO << endl;
		return IX_SLOT_DIR_LESS_ZERO;
	}

	RC rc;
	IsLeaf isLeaf = isPageLeaf(page);
	int spaceNeeded = keyLen + sizeof(IndexDir);
	int recordSize = keyLen;
	if (isLeaf) {
		spaceNeeded += sizeof(RID) + sizeof(Dup);
		recordSize += sizeof(RID) + sizeof(Dup);
	} else {
		spaceNeeded += sizeof(PageNum);
		recordSize += sizeof(PageNum);
	}
	int spaceAvailable = getFreeSpaceSize(page);

	// check space availability
	if (spaceAvailable < spaceNeeded) {
		// need split
		return IX_NOT_ENOUGH_SPACE;
	}

	// enough space
	// get the total number of slot
	SlotNum totalSlotNum = getSlotNum(page);
	SlotNum slotNum2Insert = slotNum;

	// set the total number by increment 1
	setSlotNum(page, totalSlotNum+1);

	IndexDir indexDir;

	if (totalSlotNum == 0 && !isLeaf) {
		// the page is empty && the page is NOT an leaf
		// the case only happens when the page is the root
		// will verify this outside
		// copy data to the page
		char *data = (char *)page;
		memcpy(data, &prevPageNum, sizeof(PageNum));
		data += sizeof(PageNum);
		memcpy(data, key, keyLen);
		data += keyLen;
		memcpy(data, &nextPageNum, sizeof(PageNum));

		// set the free space
		setFreeSpaceStartPoint(page,
				(char *)page + keyLen + 2*sizeof(PageNum));
		// set the prev/next page number to root(itself)
		setPrevPageNum(page, 0);
		setNextPageNum(page, 0);
		// set slot dir
		indexDir.recordLength = recordSize;
		indexDir.slotOffset = sizeof(PageNum);
		rc = setIndexDir(page, indexDir, 1);
		if (rc != SUCC) {
			cerr << "insertEntryAtPosLeaf: error set slot dir " << rc << endl;
			return rc;
		}

		return SUCC;
	}
	if (slotNum2Insert > totalSlotNum) {
		slotNum2Insert = totalSlotNum + 1;
		char *pos2InsertEntry = (char *)getFreeSpaceStartPoint(page);

		// set the index dir
		indexDir.recordLength = recordSize;
		indexDir.slotOffset = getFreeSpaceOffset(page);
		rc = setIndexDir(page, indexDir, slotNum2Insert);
		if (rc != SUCC) {
			cerr << "insertEntryAtPosLeaf: error set slot dir " << rc << endl;
			return rc;
		}

		// move the data
		if (isLeaf) {
			memcpy(pos2InsertEntry, key, keyLen);
			pos2InsertEntry += keyLen;
			memcpy(pos2InsertEntry, &rid, sizeof(RID));
			pos2InsertEntry += sizeof(RID);
			memcpy(pos2InsertEntry, &dup, sizeof(Dup));
		} else {
			memcpy(pos2InsertEntry, key, keyLen);
			pos2InsertEntry += keyLen;
			memcpy(pos2InsertEntry, &nextPageNum, sizeof(PageNum));
		}

		// set the free space start point
		Offset startPointOffset = getFreeSpaceOffset(page);
		startPointOffset += recordSize;
		setFreeSpaceStartPoint(page, (char *)page + startPointOffset);
	} else {
		// NOTE:
		// There is never a request to insert entry to the head of the page
		// get the offset of the place to be inserted
		// non leaf cannot be empty initially
		IndexDir indexDir;
		rc = getIndexDir(page, indexDir, slotNum2Insert);
		Offset slotOffset2Insert = indexDir.slotOffset; // keep the slot offset for future insertion
		if (rc != SUCC) {
			cerr << "insertEntryAtPosLeaf: error get slot dir " << rc << endl;
			return rc;
		}

		// iteratively move the data
		for (SlotNum sn = totalSlotNum; sn >= slotNum2Insert; --sn) {
			// get the slot dir
			rc = getIndexDir(page, indexDir, sn);
			if (rc != SUCC) {
				cerr << "insertEntryAtPosLeaf: error get slot dir " << rc << endl;
				return rc;
			}
			// move data
			char *originPos = (char *)page + indexDir.slotOffset;
			char *newPos = originPos + recordSize;
			memmove(newPos, originPos, indexDir.recordLength);

			// reset index dir
			indexDir.slotOffset += recordSize;
			rc = setIndexDir(page, indexDir, sn+1);
			if (rc != SUCC) {
				cerr << "insertEntryAtPosLeaf: error get slot dir " << rc << endl;
				return rc;
			}
		}
		// then insert the entry
		char *pointer2Insert = (char *)page + slotOffset2Insert;
		memcpy(pointer2Insert, key, keyLen);
		pointer2Insert += keyLen;
		if (isLeaf) {
			memcpy(pointer2Insert, &rid, sizeof(RID));
			pointer2Insert += sizeof(RID);
			memcpy(pointer2Insert, &dup, sizeof(Dup));
		} else {
			memcpy(pointer2Insert, &nextPageNum, sizeof(PageNum));
		}

		// set the index dir
		indexDir.recordLength = recordSize;
		indexDir.slotOffset = slotOffset2Insert;
		rc = setIndexDir(page, indexDir, slotNum2Insert);
		if (rc != SUCC) {
			cerr << "insertEntryAtPosLeaf: error get slot dir " << rc << endl;
			return rc;
		}

		// set the free space start point
		Offset startPointOffset = getFreeSpaceOffset(page);
		startPointOffset += recordSize;
		setFreeSpaceStartPoint(page, (char *)page + startPointOffset);
	}
	return SUCC;
}
// delete an entry of page at pos n
RC IndexManager::deleteEntryAtPos(void *page,
		const SlotNum &slotNum) {
	// do not allow slotNum <= 0
	if (slotNum <= 0) {
		cerr << "deleteEntryAtPos: slot num is less than 1 "
				<< IX_SLOT_DIR_LESS_ZERO << endl;
		return IX_SLOT_DIR_LESS_ZERO;
	}
	RC rc;
	// get the type of the page
	IsLeaf leaf = isPageLeaf(page);
	// get the total number of the slots
	SlotNum totalSlotNum = getSlotNum(page);
	if (slotNum > totalSlotNum) {
		cerr << "deleteEntryAtPos: attempt to delete non-exist slot "
				<< IX_SLOT_DIR_OVERFLOW << endl;
		return IX_SLOT_DIR_OVERFLOW;
	}
	if (totalSlotNum <= 1) {
		// there is only one slot
		// just set the page to be empty
		setPageEmpty(page);
		setPageLeaf(page, leaf);
		return SUCC;
	}

	IndexDir indexDir;
	// get the slot to be deleted
	rc = getIndexDir(page, indexDir, slotNum);
	if (rc != SUCC) {
		cerr << "deleteEntryAtPos: error get slot dir " << rc << endl;
		return rc;
	}
	// get the shift to each later entry
	Offset shiftOffset = indexDir.recordLength;

	// get current free space start point to reset free space start point
	Offset curFreeSpaceOffset = getFreeSpaceOffset(page);
	// set the free space start point
	setFreeSpaceStartPoint(page,
			(char*)page + curFreeSpaceOffset - shiftOffset);

	for (SlotNum sn = slotNum+1; sn <= totalSlotNum; ++sn) {
		// get the index dir
		rc = getIndexDir(page, indexDir, sn);
		if (rc != SUCC) {
			cerr << "deleteEntryAtPos: error get slot dir " << rc << endl;
			return rc;
		}
		// get the copy from address
		char *fromAddr = (char *)page + indexDir.slotOffset;
		// get the copy to address
		char *toAddr = fromAddr - shiftOffset;
		// update dir
		indexDir.slotOffset -= shiftOffset;
		// update the slot dir
		rc = setIndexDir(page, indexDir, sn-1);
		// copy
		memmove(toAddr, fromAddr, indexDir.recordLength);
	}

	// set the slot num
	setSlotNum(page, totalSlotNum-1);

	return SUCC;
}
// compare two key
// negative: <; positive: >; equal: =;
int IndexManager::compareKey(const Attribute &attr,
		  const void *lhs, const void *rhs) {
	char *str_lhs(NULL), *str_rhs(NULL);
	int result(0);
	switch(attr.type) {
	case TypeInt:
		return *((int *)lhs) - *((int *)rhs);
		break;
	case TypeReal:
		return *((float *)lhs) - *((float *)rhs);
		break;
	case TypeVarChar:
		// left hand size string
		str_lhs = new char[*((int *)lhs)+1];
		memcpy(str_lhs, (char *)lhs + sizeof(int), *((int *)lhs));
		str_lhs[*((int *)lhs)] = '\0';
		// right hand size string
		str_rhs = new char[*((int *)rhs)+1];
		memcpy(str_rhs, (char *)rhs + sizeof(int), *((int *)rhs));
		str_rhs[*((int *)rhs)] = '\0';
		// compare the string
		result = strcmp(str_lhs, str_rhs);
		delete []str_lhs;
		delete []str_rhs;
		return result;
		break;
	}
	return 0;
}

// for debug only
void IndexManager::printPage(const void *page, const Attribute &attr) {
	IsLeaf isLeaf = isPageLeaf(page);
	if (isLeaf == IS_LEAF)
		printLeafPage(page, attr);
	else if (isLeaf == NOT_LEAF)
		printNonLeafPage(page, attr);
	else if (isLeaf == IS_DUP_PAGE)
		printDupPage(page);
}

void IndexManager::printDupPage(const void *page) {
	cout << "==============\tDuplicate Page Information\t==============" << endl;
	cout << "Free space: " << getFreeSpaceSize(page) << "Bytes" << endl;
	SlotNum totalSlotNum = getSlotNum(page);
	cout << "Total # of Entries: " << totalSlotNum << endl;
	cout << "-------------------------------------------------" << endl;
	cout << "slot number.\tnext RID\tdata RID" << endl;
	char *data = (char *)page;
	for (SlotNum sn = 1; sn <= totalSlotNum; ++sn) {
		cout << sn << "\t";
		IndexDir indexDir;
		RC rc = getIndexDir(page, indexDir, sn);
		if (rc != SUCC) {
			cout << "slot num is incorrect " << rc << endl;
			return;
		}
		if (indexDir.slotOffset == DUP_SLOT_DEL) {
			cout << "deleted record" << endl;
			data += DUP_RECORD_SIZE;
			continue;
		}
		RID nextRID, dataRID;
		data = (char *)page + DUP_RECORD_SIZE * (sn - 1);
		memcpy(&nextRID, data, sizeof(RID));
		data += sizeof(RID);
		memcpy(&dataRID, data, sizeof(RID));
		data += sizeof(RID);
		cout << nextRID.pageNum << ":" << nextRID.slotNum << "\t";
		cout << dataRID.pageNum << ":" << dataRID.slotNum << "\n";
	}
	cout << endl;
}
void IndexManager::printNonLeafPage(const void *page, const Attribute &attr) {
	cout << "==============\tNonleaf Page Information\t==============" << endl;
	cout << "Free space: " << getFreeSpaceSize(page) << "Bytes" << endl;
	cout << "Previous Page: " << getPrevPageNum(page) <<
			"\tNext Page: " << getNextPageNum(page) << endl;
	SlotNum totalSlotNum = getSlotNum(page);
	cout << "Total # of Entries: " << totalSlotNum << endl;
	cout << "-------------------------------------------------" << endl;
	cout << "No.\toffset\tlength\tkey\tnext page number" << endl;
	for (SlotNum sn = 1; sn <= totalSlotNum; ++sn) {
		IndexDir indexDir;
		RC rc = getIndexDir(page, indexDir, sn);
		if (rc != SUCC) {
			cout << "slot num is incorrect " << rc << endl;
			return;
		}
		cout << sn << "\t" << indexDir.slotOffset << "\t" << indexDir.recordLength << "\t";
		char *data = (char *)page + indexDir.slotOffset;
		printKey(attr, data);
		PageNum pageNum;
		memcpy(&pageNum, data+indexDir.recordLength-sizeof(PageNum), sizeof(PageNum));
		cout << "\t" << pageNum << endl;
	}
	cout << endl;
}
void IndexManager::printLeafPage(const void *page, const Attribute &attr) {
	cout << "==============\tLeaf Page Information\t==============" << endl;
	cout << "Free space: " << getFreeSpaceSize(page) << "Bytes" << endl;
	cout << "Previous Page: " << getPrevPageNum(page) <<
			"\tNext Page: " << getNextPageNum(page) << endl;
	SlotNum totalSlotNum = getSlotNum(page);
	cout << "Total # of Entries: " << totalSlotNum << endl;
	cout << "-------------------------------------------------" << endl;
	cout << "No.\toffset\tlength\tkey\tPageNum\tSlotNum\tDup" << endl;
	for (SlotNum sn = 1; sn <= totalSlotNum; ++sn) {
		IndexDir indexDir;
		RC rc = getIndexDir(page, indexDir, sn);
		if (rc != SUCC) {
			cout << "slot num is incorrect " << rc << endl;
			return;
		}
		cout << sn << "\t" << indexDir.slotOffset << "\t" << indexDir.recordLength << "\t";
		char *data = (char *)page + indexDir.slotOffset;
		printKey(attr, data);
		RID rid;
		memcpy(&rid, data+indexDir.recordLength-sizeof(Dup)-sizeof(RID), sizeof(RID));
		cout << "\t" << rid.pageNum << "\t" << rid.slotNum << "\t";
		Dup dup(false);
		memcpy(&dup, data+indexDir.recordLength-sizeof(Dup), sizeof(Dup));
		cout << dup << endl;
	}
	cout << endl;
}
void IndexManager::printKey(const Attribute &attr,
		const void *key) {
	stringstream ss;
	int len(0);
	switch (attr.type) {
	case TypeInt:
		ss << *((int *)key);
		cout << ss.str();
		break;
	case TypeReal:
		ss << *((float *)key);
		cout << ss.str();
		break;
	case TypeVarChar:
		len = *((int *)key);
		memcpy(Entry, (char *)key+sizeof(int), len);
		Entry[len] = '\0';
		cout << Entry;
		break;
	}
}


SpaceManager* SpaceManager::instance()
{
    if(!_space_manager)
    	_space_manager = new SpaceManager();

    return _space_manager;
}

SpaceManager::SpaceManager()
{
}

SpaceManager::~SpaceManager()
{
}

RC SpaceManager::initIndexFile(const string &indexFileName) {
	FileHandle fileHandle;
	RC rc;
	PagedFileManager *pfm = PagedFileManager::instance();
	IndexManager	*ix = IndexManager::instance();

	// open file
	rc = pfm->openFile(indexFileName.c_str(), fileHandle);
	if (rc != SUCC) {
		cerr << "initIndexFile: open file " <<
				indexFileName << " error " << rc << endl;
		return rc;
	}

	// init list
	auto itrDup = dupRecordPageList.find(indexFileName);
	if (itrDup == dupRecordPageList.end()) {
		dupRecordPageList.insert(
				pair<string, unordered_set<PageNum> >(indexFileName, {}));
		itrDup = dupRecordPageList.find(indexFileName);
	} else {
		itrDup->second.clear();
	}
	auto itrEmpty = emptyPageList.find(indexFileName);
	if (itrEmpty == emptyPageList.end()) {
		emptyPageList.insert(
				pair<string, unordered_set<PageNum> >(indexFileName, {}));
		itrEmpty = emptyPageList.find(indexFileName);
	} else {
		itrEmpty->second.clear();
	}


	PageNum totalPageNum(0);
	totalPageNum = fileHandle.getNumberOfPages();
	for (PageNum pn = 1; pn < totalPageNum; ++pn) {
		rc = fileHandle.readPage(pn, page);
		if (rc != SUCC) {
			cerr << "initIndexFile: read page error " << rc << endl;
			return rc;
		}
		IsLeaf isDupPage = ix->isPageLeaf(page);
		if(isDupPage == IS_DUP_PAGE) {
			int availSpace = ix->getFreeSpaceSize(page);
			if (availSpace > DUP_RECORD_SIZE) {
				itrDup->second.insert(pn);
			}
		} else if(ix->isPageEmpty(page)) {
			itrEmpty->second.insert(pn);
		}
	}

	// close file
	rc = pfm->closeFile(fileHandle);
	if (rc != SUCC) {
		cerr << "initIndexFile: close file " <<
				indexFileName << " error " << rc << endl;
		return rc;
	}
	return SUCC;
}
void SpaceManager::closeIndexFileInfo(const string &indexFileName) {
	// init list
	auto itrDup = dupRecordPageList.find(indexFileName);
	if (itrDup == dupRecordPageList.end()) {
		return;
	} else {
		dupRecordPageList.erase(itrDup);
	}
	auto itrEmpty = emptyPageList.find(indexFileName);
	if (itrEmpty == emptyPageList.end()) {
		return;
	} else {
		emptyPageList.erase(itrEmpty);
	}
}
// given first duplicated head RID and data RID
// insert the dup record with assigned RID in index
// NOTE: first use: need config dupHeadRid.PageNum = DUP_PAGENUM_END
RC SpaceManager::insertDupRecord(FileHandle &fileHandle,
		const RID &dupHeadRID,
		const RID &dataRID,
		RID &dupAssignedRID) {
	IndexManager *ix = IndexManager::instance();
	RC rc;
	PageNum pageNum;
	int spaceAvailable = 0;
	int loop = 0;
	do {
		rc = getDupPage(fileHandle, pageNum);
		if (rc != SUCC) {
			cerr << "insertDupRecord: get dup page error " << rc << endl;
			return rc;
		}
		rc = fileHandle.readPage(pageNum, page);
		if (rc != SUCC) {
			cerr << "insertDupRecord: read page error " << rc << endl;
			return rc;
		}
		// check the space availability
		spaceAvailable = ix->getFreeSpaceSize(page);
		if (loop > 10) {
			cerr << "insertDupRecord: fail to allocate new dup page " <<
					IX_FAILTO_ALLOCATE_PAGE << endl;
			return IX_FAILTO_ALLOCATE_PAGE;
		}
		++loop;
	} while (spaceAvailable < DUP_RECORD_SIZE);

	// set the page to be dup
	ix->setPageLeaf(page, IS_DUP_PAGE);

	SlotNum totalSlotNum = ix->getSlotNum(page);
	SlotNum slotNum2Insert = totalSlotNum+1;
	IndexDir indexDir;
	for (SlotNum sn = 1; sn <= totalSlotNum; ++sn) {
		rc = ix->getIndexDir(page, indexDir, sn);
		if (rc != SUCC) {
			cerr << "insertDupRecord: get slot dir error " << rc << endl;
			return rc;
		}
		if (indexDir.slotOffset == DUP_SLOT_DEL) {
			// hit a slot that is deleted
			slotNum2Insert = sn;
			break;
		}
	}

	char *point2InsertRecord = (char *)page + (slotNum2Insert-1) * DUP_RECORD_SIZE;
	memcpy(point2InsertRecord, &dupHeadRID, sizeof(RID));
	point2InsertRecord += sizeof(RID);
	memcpy(point2InsertRecord, &dataRID, sizeof(RID));
	point2InsertRecord += sizeof(RID);

	// check if need more slot space
	// if so need to set the new total slot num
	if (slotNum2Insert == totalSlotNum+1) {
		ix->setSlotNum(page, slotNum2Insert);
		// set the space start point
		ix->setFreeSpaceStartPoint(page, point2InsertRecord);
	}

	// set the slot dir to be in use
	indexDir.slotOffset = DUP_SLOT_IN_USE;
	rc = ix->setIndexDir(page, indexDir, slotNum2Insert);
	if (rc != SUCC) {
		cerr << "insertDupRecord: get slot dir error " << rc << endl;
		return rc;
	}

	// write page back
	rc = fileHandle.writePage(pageNum, page);
	if (rc != SUCC) {
		cerr << "insertDupRecord: write page error " << rc << endl;
		return rc;
	}

	// prepare returned value
	dupAssignedRID.pageNum = pageNum;
	dupAssignedRID.slotNum = slotNum2Insert;

	// push the page to the list
	spaceAvailable = ix->getFreeSpaceSize(page);
	if (spaceAvailable >= DUP_RECORD_SIZE) {
		putDupPage(fileHandle, pageNum);
	}

	return SUCC;
}
RC SpaceManager::deleteDupRecord(FileHandle &fileHandle,
		RID &dupHeadRID,
		const RID &dataRID) {
	IndexManager *ix = IndexManager::instance();
	RC rc;
	PageNum curPageNum = dupHeadRID.pageNum;
	SlotNum curSlotNum = dupHeadRID.slotNum;
	PageNum prevPageNum = dupHeadRID.pageNum;
	SlotNum prevSlotNum = dupHeadRID.slotNum;

	while (true) {
		rc = fileHandle.readPage(curPageNum, page);
		if (rc != SUCC) {
			cerr << "deleteDupRecord: try to read page " <<
					curPageNum <<
					" warning code " << rc << endl;
			return SUCC;
		}

		// get the total number of slot num
		SlotNum totalSlotNum = ix->getSlotNum(page);
		if (curSlotNum > totalSlotNum) {
			cerr << "try to delete a dup record, but does not exist in index" << endl;
			return SUCC;
		}

		// read the data rid and next rid
		RID curDataRid, nextRid;
		char *data = page + DUP_RECORD_SIZE * (curSlotNum-1);
		memcpy(&nextRid, data, sizeof(RID));
		data += sizeof(RID);
		memcpy(&curDataRid, data, sizeof(RID));

		if (curDataRid.pageNum != dataRID.pageNum ||
				curDataRid.slotNum != dataRID.slotNum) {
			if (curPageNum == DUP_PAGENUM_END) {
				// this is the last record
				// nothing hit
				// return
				return SUCC;
			}
			// not the record to be deleted
			// get the next dup record
			prevPageNum = curPageNum;
			prevSlotNum = curSlotNum;
			curPageNum = nextRid.pageNum;
			curSlotNum = nextRid.slotNum;
			continue;
		}

		// hit!
		// 1. need to reset the previous nextRID
		// 1.1 the deleted record is the headRID
		if (curPageNum == dupHeadRID.pageNum &&
				curSlotNum == dupHeadRID.slotNum) {
			dupHeadRID.pageNum = nextRid.pageNum;
			dupHeadRID.slotNum = nextRid.slotNum;
		} else {
			// 1.2 the last one/a middle one
			// need to change the next rid of previous page
			rc = fileHandle.readPage(prevPageNum, prevPage);
			if (rc != SUCC) {
				cerr << "deleteDupRecord: read previous page error " << rc << endl;
				return rc;
			}
			RID prevRID;
			// get the next RID info of current page
			data = page + DUP_RECORD_SIZE * (curSlotNum-1);
			memcpy(&prevRID, data, sizeof(RID));
			// set the next RID info of previous page
			data = prevPage + DUP_RECORD_SIZE * (prevSlotNum-1);
			memcpy(data, &prevRID, sizeof(RID));
			rc = fileHandle.writePage(prevPageNum, prevPage);
			if (rc != SUCC) {
				cerr << "deleteDupRecord: write previous page error " << rc << endl;
				return rc;
			}
			// debug info
			/*
			cout << "deleting data at page " << dataRID.pageNum <<
					"\t of slot " << dataRID.slotNum << endl;
			cout << ">>>>>curPageNum " << curPageNum <<
					"\tcurSlotNum " << curSlotNum <<
					"\tprevPageNum " << prevPageNum <<
					"\tprevSlotNum " << prevSlotNum <<
					"\tprevRID.pageNum " << prevRID.pageNum <<
					"\tprevRID.slotNum " << prevRID.slotNum <<
					endl;
					*/
		}

		// reread page in case it is covered by previous operation
		rc = fileHandle.readPage(curPageNum, page);
		if (rc != SUCC) {
			cerr << "deleteDupRecord: read page error " << rc << endl;
			return rc;
		}
		// 2. need to reset the index dir
		// get the Slot Dir;
		if (totalSlotNum < curSlotNum) {
			// this is not an error
			// output a warning and return success
			cerr << "deleteDupRecord: total slot num is less than current slot number " << rc << endl;
			return SUCC;
		}
		if (totalSlotNum == curSlotNum) {
			SlotNum slotDirShrink = 0, backSlotNum(curSlotNum-1);
			IndexDir indexDir;
			do {
				rc = ix->getIndexDir(page, indexDir, backSlotNum);
				++slotDirShrink;
				--backSlotNum;
			} while(indexDir.slotOffset == DUP_SLOT_DEL);
			// simply set the total slot num - 1
			ix->setSlotNum(page, totalSlotNum-slotDirShrink);
			// set the free space correspondingly
			char *startPoint = (char *)ix->getFreeSpaceStartPoint(page);
			startPoint -= DUP_RECORD_SIZE*slotDirShrink;
			ix->setFreeSpaceStartPoint(page, startPoint);

			if (startPoint == page) {
				// the page is empty
				// recollect it
				putEmptyPage(fileHandle, curPageNum);
			}
		} else {
			IndexDir indexDir;
			rc = ix->getIndexDir(page, indexDir, curSlotNum);
			if (rc != SUCC) {
				cerr << "deleteDupRecord: read slot dir error " << rc << endl;
				return rc;
			}
			// if the index dir is already deleted
			// then everything is done, just return
			if (indexDir.slotOffset == DUP_SLOT_DEL)
				return SUCC;

			// set the slot dir to be deleted
			indexDir.slotOffset = DUP_SLOT_DEL;
			rc = ix->setIndexDir(page, indexDir, curSlotNum);
			if (rc != SUCC) {
				cerr << "deleteDupRecord: set slot dir error " << rc << endl;
				return rc;
			}
		}

		// write page back
		rc = fileHandle.writePage(curPageNum, page);
		if (rc != SUCC) {
			cerr << "deleteDupRecord: write page error " << rc << endl;
			return rc;
		}

		// push the page to the list
		// if a page already exists, no need for it already fits
		// if a page does not exist, then push
		int spaceAvailable = ix->getFreeSpaceSize(page);
		if (!dupPageExist(fileHandle, curPageNum) &&
				spaceAvailable > DUP_RECORD_SIZE)
			putDupPage(fileHandle, curPageNum);

		// hit/finished so we return
		break;
	}

	return SUCC;
}

// get dup record page
RC SpaceManager::getDupPage(FileHandle &fileHandle,
		PageNum &pageNum) {
	RC rc;
	auto itr = dupRecordPageList.find(fileHandle.fileName);
	if (itr == dupRecordPageList.end() ||
			itr->second.empty()) {
		rc = getEmptyPage(fileHandle, pageNum);
		if (rc != SUCC) {
			cerr << "getDupPage: get empty page error " << rc << endl;
			return rc;
		}
	} else {
		pageNum = *(itr->second.begin());
		itr->second.erase(itr->second.begin());
	}

	return SUCC;
}
// put an available dup page to the list
// Note: must check the space availability before inserting
void SpaceManager::putDupPage(FileHandle &fileHandle,
		const PageNum &pageNum) {
	auto itr = dupRecordPageList.find(fileHandle.fileName);
	if (itr == dupRecordPageList.end()) {
		dupRecordPageList.insert(
				pair<string, unordered_set<PageNum> >(fileHandle.fileName, {}));
		itr = dupRecordPageList.find(fileHandle.fileName);
	}
	itr->second.insert(pageNum);
}
bool SpaceManager::dupPageExist(FileHandle &fileHandle,
		const PageNum &pageNum) {
	auto itr = dupRecordPageList.find(fileHandle.fileName);
	if (itr == dupRecordPageList.end() ||
			itr->second.count(pageNum) == 0) {
		return false;
	} else {
		return true;
	}
}
// get an empty page if there is one
// if no empty page exists, new one and return that
RC SpaceManager::getEmptyPage(FileHandle &fileHandle,
		PageNum &pageNum) {
	RC rc = SUCC;
	auto itr = emptyPageList.find(fileHandle.fileName);
	if (itr == emptyPageList.end() ||
			itr->second.empty()) {
		IndexManager *ix = IndexManager::instance();
		// no empty page, new one
		PageNum totalPageNum = fileHandle.getNumberOfPages();
		ix->setPageEmpty(page);
		rc = fileHandle.appendPage(page);
		if (rc != SUCC) {
			cerr << "getEmptyPage: get new page error " << rc << endl;
			return rc;
		}
		pageNum = totalPageNum;
		return SUCC;
	} else {
		pageNum = *(itr->second.begin());
		itr->second.erase(itr->second.begin());
		return SUCC;
	}
	return SUCC;
}
RC SpaceManager::putEmptyPage(FileHandle &fileHandle,
		const PageNum &pageNum) {
	IndexManager *ix = IndexManager::instance();
	RC rc;

	ix->setPageEmpty(page);

	// write page to disk
	rc = fileHandle.writePage(pageNum, page);
	if(rc != SUCC) {
		cerr << "putEmptyPage: get new page error " << rc << endl;
		return rc;
	}

	// save in the list
	auto itr = emptyPageList.find(fileHandle.fileName);
	if (itr == emptyPageList.end()) {
		emptyPageList.insert(
				pair<string, unordered_set<PageNum> >(fileHandle.fileName, {}));
		itr = emptyPageList.find(fileHandle.fileName);
	}
	itr->second.insert(pageNum);
	return SUCC;
}

