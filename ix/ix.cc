
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

	char page[PAGE_SIZE];
	setPageEmpty(page);
	setPageLeaf(page, CONST_IS_LEAF);
	rc = fileHandle.appendPage(page);
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
	char rootPage[PAGE_SIZE];
	char copiedUpKey[PAGE_SIZE];
	bool copiedUp = false;
	PageNum copiedUpNextPageNum;
	rc = insertEntry(ROOT_PAGE, fileHandle,
			attribute, key, rid,
			copiedUpKey, copiedUp, copiedUpNextPageNum);
	// read the root page
	rc = fileHandle.readPage(ROOT_PAGE, rootPage);
	if (rc != SUCC) {
		cerr << "IndexManager::insertEntry: readPage error " << rc << endl;
		return rc;
	}
	// get the type of root
	IsLeaf isLeaf = isPageLeaf(rootPage);
	if (copiedUp && isLeaf == CONST_NOT_LEAF) {
		int copiedUpKeyLen = getKeySize(attribute, copiedUpKey);
		int spaceNeeded = copiedUpKeyLen + sizeof(PageNum) + sizeof(IndexDir);
		int spaceAvailable = getFreeSpaceSize(rootPage);

		if (spaceNeeded <= spaceAvailable) {
			// just insert
			SlotNum slotNumToInsert;
			rc = binarySearchEntry(rootPage, attribute, copiedUpKey, slotNumToInsert);
			rc = insertEntryAtPos(rootPage, slotNumToInsert, attribute,
					copiedUpKey, copiedUpKeyLen, rid, false,
					0, copiedUpNextPageNum);
			if (rc != SUCC) {
				cerr << "IndexManager::insertEntry: insertEntryAtPos error 1 " << rc << endl;
				return rc;
			}
			spaceAvailable = getFreeSpaceSize(rootPage);
			// write the root page back
			rc = fileHandle.writePage(ROOT_PAGE, rootPage);
			if (rc != SUCC) {
				cerr << "IndexManager::insertEntry: writePage error (root page) " << rc << endl;
				return rc;
			}
		} else {
			// need to split the root again
			char leftPage[PAGE_SIZE];
			char rightPage[PAGE_SIZE];
			char movedUpKey[PAGE_SIZE];
			rc = splitPageNonLeaf(rootPage, leftPage, rightPage,
					attribute, copiedUpKey, copiedUpNextPageNum, movedUpKey);
			if (rc != SUCC) {
				cerr << "IndexManager::insertEntry: splitPageNonLeaf error " << rc << endl;
				return rc;
			}

			// get a new page for left page
			SpaceManager *sm = SpaceManager::instance();
			PageNum newLeftPageNum;
			rc = sm->getEmptyPage(fileHandle, newLeftPageNum);
			if (rc != SUCC) {
				cerr << "IndexManager::insertEntry: getEmptyPage error at left page " << rc << endl;
				return rc;
			}
			// get a new page for right page
			PageNum newRightPageNum;
			rc = sm->getEmptyPage(fileHandle, newRightPageNum);
			if (rc != SUCC) {
				cerr << "IndexManager::insertEntry: getEmptyPage error at left page " << rc << endl;
				return rc;
			}

			setNextPageNum(leftPage, newRightPageNum);
			setPrevPageNum(rightPage, newLeftPageNum);

			// write the new left page back
			rc = fileHandle.writePage(newLeftPageNum, leftPage);
			if (rc != SUCC) {
				cerr << "IndexManager::insertEntry: writePage error " << rc << endl;
				return rc;
			}

			// write the new right page back
			rc = fileHandle.writePage(newRightPageNum, rightPage);
			if (rc != SUCC) {
				cerr << "IndexManager::insertEntry: writePage error " << rc << endl;
				return rc;
			}

			// empty the root
			setPageEmpty(rootPage);
			setPageLeaf(rootPage, CONST_NOT_LEAF);

			// insert the very first record to the root
			int keyLen = getKeySize(attribute, movedUpKey);
			rc = insertEntryAtPos(rootPage, 1, attribute,
					movedUpKey, keyLen, rid, false,
					newLeftPageNum, newRightPageNum);
			if (rc != SUCC) {
				cerr << "IndexManager::insertEntry: insertEntryAtPos error " << rc << endl;
				return rc;
			}
			// write the root page back
			rc = fileHandle.writePage(ROOT_PAGE, rootPage);
			if (rc != SUCC) {
				cerr << "IndexManager::insertEntry: writePage error (root page) " << rc << endl;
				return rc;
			}
		}
	}
	else if (copiedUp && isLeaf == CONST_IS_LEAF) {
		// get a new page for left page
		SpaceManager *sm = SpaceManager::instance();
		char leftPage[PAGE_SIZE];
		char rightPage[PAGE_SIZE];
		PageNum newPageNum;
		rc = sm->getEmptyPage(fileHandle, newPageNum);
		if (rc != SUCC) {
			cerr << "IndexManager::insertEntry: getEmptyPage error " << rc << endl;
			return rc;
		}
		// copy root page to the left page
		memcpy(leftPage, rootPage, PAGE_SIZE);

		rc = fileHandle.readPage(copiedUpNextPageNum, rightPage);
		if (rc != SUCC) {
			cerr << "IndexManager::insertEntry: readPage (right page) error " << rc << endl;
			return rc;
		}

		// set the next page number
		setPrevPageNum(rightPage, newPageNum);
		setNextPageNum(leftPage, copiedUpNextPageNum);

		// write the new left page back
		rc = fileHandle.writePage(newPageNum, leftPage);
		if (rc != SUCC) {
			cerr << "IndexManager::insertEntry: writePage (left page) error " << rc << endl;
			return rc;
		}
		// write the new right page back
		rc = fileHandle.writePage(copiedUpNextPageNum, rightPage);
		if (rc != SUCC) {
			cerr << "IndexManager::insertEntry: writePage (right page) error " << rc << endl;
			return rc;
		}

		// empty the root
		setPageEmpty(rootPage);
		setPageLeaf(rootPage, CONST_NOT_LEAF);

		// insert the very first record to the root
		int keyLen = getKeySize(attribute, copiedUpKey);
		rc = insertEntryAtPos(rootPage, 1, attribute,
				copiedUpKey, keyLen, rid, false,
				newPageNum, copiedUpNextPageNum);
		if (rc != SUCC) {
			cerr << "IndexManager::insertEntry: insertEntryAtPos error " << rc << endl;
			return rc;
		}
		// write the root page back
		isLeaf = isPageLeaf(rootPage);
		rc = fileHandle.writePage(ROOT_PAGE, rootPage);
		if (rc != SUCC) {
			cerr << "IndexManager::insertEntry: writePage error (root page) " << rc << endl;
			return rc;
		}
	}
	return rc;
}

RC IndexManager::deleteEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	RC rc;
	rc = deleteEntry(ROOT_PAGE, fileHandle, attribute, key, rid);
	return rc;
}


// scan() returns an iterator to allow the caller to go through the results
// one by one in the range(lowKey, highKey).
// For the format of "lowKey" and "highKey", please see insertEntry()
// If lowKeyInclusive (or highKeyInclusive) is true, then lowKey (or highKey)
// should be included in the scan
// If lowKey is null, then the range is -infinity to highKey
// If highKey is null, then the range is lowKey to +infinity
RC IndexManager::scan(FileHandle &fileHandle,
    const Attribute &attribute,
    const void      *lowKey,
    const void      *highKey,
    bool			lowKeyInclusive,
    bool        	highKeyInclusive,
    IX_ScanIterator &ix_ScanIterator)
{
	// set the starting index
	ix_ScanIterator.setIndex(0);
	// set the attribute
	ix_ScanIterator.setAttribute(attribute);
	// get start rid
	RID rid;
	vector<RID> rids;
	// searchEntry(5) returns rid in index not data file
	RC rc_search = searchEntry(ROOT_PAGE, fileHandle, attribute,
			lowKey, rid);
	if (rc_search != IX_SEARCH_LOWER_BOUND &&
			rc_search != IX_SEARCH_UPPER_BOUND &&
			rc_search != IX_SEARCH_HIT &&
			rc_search != IX_SEARCH_HIT_MED) {
		cerr << "scan: error in finding the key in index " << rc_search << endl;
		return rc_search;
	}
	if (rc_search == IX_SEARCH_LOWER_BOUND ||
			rc_search == IX_SEARCH_HIT_MED) {
		lowKeyInclusive = true;
	} else if (rc_search == IX_SEARCH_UPPER_BOUND) {
		// lower key is above upper bound
		return SUCC;
	}

	char page[PAGE_SIZE];
	RC rc;

	bool skipFirst = true;

	while (true) {
		// read the page
		rc = fileHandle.readPage(rid.pageNum, page);
		if (rc != SUCC) {
			cerr << "scan: readPage error " << rc << endl;
			return rc;
		}
		// get the total slot number
		SlotNum totalSlotNum = getSlotNum(page);
		for (SlotNum sn = rid.slotNum; sn <= totalSlotNum; ++sn) {
			// get the index dir
			IndexDir indexDir;
			rc = getIndexDir(page, indexDir, sn);
			if (rc != SUCC) {
				cerr << "scan: getIndexDir error " << rc << endl;
				return rc;
			}
			char *key = page + indexDir.slotOffset;
			char *p_headRid = page + indexDir.slotOffset
					+ indexDir.recordLength - sizeof(Dup) - sizeof(RID);
			RID headRID;
			memcpy(&headRID, p_headRid, sizeof(RID));
			char *p_dup = page + indexDir.slotOffset
					+ indexDir.recordLength - sizeof(Dup);
			Dup dup = false;
			memcpy(&dup, p_dup, sizeof(Dup));

			int cmpResult;
			if (highKey == NULL) {
				cmpResult = -1;
			} else {
				cmpResult = compareKey(attribute, key, highKey);
			}
			if (highKeyInclusive && cmpResult > 0)
				return SUCC;
			if (!highKeyInclusive && cmpResult >= 0)
				return SUCC;

			if (skipFirst && lowKeyInclusive == false) { // i.e. IX_SEARCH_HIT!
				skipFirst = false;
				continue;
			}

			if (dup == false) {
				// not a duplicate key, good
				ix_ScanIterator.pushRIDKey(headRID, key, attribute);
			} else {
				// need to add duplicate records all
				SpaceManager *sm = SpaceManager::instance();
				RID dataRID;
				do {
					rc = sm->getNextDupRecord(fileHandle, headRID, dataRID);
					if (rc != SUCC) {
						cerr << "scan: getNextDupRecord error " << rc << endl;
						return rc;
					}
					ix_ScanIterator.pushRIDKey(dataRID, key, attribute);
				} while(headRID.pageNum != DUP_PAGENUM_END);
			}
		}
		rid.pageNum = getNextPageNum(page);
		rid.slotNum = 1;
		if (rid.pageNum == ROOT_PAGE) {
			return SUCC;
		}
	}
	//TODO
	return SUCC;
}

IX_ScanIterator::IX_ScanIterator() :
		curIndex(0)
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	if (curIndex >= rids.size()) {
		return IX_EOF;
	}
	IndexManager *ix = IndexManager::instance();
	int keyLen = ix->getKeySize(attribute, keys[curIndex]);
	memcpy(key, keys[curIndex], keyLen);
	rid.pageNum = rids[curIndex].pageNum;
	rid.slotNum = rids[curIndex].slotNum;

	++curIndex;
	return SUCC;
}

RC IX_ScanIterator::close()
{
	rids.clear();
	for (size_t i = 0; i < keys.size(); ++i) {
		delete []keys[i];
	}
	keys.clear();
	return SUCC;
}

void IX_ScanIterator::setAttribute(const Attribute &attr) {
	attribute.length = attr.length;
	attribute.name = attr.name;
	attribute.type = attr.type;
}

void IX_ScanIterator::pushRIDKey(const RID &rid,
		const char *key, const Attribute &attr) {
	IndexManager *ix = IndexManager::instance();
	rids.push_back(rid);
	int keyLen = ix->getKeySize(attr, key);
	char *newKey = new char[keyLen];
	memcpy(newKey, key, keyLen);
	keys.push_back(newKey);
}

void IX_PrintError (RC rc)
{
	//TODO
}

/*
 * Split/merge pages
 */
RC IndexManager::splitPageLeaf(void *oldPage,
		void *pageLeft, void *pageRight,
		const Attribute &attr,
		const void *key, const RID &rid,
		void *keyCopiedUp) {
	RC rc;
	setPageEmpty(pageLeft);
	setPageEmpty(pageRight);
	setPageLeaf(pageLeft, CONST_IS_LEAF);
	setPageLeaf(pageRight, CONST_IS_LEAF);
	int keyLen = getKeySize(attr, key);
	// get the total slot number
	SlotNum totalSlotNum = getSlotNum(oldPage);
	// get prev/next page number
	PageNum oldPrevPageNum = getPrevPageNum(oldPage);
	PageNum oldNextPageNum = getNextPageNum(oldPage);

	// split the entry
	SlotNum snLeft(1), snRight(1), halfSN(totalSlotNum/2);
	bool isInserted = false;
	for (SlotNum sn = 1; sn <= totalSlotNum; ++sn) {
		// get index dir
		IndexDir indexDir;
		rc = getIndexDir(oldPage, indexDir, sn);
		if (rc != SUCC) {
			cerr << "IndexManager::splitPage: get index dir error " << rc << endl;
			return rc;
		}
		// get the key
		char *curKey = (char *)oldPage + indexDir.slotOffset;
		unsigned curKeyLen = getKeySize(attr, curKey);
		if (compareKey(attr, key, curKey) < 0 && !isInserted) {
//			cout << "key is " << *(int*)key << endl;
//			cout << "curKey is " << *(int*)curKey << endl;
			isInserted = true;
			// need to first insert the key to be inserted
			if (sn < halfSN) {
				rc = insertEntryAtPos(pageLeft, snLeft,
						attr,
						key, keyLen,
						rid, false,
						0, 0);
				++snLeft;
			} else {
				rc = insertEntryAtPos(pageRight, snRight,
						attr,
						key, keyLen,
						rid, false,
						0, 0);
				++snRight;
			}
			if (rc != SUCC) {
				cerr << "IndexManager::splitPage: insertEntryAtPos error " << rc << endl;
				return rc;
			}
		}
		// get the rid of the current key
		RID curRid;
		char *data = (char *)oldPage + indexDir.slotOffset
				+ indexDir.recordLength - sizeof(Dup) - sizeof(RID);
		memcpy(&curRid, data, sizeof(RID));
		data += sizeof(RID);
		Dup curDup;
		memcpy(&curDup, data, sizeof(Dup));
		if (sn < halfSN) {
			rc = insertEntryAtPos(pageLeft, snLeft,
					attr,
					curKey, curKeyLen,
					curRid, curDup,
					0,0);
			++snLeft;
		} else {
			rc = insertEntryAtPos(pageRight, snRight,
					attr,
					curKey, curKeyLen,
					curRid, curDup,
					0, 0);
			++snRight;
		}
		if (rc != SUCC) {
			cerr << "IndexManager::splitPage: insertEntryAtPos error " << rc << endl;
			return rc;
		}
	}

	if (!isInserted) {
		rc = insertEntryAtPos(pageRight, snRight,
				attr,
				key, keyLen,
				rid, false,
				0, 0);
		if (rc != SUCC) {
			cerr << "IndexManager::splitPage: insertEntryAtPos error " << rc << endl;
			return rc;
		}
	}
	// set the new page's prev and next page
	setPrevPageNum(pageLeft, oldPrevPageNum);
	setNextPageNum(pageRight, oldNextPageNum);

	// copy the key of the right page at the top
	keyLen = getKeySize(attr, pageRight);
	memcpy(keyCopiedUp, pageRight, keyLen);

	return SUCC;
}
RC IndexManager::splitPageNonLeaf(void *oldPage,
		  void *pageLeft, void *pageRight,
		  const Attribute &attr,
		  const void *key, const PageNum &nextPageNum,
		  void *keyMovedUp) {
	RC rc;
	setPageEmpty(pageLeft);
	setPageEmpty(pageRight);
	setPageLeaf(pageLeft, CONST_NOT_LEAF);
	setPageLeaf(pageRight, CONST_NOT_LEAF);
	int keyLen = getKeySize(attr, key);
	// get the total slot number
	SlotNum totalSlotNum = getSlotNum(oldPage);
	// split the entry
	SlotNum snLeft(1), snRight(1), halfSN(totalSlotNum/2);
	bool isInserted = false;
	// get prev/next page number
	PageNum oldPrevPageNum = getPrevPageNum(oldPage);
	PageNum oldNextPageNum = getNextPageNum(oldPage);

	// random rid
	RID rid; rid.pageNum = -1, rid.slotNum = -1;
	PageNum prevPageNum = *((PageNum *)oldPage);

	for (SlotNum sn = 1; sn < halfSN; ++sn) {
		// get index dir
		IndexDir indexDir;
		rc = getIndexDir(oldPage, indexDir, sn);
		if (rc != SUCC) {
			cerr << "IndexManager::splitPageNonLeaf: get index dir error " << rc << endl;
			return rc;
		}

		// get the key
		char *curKey = (char *)oldPage + indexDir.slotOffset;
		unsigned curKeyLen = getKeySize(attr, curKey);
		if (compareKey(attr, key, curKey) < 0 && !isInserted) {
			isInserted = true;
			// need to first insert the key to be inserted
			rc = insertEntryAtPos(pageLeft, snLeft,
					attr,
					key, keyLen,
					rid, false,
					prevPageNum, nextPageNum);
			++snLeft;
			if (rc != SUCC) {
				cerr << "IndexManager::splitPageNonLeaf: insertEntryAtPos error " << rc << endl;
				return rc;
			}
		}

		// get the next page number
		char *p_curNextPagenum = (char *)oldPage + indexDir.slotOffset
				+ indexDir.recordLength - sizeof(PageNum);
		PageNum curNextPageNum = *((PageNum *)p_curNextPagenum);
		rc = insertEntryAtPos(pageLeft, snLeft,
				attr,
				curKey, curKeyLen,
				rid, false,
				prevPageNum,curNextPageNum);
		++snLeft;
	}

	// copy the moved up key
	// get index dir
	IndexDir indexDir;
	rc = getIndexDir(oldPage, indexDir, halfSN);
	if (rc != SUCC) {
		cerr << "IndexManager::splitPageNonLeaf: get index dir error " << rc << endl;
		return rc;
	}
	// copy the moved up key
	char *curKey = (char *)oldPage + indexDir.slotOffset;
	unsigned curKeyLen = getKeySize(attr, curKey);
	bool insetedKeyMovedUp = false;
	if (compareKey(attr, key, curKey) < 0 && !isInserted) {
		isInserted = true;
		unsigned keyLen = getKeySize(attr, key);
		memcpy(keyMovedUp, key, keyLen);
		halfSN--; // do not move up the halfSN-th element
		prevPageNum = nextPageNum;
		insetedKeyMovedUp = true; // if the inserted key is moved up
		// the previous page num is different
	} else {
		memcpy(keyMovedUp, curKey, curKeyLen);
	}

	// copy the right page
	for (SlotNum sn = halfSN+1; sn <= totalSlotNum; ++sn) {
		// get index dir
		IndexDir indexDir;
		rc = getIndexDir(oldPage, indexDir, sn);
		if (rc != SUCC) {
			cerr << "IndexManager::splitPageNonLeaf: get index dir error " << rc << endl;
			return rc;
		}

		// get the key
		char *curKey = (char *)oldPage + indexDir.slotOffset;
		unsigned curKeyLen = getKeySize(attr, curKey);
		char *curPrevPageNum = curKey - sizeof(PageNum);
		if (!insetedKeyMovedUp)
			prevPageNum = *((PageNum *)curPrevPageNum);
		if (compareKey(attr, key, curKey) < 0 && !isInserted) {
			isInserted = true;
			// need to first insert the key to be inserted
			rc = insertEntryAtPos(pageRight, snRight,
					attr,
					key, keyLen,
					rid, false,
					prevPageNum, nextPageNum);
			++snRight;
			if (rc != SUCC) {
				cerr << "IndexManager::splitPageNonLeaf: insertEntryAtPos error " << rc << endl;
				return rc;
			}
		}

		// get the next page number
		char *p_curNextPagenum = (char *)oldPage + indexDir.slotOffset
				+ indexDir.recordLength - sizeof(PageNum);
		PageNum curNextPageNum = *((PageNum *)p_curNextPagenum);
		rc = insertEntryAtPos(pageRight, snRight,
				attr,
				curKey, curKeyLen,
				rid, false,
				prevPageNum,curNextPageNum);
		++snRight;
	}

	if (!isInserted) {
		rc = insertEntryAtPos(pageRight, snRight,
				attr,
				key, keyLen,
				rid, false,
				prevPageNum, nextPageNum);
		if (rc != SUCC) {
			cerr << "IndexManager::splitPage: insertEntryAtPos error " << rc << endl;
			return rc;
		}
	}

	// set the new page's prev and next page
	setPrevPageNum(pageLeft, oldPrevPageNum);
	setNextPageNum(pageRight, oldNextPageNum);
	return SUCC;
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
	setPageLeaf(page, CONST_NOT_LEAF);
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
	IsLeaf isLeaf(CONST_NOT_LEAF);
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
		const void *key, const RID &rid,
		void *copiedUpKey, bool &copiedUp,
		PageNum &copiedUpNextPageNum) {
	copiedUp = false;
	RC rc;
	SpaceManager *sm = SpaceManager::instance();
	char page[PAGE_SIZE];

	rc = fileHandle.readPage(pageNum, page);
	if (rc != SUCC) {
		cerr << "IndexManager::insertEntry: error read page " << rc << endl;
		return rc;
	}
	IsLeaf pageType = isPageLeaf(page);
	if (pageType == CONST_IS_DUP_PAGE) {
		cerr << "IndexManager::insertEntry: read dup page " << IX_READ_DUP_PAGE << endl;
		return IX_READ_DUP_PAGE;
	}

	if (isPageEmpty(page) && pageNum == ROOT_PAGE) {
		// the very first data, just insert and return
		// set the data
		char *data = (char *)page;
		int keyLen = getKeySize(attribute, key);
		memcpy(data, key, keyLen);
		data += keyLen;
		memcpy(data, &rid, sizeof(RID));
		data += sizeof(RID);
		Dup dup = false;
		memcpy(data, &dup, sizeof(Dup));

		// set the index slot dir
		setSlotNum(page, 1);
		IndexDir indexDir;
		indexDir.recordLength = keyLen + sizeof(RID) + sizeof(Dup);
		indexDir.slotOffset = 0;
		setIndexDir(page, indexDir, 1);

		// set start point
		data = page + indexDir.recordLength;
		setFreeSpaceStartPoint(page, data);

		// set page type as leaf
		setPageLeaf(page, CONST_IS_LEAF);

		// write disk
		rc = fileHandle.writePage(pageNum, page);
		if (rc != SUCC) {
			cerr << "IndexManager::insertEntry: error write root page " << rc << endl;
			return rc;
		}
		return SUCC;
	}

	// search the key
	SlotNum slotNum;
	RC rc_search = binarySearchEntry(page, attribute, key, slotNum);

	if (pageType == CONST_IS_LEAF) {
		if (rc_search == IX_SEARCH_HIT) {
			IndexDir indexDir;
			rc = getIndexDir(page, indexDir, slotNum);
			if (rc != SUCC) {
				cerr << "request slot num " << slotNum << endl;
				cerr << "while there are only " << getSlotNum(page) << endl;
				cerr << "IndexManager::insertEntry: get index dir error " << rc << endl;
				return rc;
			}
			char *data = page + indexDir.slotOffset;
			// hit, now check duplication
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
			rc = fileHandle.writePage(pageNum, page);
			if (rc != SUCC) {
				cerr << "IndexManager::insertEntry: write page error 1 " << rc << endl;
				return rc;
			}
		} else { // still in the leaf
			// not hit, need to insert
			// first: need to see if the page fits the entry
			int keyLen = getKeySize(attribute, key);
			int spaceNeeded = keyLen + sizeof(RID) + sizeof(Dup) + sizeof(IndexDir);
			int spaceAvailable = getFreeSpaceSize(page);
			if (spaceAvailable >= spaceNeeded) {
				// just insert, that page fits
				// make copiedUp false as it just fits, no need to split
				copiedUp = false;

				rc = insertEntryAtPos(page, slotNum, attribute,
						key, keyLen, rid, false, 0, 0);
				if (rc != SUCC) {
					cerr << "IndexManager::insertEntry: insert entry error 3 " << rc << endl;
					return rc;
				}
				// write to the page
				rc = fileHandle.writePage(pageNum, page);
				if (rc != SUCC) {
					cerr << "IndexManager::insertEntry: write page error 2 " << rc << endl;
					return rc;
				}
			} else {
				// the leaf page does not fit
				// must split
				char leftPage[PAGE_SIZE];
				char rightPage[PAGE_SIZE];
				copiedUp = true;
				rc = splitPageLeaf(page, leftPage, rightPage,
						attribute, key, rid, copiedUpKey);
				if (rc != SUCC) {
					cerr << "IndexManager::insertEntry: splitPageLeaf error " << rc << endl;
					return rc;
				}
				// get an empty page to hold the right page
				rc = sm->getEmptyPage(fileHandle, copiedUpNextPageNum);
				if (rc != SUCC) {
					cerr << "IndexManager::insertEntry: getEmptyPage error " << rc << endl;
					return rc;
				}
				PageNum origNextPageNum = getNextPageNum(page);
				PageNum origPrevPageNum = getPrevPageNum(page);
				setNextPageNum(rightPage, origNextPageNum);
				setPrevPageNum(rightPage, pageNum);
				// write back to the disk
				rc = fileHandle.writePage(copiedUpNextPageNum, rightPage);
				if (rc != SUCC) {
					cerr << "IndexManager::insertEntry: writePage error " << rc << endl;
					return rc;
				}

				setNextPageNum(leftPage, copiedUpNextPageNum);
				setPrevPageNum(leftPage, origPrevPageNum);
				// write to the page
				rc = fileHandle.writePage(pageNum, leftPage);
				if (rc != SUCC) {
					cerr << "IndexManager::insertEntry: write page error 1 " << rc << endl;
					return rc;
				}

				// load next page affected, reset its page pointer
				if (origNextPageNum != ROOT_PAGE) {
					rc = fileHandle.readPage(origNextPageNum, rightPage);
					if (rc != SUCC) {
						cerr << "IndexManager::insertEntry: readPage(origNextPageNum, leftPage) error " << rc << endl;
						return rc;
					}
					setPrevPageNum(rightPage, copiedUpNextPageNum);
					// write changes
					rc = fileHandle.writePage(origNextPageNum, rightPage);
					if (rc != SUCC) {
						cerr << "IndexManager::insertEntry: writePage(origNextPageNum, leftPage) error " << rc << endl;
						return rc;
					}
				}
			}
		}
	} else {
		// continue search until reach a leaf
		// |p|key|p|key|p|key|p|
		IndexDir indexDir;
		SlotNum slotNumtoLookat = slotNum;
		SlotNum totalSlotNum = getSlotNum(page);
		if (slotNum > totalSlotNum) {
			// here we need the slot number to get the entry
			// the entry has page number points to its right child
			slotNumtoLookat = totalSlotNum;
		}
		rc = getIndexDir(page, indexDir, slotNumtoLookat);
		if (rc != SUCC) {
			cerr << "request slot num " << slotNumtoLookat << endl;
			cerr << "while there are only " << getSlotNum(page) << endl;
			cerr << "IndexManager::insertEntry: get index dir error " << rc << endl;
			return rc;
		}
		char *data = page + indexDir.slotOffset;
		if (rc_search == IX_SEARCH_LOWER_BOUND ||
				rc_search == IX_SEARCH_HIT_MED) {
			// look at left
			data -= sizeof(PageNum);

		} else if (rc_search == IX_SEARCH_UPPER_BOUND ||
				rc_search == IX_SEARCH_HIT) {
			// look at right
			data += (indexDir.recordLength - sizeof(PageNum));
		}
		PageNum nextPageNum;
		memcpy(&nextPageNum, data, sizeof(PageNum));
		rc = insertEntry(nextPageNum,
				fileHandle,
				attribute, key, rid,
				copiedUpKey, copiedUp, copiedUpNextPageNum);
		if (rc != SUCC) {
			cerr << "IndexManager::insertEntry: next insert index entry error 2 " << rc << endl;
			return rc;
		}
		if (copiedUp) {
			// its child split
			// begin insert the copied key to the nonleaf page
			// first check the available size
			int keyLen = getKeySize(attribute, copiedUpKey);
			int spaceNeeded = keyLen + sizeof(PageNum) + sizeof(IndexDir);
			int spaceAvailable = getFreeSpaceSize(page);
			if (spaceNeeded <= spaceAvailable) {
				// good, it can fit into the non leaf page
				// just insert
				copiedUp = false;
				rc = insertEntryAtPos(page, slotNum, attribute,
						copiedUpKey, keyLen, rid, false, 0, copiedUpNextPageNum);
				if (rc != SUCC) {
					cerr << "IndexManager::insertEntry: insert entry error 1 " << rc << endl;
					return rc;
				}
				// write to the page
				rc = fileHandle.writePage(pageNum, page);
				if (rc != SUCC) {
					cerr << "IndexManager::insertEntry: write page error 2 " << rc << endl;
					return rc;
				}
			} else {
				// no space for the insertion, must split again at non leaf page
				if (pageNum == ROOT_PAGE) {
					// the overflow page is the root page, process it outside
					// if the page is not the root page, the left page is not copied
					// this is the difference and reason to process root separately
					return SUCC;
				}
				// split nonleaf page
				char leftPage[PAGE_SIZE];
				char rightPage[PAGE_SIZE];
				char keyMovedUp[PAGE_SIZE];
				PageNum movedUpNextPageNum;

				rc = splitPageNonLeaf(page, leftPage, rightPage,
						attribute, copiedUpKey, copiedUpNextPageNum, keyMovedUp);
				if (rc != SUCC) {
					cerr << "IndexManager::insertEntry: splitPageLeaf error " << rc << endl;
					return rc;
				}
				// get the prev and next page number
				PageNum origNextPageNum = getNextPageNum(page);
				PageNum origPrevPageNum = getPrevPageNum(page);

				// get an empty page to hold the right page
				rc = sm->getEmptyPage(fileHandle, movedUpNextPageNum);
				if (rc != SUCC) {
					cerr << "IndexManager::insertEntry: getEmptyPage error " << rc << endl;
					return rc;
				}
				setNextPageNum(rightPage, origNextPageNum);
				setPrevPageNum(rightPage, pageNum);
				// write the right page back to the disk
				rc = fileHandle.writePage(movedUpNextPageNum, rightPage);
				if (rc != SUCC) {
					cerr << "IndexManager::insertEntry: writePage error " << rc << endl;
					return rc;
				}

				setNextPageNum(leftPage, movedUpNextPageNum);
				setPrevPageNum(leftPage, origPrevPageNum);
				// write to the left page
				rc = fileHandle.writePage(pageNum, leftPage);
				if (rc != SUCC) {
					cerr << "IndexManager::insertEntry: write page error 1 " << rc << endl;
					return rc;
				}

				copiedUp = true;
				copiedUpNextPageNum = movedUpNextPageNum;
				copyKey(copiedUpKey, keyMovedUp, attribute);

				// load next page affected
				if (origNextPageNum != ROOT_PAGE) {
					rc = fileHandle.readPage(origNextPageNum, rightPage);
					if (rc != SUCC) {
						cerr << "IndexManager::insertEntry: readPage(origNextPageNum, leftPage) error " << rc << endl;
						return rc;
					}
					setPrevPageNum(rightPage, movedUpNextPageNum);
					// write changes
					rc = fileHandle.writePage(origNextPageNum, rightPage);
					if (rc != SUCC) {
						cerr << "IndexManager::insertEntry: writePage(origNextPageNum, leftPage) error " << rc << endl;
						return rc;
					}
				}
			}
		}
	}

	return SUCC;
}
// Delete index entry
RC IndexManager::deleteEntry(const PageNum &pageNum,
		FileHandle &fileHandle,
		const Attribute &attribute,
		const void *key, const RID &rid) {
	RC rc;
	char page[PAGE_SIZE];

	rc = fileHandle.readPage(pageNum, page);
	if (rc != SUCC) {
		cerr << "IndexManager::deleteEntry: error read page " << rc << endl;
		return rc;
	}
	IsLeaf pageType = isPageLeaf(page);
	if (pageType == CONST_IS_DUP_PAGE) {
		cerr << "IndexManager::deleteEntry: read dup page " << IX_READ_DUP_PAGE << endl;
		return IX_READ_DUP_PAGE;
	}

	// search the key
	SlotNum slotNum;
	RC rc_search = binarySearchEntry(page, attribute, key, slotNum);

	if (pageType == CONST_IS_LEAF) {
		// a leaf page
		if (rc_search == IX_SEARCH_HIT) {
			// if hit, good just delete and return
			IndexDir indexDir;
			rc = getIndexDir(page, indexDir, slotNum);
			if (rc != SUCC) {
				cerr << "IndexManager::deleteEntry: getIndexDir error " << rc << endl;
				return rc;
			}
			char *data = page + indexDir.slotOffset
					+ indexDir.recordLength - sizeof(Dup);
			Dup dup;
			memcpy(&dup, data, sizeof(Dup));
			if (dup == false) {
				// good it is not duplicate, just delete
				rc = deleteEntryAtPos(page, slotNum);
				if (rc != SUCC) {
					return rc;
				}
				rc = fileHandle.writePage(pageNum, page);
				if (rc != SUCC) {
					cerr << "IndexManager::deleteEntry: writePage error " << rc << endl;
					return rc;
				}
				return SUCC;
			} else {
				// the data is duplicated, must find the one that is to be del
				SpaceManager *sm = SpaceManager::instance();
				RID dupHeadRID;
				data = page + indexDir.slotOffset
						+ indexDir.recordLength - sizeof(Dup) - sizeof(RID);
				memcpy(&dupHeadRID, data, sizeof(RID));
				// delete the targeted record
				rc = sm->deleteDupRecord(fileHandle, dupHeadRID, rid);
				if (rc != SUCC) {
					// the record does not exist
					return rc;
				}
				if (dupHeadRID.pageNum != DUP_PAGENUM_END) {
					// update the dup head rid
					// there are more than 1 word duplicated
					memcpy(data, &dupHeadRID, sizeof(RID));
				} else {
					// there is only one word left
					deleteEntryAtPos(page, slotNum);
				}
				rc = fileHandle.writePage(pageNum, page);
				if (rc != SUCC) {
					cerr << "IndexManager::deleteEntry: writePage error " << rc << endl;
					return rc;
				}
			}
		} else {
			return rc_search;
		}
	} else if (pageType == CONST_NOT_LEAF) {
		IndexDir indexDir;
		SlotNum slotNumtoLookat = slotNum;
		SlotNum totalSlotNum = getSlotNum(page);
		if (slotNum > totalSlotNum) {
			// here we need the slot number to get the entry
			// the entry has page number points to its right child
			slotNumtoLookat = totalSlotNum;
		}
		rc = getIndexDir(page, indexDir, slotNumtoLookat);
		if (rc != SUCC) {
			cerr << "request slot num " << slotNumtoLookat << endl;
			cerr << "while there are only " << getSlotNum(page) << endl;
			cerr << "IndexManager::deleteEntry: get index dir error " << rc << endl;
			return rc;
		}
		char *data = page + indexDir.slotOffset;

		if (rc_search == IX_SEARCH_HIT ||
				rc_search == IX_SEARCH_UPPER_BOUND) {
			// look at right
			data += (indexDir.recordLength - sizeof(PageNum));
		} else if (rc_search == IX_SEARCH_LOWER_BOUND ||
				rc_search == IX_SEARCH_HIT_MED){
			// look at left
			data -= sizeof(PageNum);
		}

		PageNum nextPageNum;
		memcpy(&nextPageNum, data, sizeof(PageNum));
		rc = deleteEntry(nextPageNum, fileHandle, attribute,
				key, rid);
		if (rc != SUCC) {
//			cerr << "IndexManager::deleteEntry: next insert index entry error 1 " << rc << endl;
			return rc;
		}
	}

	return IX_DEL_FAILURE;
}

// searchEntry(5) returns rid in index not data file
RC IndexManager::searchEntry(const PageNum &pageNum,
		FileHandle &fileHandle,
		const Attribute &attribute,
		const void *key, RID &rid) {
	RC rc;
	char page[PAGE_SIZE];

	rc = fileHandle.readPage(pageNum, page);
	if (rc != SUCC) {
		cerr << "IndexManager::searchEntry: error read page " << rc << endl;
		return rc;
	}
	IsLeaf pageType = isPageLeaf(page);
	if (pageType == CONST_IS_DUP_PAGE) {
		cerr << "IndexManager::searchEntry: read dup page " << IX_READ_DUP_PAGE << endl;
		return IX_READ_DUP_PAGE;
	}

	// search the key
	SlotNum slotNum;
	RC rc_search;
	if (key != NULL) {
		rc_search = binarySearchEntry(page, attribute, key, slotNum);
	} else {
		rc_search = IX_SEARCH_LOWER_BOUND;
		slotNum = 1;
	}

	if (pageType == CONST_IS_LEAF) {
		// a leaf page
		if (rc_search == IX_SEARCH_HIT ||
				rc_search == IX_SEARCH_HIT_MED ||
				rc_search == IX_SEARCH_LOWER_BOUND) {
			// if hit, good just find and return
			rid.pageNum = pageNum;
			rid.slotNum = slotNum;
			return rc_search;
		} else if (rc_search == IX_SEARCH_UPPER_BOUND){
			rid.pageNum = IX_EOF;
			rid.slotNum = IX_EOF;
			return rc_search;
		}
	} else if (pageType == CONST_NOT_LEAF) {
		IndexDir indexDir;
		SlotNum slotNumtoLookat = slotNum;
		SlotNum totalSlotNum = getSlotNum(page);
		if (slotNum > totalSlotNum) {
			// here we need the slot number to get the entry
			// the entry has page number points to its right child
			slotNumtoLookat = totalSlotNum;
		}
		rc = getIndexDir(page, indexDir, slotNumtoLookat);
		if (rc != SUCC) {
			cerr << "request slot num " << slotNumtoLookat << endl;
			cerr << "while there are only " << getSlotNum(page) << endl;
			cerr << "IndexManager::searchEntry: get index dir error " << rc << endl;
			return rc;
		}
		char *data = page + indexDir.slotOffset;

		if (rc_search == IX_SEARCH_HIT ||
				rc_search == IX_SEARCH_UPPER_BOUND) {
			// look at right
			data += (indexDir.recordLength - sizeof(PageNum));
		} else if (rc_search == IX_SEARCH_LOWER_BOUND ||
				rc_search == IX_SEARCH_HIT_MED){
			// look at left
			data -= sizeof(PageNum);
		}

		PageNum nextPageNum;
		memcpy(&nextPageNum, data, sizeof(PageNum));
		rc_search = searchEntry(nextPageNum, fileHandle, attribute, key, rid);
		return rc_search;
	}
	return rc_search;
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


	if (totalSlotNum == 0) {
		slotNum = 1;
		return IX_SEARCH_LOWER_BOUND;
	}

	IndexDir indexDir;
	// indexKey >= key (target)
	rc = getIndexDir(page, indexDir, 1);
	if (rc != SUCC) {
		cerr << "binarySearchEntry: error set slot dir " << rc << endl;
		return rc;
	}
	char *indexKey = (char *)page + indexDir.slotOffset;
	int compResult = compareKey(attr, indexKey, key);
	if (compResult == 0) {
		slotNum = 1;
		return IX_SEARCH_HIT;
	}
	else if (compResult > 0) {
		slotNum = 1;
	    return IX_SEARCH_LOWER_BOUND;
	}

	// indexKey < key (target)
	rc = getIndexDir(page, indexDir, totalSlotNum);
	if (rc != SUCC) {
		cerr << "binarySearchEntry: error set slot dir " << rc << endl;
		return rc;
	}
	indexKey = (char *)page + indexDir.slotOffset;
	compResult = compareKey(attr, indexKey, key);
	if (compResult == 0) {
		slotNum = totalSlotNum;
		return IX_SEARCH_HIT;
	}
	if (compResult < 0) {
		slotNum = totalSlotNum + 1;
		return IX_SEARCH_UPPER_BOUND;
	}

	while (true) {
		int cmpRltLeft(0), cmpRltRight(0);
		med = beg + (end - beg) / 2;
		// hit case
		rc = getIndexDir(page, indexDir, med);
		if (rc != SUCC) {
			cerr << "binarySearchEntry: error set slot dir " << rc << endl;
			return rc;
		}
		indexKey = (char *)page + indexDir.slotOffset;
		cmpRltLeft = compareKey(attr, indexKey, key);
		if (cmpRltLeft == 0) {
			slotNum = med;
			return IX_SEARCH_HIT;
		}

		if (med > 1) {
			rc = getIndexDir(page, indexDir, med - 1);
			if (rc != SUCC) {
				cerr << "binarySearchEntry: error set slot dir " << rc << endl;
				return rc;
			}
			indexKey = (char *)page + indexDir.slotOffset;
			cmpRltRight = compareKey(attr, key, indexKey);
		} else { // compare with an infinity small number
			cmpRltRight = 1;
		}

		// A[mid] >= target && target > A[mid - 1]
		if (cmpRltLeft > 0 && cmpRltRight > 0) {
			slotNum = med;
			return IX_SEARCH_HIT_MED;
		} else if (cmpRltLeft > 0) {
			end = med - 1;
		} else {
			beg = med + 1;
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
	if (lhs == NULL && rhs == NULL) {
		return -1;
	} else if (lhs == NULL) {
		return -1;
	} else if (rhs == NULL) {
		return 1;
	}
	char *str_lhs(NULL), *str_rhs(NULL);
	int result(0);
	float dResult(0.0);
	switch(attr.type) {
	case TypeInt:
		return *((int *)lhs) - *((int *)rhs);
		break;
	case TypeReal:
		dResult = *((float *)lhs) - *((float *)rhs);
		if (dResult > 0)
			return 1;
		else if (dResult < 0)
			return -1;
		else
			return 0;
		return result;
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
void IndexManager::copyKey(void *dest, const void *src,
		const Attribute &attr) {
	int len(0);
	switch(attr.type) {
	case TypeInt:
		memcpy(dest, src, sizeof(int));
		break;
	case TypeReal:
		memcpy(dest, src, sizeof(float));
		break;
	case TypeVarChar:
		len = *((int *)src);
		memcpy(dest, src, len + sizeof(int));
		break;
	}
}



// for debug only
void IndexManager::printPage(const void *page, const Attribute &attr) {
	IsLeaf isLeaf = isPageLeaf(page);
	if (isLeaf == CONST_IS_LEAF)
		printLeafPage(page, attr);
	else if (isLeaf == CONST_NOT_LEAF)
		printNonLeafPage(page, attr);
	else if (isLeaf == CONST_IS_DUP_PAGE)
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
	cout << "Previous page number: " << *((PageNum *)page) << endl;
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
	char entry[PAGE_SIZE];
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
		memcpy(entry, (char *)key+sizeof(int), len);
		entry[len] = '\0';
		cout << entry;
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
		if(isDupPage == CONST_IS_DUP_PAGE) {
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
	ix->setPageLeaf(page, CONST_IS_DUP_PAGE);

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
			return rc;
		}

		// get the total number of slot num
		SlotNum totalSlotNum = ix->getSlotNum(page);
		if (curSlotNum > totalSlotNum) {
			cerr << "try to delete a dup record, but does not exist in index" << endl;
			return IX_DEL_FAILURE;
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
				return IX_DEL_FAILURE;
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
			return IX_DEL_FAILURE;
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
				return IX_DEL_FAILURE;

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

RC SpaceManager::getNextDupRecord(FileHandle &fileHandle,
		RID &dupHeadRID,
		RID &dataRID) {
	IndexManager *ix = IndexManager::instance();
	RC rc;

	char page[PAGE_SIZE];
	rc = fileHandle.readPage(dupHeadRID.pageNum, page);
	if (rc != SUCC) {
		cerr << "try to read page num " << dupHeadRID.pageNum << endl;
		cerr << "getNextDupRecord:readPage error " << rc << endl;
		return rc;
	}

	IndexDir indexDir;
	rc = ix->getIndexDir(page, indexDir, dupHeadRID.slotNum);
	if (rc != SUCC) {
		cerr << "getNextDupRecord:getIndexDir error " << rc << endl;
		return rc;
	}

	if (indexDir.slotOffset == DUP_SLOT_DEL) {
		return IX_SEARCH_NOT_HIT;
	}

	char *data = page + DUP_RECORD_SIZE * (dupHeadRID.slotNum-1);
	memcpy(&dataRID, data + sizeof(RID), sizeof(RID));
	memcpy(&dupHeadRID, data, sizeof(RID));

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

