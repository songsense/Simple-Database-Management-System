
#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;

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
	return PagedFileManager::instance()->createFile(fileName.c_str());
}

RC IndexManager::destroyFile(const string &fileName)
{
	return PagedFileManager::instance()->destroyFile(fileName.c_str());
}

RC IndexManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	return PagedFileManager::instance()->openFile(fileName.c_str(), fileHandle);
}

RC IndexManager::closeFile(FileHandle &fileHandle)
{
	return PagedFileManager::instance()->closeFile(fileHandle);
}

RC IndexManager::insertEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	//TODO
	return -1;
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

void IndexManager::setPageEmpty(void *page) {
	setFreeSpaceStartPoint(page, page);
	setNextPageNum(page, 0);
	setPrevPageNum(page, 0);
	setPageLeaf(page, false);
	setSlotNum(page, 0);
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

// set/get prev/next page number
PageNum IndexManager::getPrevPageNum(const void *page) {
	char *data = (char *)page + PAGE_SIZE - sizeof(Offset)
			- sizeof(PageNum);
	PageNum pageNum(EOF_PAGE_NUM);
	memcpy(&pageNum, data, sizeof(PageNum));
	return pageNum;
}
PageNum IndexManager::getNextPageNum(const void *page) {
	char *data = (char *)page + PAGE_SIZE - sizeof(Offset)
			- sizeof(PageNum)*2;
	PageNum pageNum(EOF_PAGE_NUM);
	memcpy(&pageNum, data, sizeof(PageNum));
	return pageNum;
}
void IndexManager::setPrevPageNum(void *page,
		const PageNum &pageNum) {
	char *data = (char *)page + PAGE_SIZE - sizeof(Offset)
			- sizeof(PageNum);
	memcpy(data, &pageNum, sizeof(PageNum));
}
void IndexManager::setNextPageNum(void *page, const PageNum &pageNum) {
	char *data = (char *)page + PAGE_SIZE - sizeof(Offset)
			- sizeof(PageNum)*2;
	memcpy(data, &pageNum, sizeof(PageNum));
}

bool IndexManager::isPageLeaf(const void *page) {
	char *data = (char *)page + PAGE_SIZE
			- sizeof(Offset) - sizeof(PageNum)*2
			- sizeof(IsLeaf);
	bool isLeaf(false);
	memcpy(&isLeaf, data, sizeof(IsLeaf));
	return isLeaf;
}
void IndexManager::setPageLeaf(void *page, const bool &isLeaf) {
	char *data = (char *)page + PAGE_SIZE
			- sizeof(Offset) - sizeof(PageNum)*2
			- sizeof(IsLeaf);
	memcpy(data, &isLeaf, sizeof(IsLeaf));
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
	memcpy(data, &slotNum, sizeof(slotNum));
}
RC IndexManager::getSlotDir(const void *page,
		IndexDir &indexDir,
		const SlotNum &slotNum) {
	SlotNum totalSlotNum = getSlotNum(page);
	if (totalSlotNum < slotNum)
		return IX_SLOT_DIR_OVERFLOW;
	char *data = (char *)page + PAGE_SIZE
				- sizeof(Offset)
				- sizeof(PageNum)*2 - sizeof(IsLeaf)
				- sizeof(SlotNum) - slotNum * sizeof(SlotDir);
	memcpy(&indexDir, data, sizeof(IndexDir));
	return SUCC;
}
RC IndexManager::setSlotDir(void *page,
		const IndexDir &indexDir,
		const SlotNum &slotNum) {
	SlotNum totalSlotNum = getSlotNum(page);
	if (totalSlotNum < slotNum)
		return IX_SLOT_DIR_OVERFLOW;
	char *data = (char *)page + PAGE_SIZE
				- sizeof(Offset)
				- sizeof(PageNum)*2 - sizeof(IsLeaf)
				- sizeof(SlotNum) - slotNum * sizeof(SlotDir);
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
		rc = getSlotDir(page, indexDir, med);
		if (rc != SUCC) {
			cerr << "binarySearchEntry: error set slot dir " << rc << endl;
			return rc;
		}
		char *indexKey = (char *)page + indexDir.slotOffset;
		int compResult = compareKey(attr, key, indexKey);
		if (compResult == 0) {
			slotNum = med;
			return IX_HIT;
		} else if (compResult > 0) {
			beg = med+1;
		} else {
			end = med-1;
		}
	}
	if (beg > totalSlotNum) {
		slotNum = totalSlotNum+1;
		return IX_UPPER_BOUND;
	} else if (end < 1) {
		slotNum = 1;
		return IX_LOWER_BOUND;
	} else {
		med = beg + (end-beg)/2;
		rc = getSlotDir(page, indexDir, med);
		if (rc != SUCC) {
			cerr << "binarySearchEntry: error set slot dir " << rc << endl;
			return rc;
		}
		char *indexKey = (char *)page + indexDir.slotOffset;
		int compResult = compareKey(attr, key, indexKey);
		if (compResult == 0) {
			slotNum = med;
			return IX_HIT;
		} else if (compResult > 0) {
			slotNum = med+1;
			return IX_ABOVE;
		} else {
			slotNum = med;
			return IX_BELOW;
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
	bool isLeaf = isPageLeaf(page);
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
		rc = setSlotDir(page, indexDir, 1);
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
		rc = setSlotDir(page, indexDir, slotNum2Insert);
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
		rc = getSlotDir(page, indexDir, slotNum2Insert);
		Offset slotOffset2Insert = indexDir.slotOffset; // keep the slot offset for future insertion
		if (rc != SUCC) {
			cerr << "insertEntryAtPosLeaf: error get slot dir " << rc << endl;
			return rc;
		}

		// iteratively move the data
		for (SlotNum sn = totalSlotNum; sn >= slotNum2Insert; --sn) {
			// get the slot dir
			rc = getSlotDir(page, indexDir, sn);
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
			rc = setSlotDir(page, indexDir, sn+1);
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
		rc = setSlotDir(page, indexDir, slotNum2Insert);
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
	bool leaf = isPageLeaf(page);
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
	rc = getSlotDir(page, indexDir, slotNum);
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
		rc = getSlotDir(page, indexDir, sn);
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
		rc = setSlotDir(page, indexDir, sn-1);
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
	bool isLeaf = isPageLeaf(page);
	if (isLeaf)
		printLeafPage(page, attr);
	else
		printNonLeafPage(page, attr);
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
		RC rc = getSlotDir(page, indexDir, sn);
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
		RC rc = getSlotDir(page, indexDir, sn);
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


