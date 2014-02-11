
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
	setPageLeaf(page, false);
	setSlotNum(page, 0);
}

void* IndexManager::getFreeSpaceStartPoint(const void *page) {
	char *data = (char *)page + PAGE_SIZE - sizeof(FreeSpaceSize);
	FreeSpaceSize freeSpaceStartLen = *((FreeSpaceSize *)data);
	data = (char *)page + freeSpaceStartLen;
	return (void *)data;
}
FreeSpaceSize IndexManager::getFreeSpaceOffset(const void *page) {
	char *data = (char *)page + PAGE_SIZE - sizeof(FreeSpaceSize);
	return *((FreeSpaceSize *)data);
}
void IndexManager::setFreeSpaceStartPoint(void *page, const void *point) {
	Address addr_len = *((Address *)point) - *((Address *)page);
	FreeSpaceSize freeSpaceStartLen = (FreeSpaceSize)addr_len;
	char *data = (char *)page + PAGE_SIZE - sizeof(FreeSpaceSize);
	memcpy(data, &freeSpaceStartLen, sizeof(FreeSpaceSize));
}
int IndexManager::getFreeSpaceSize(const void *page) {
	char *data = (char *)page + PAGE_SIZE - sizeof(FreeSpaceSize);
	FreeSpaceSize freeSpaceStartLen = *((FreeSpaceSize *)data);
	SlotNum totalSlotNum = getSlotNum(page);
	int restSpace = PAGE_SIZE - freeSpaceStartLen
			- sizeof(FreeSpaceSize) - sizeof(IsLeaf)
			- sizeof(SlotNum) - totalSlotNum * sizeof(IndexDir);
	return restSpace;
}
PageNum IndexManager::getPrevPageNum(const void *page) {
	char *data = (char *)page + PAGE_SIZE - sizeof(FreeSpaceSize)
			- sizeof(PageNum);
	PageNum pageNum(EOF_PAGE_NUM);
	memcpy(&pageNum, data, sizeof(PageNum));
	return pageNum;
}
PageNum IndexManager::getNextPageNum(const void *page) {
	char *data = (char *)page + PAGE_SIZE - sizeof(FreeSpaceSize)
			- sizeof(PageNum)*2;
	PageNum pageNum(EOF_PAGE_NUM);
	memcpy(&pageNum, data, sizeof(PageNum));
	return pageNum;
}
void IndexManager::setPrevPageNum(void *page,
		const PageNum &pageNum) {
	char *data = (char *)page + PAGE_SIZE - sizeof(FreeSpaceSize)
			- sizeof(PageNum);
	memcpy(data, &pageNum, sizeof(PageNum));
}
void IndexManager::setNextPageNum(void *page, const PageNum &pageNum) {
	char *data = (char *)page + PAGE_SIZE - sizeof(FreeSpaceSize)
			- sizeof(PageNum)*2;
	memcpy(data, &pageNum, sizeof(PageNum));
}
bool IndexManager::isPageLeaf(const void *page) {
	char *data = (char *)page + PAGE_SIZE
			- sizeof(FreeSpaceSize) - sizeof(PageNum)*2
			- sizeof(IsLeaf);
	bool isLeaf(false);
	memcpy(&isLeaf, data, sizeof(IsLeaf));
	return isLeaf;
}
void IndexManager::setPageLeaf(void *page, const bool &isLeaf) {
	char *data = (char *)page + PAGE_SIZE
			- sizeof(FreeSpaceSize) - sizeof(PageNum)*2
			- sizeof(IsLeaf);
	memcpy(data, &isLeaf, sizeof(IsLeaf));
}
SlotNum IndexManager::getSlotNum(const void *page) {
	char *data = (char *)page + PAGE_SIZE
			- sizeof(FreeSpaceSize)
			- sizeof(PageNum)*2 - sizeof(IsLeaf) - sizeof(SlotNum);
	SlotNum slotNum(0);
	memcpy(&slotNum, data,sizeof(slotNum));
	return slotNum;
}
void IndexManager::setSlotNum(void *page, const SlotNum &slotNum) {
	char *data = (char *)page + PAGE_SIZE
			- sizeof(FreeSpaceSize)
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
				- sizeof(FreeSpaceSize)
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
				- sizeof(FreeSpaceSize)
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
		const void *key, const unsigned &keyLen,
		SlotNum &slotNum) {
	//TODO
	return -1;
}
// insert an entry into page at pos n
RC IndexManager::insertEntryAtPosLeaf(void *page, const SlotNum &slotNum,
		const Attribute &attr,
		const void *key, const unsigned &keyLen,
		const RID &rid, const PageNum &nextPageNum) {
	RC rc;
	int spaceNeeded = keyLen + sizeof(IndexDir);
	int spaceAvailable = getFreeSpaceSize(page);
	if (spaceAvailable < spaceNeeded) {
		return IX_NOT_ENOUGH_SPACE;
	}
	// get the total number of slot
	SlotNum totalSlotNum = getSlotNum(page);
	SlotNum pos2Insert(slotNum);
	// if the pos is greater than slotNum, pushes it to the tail
	if (pos2Insert > totalSlotNum) {
		pos2Insert = totalSlotNum+1;
		char *pos2InsertEntry = (char *)getFreeSpaceStartPoint(page);
		// set the slot number
		setSlotNum(page, pos2Insert);
		// set the index slot
		IndexDir indexDir;
		indexDir.recordLength = (RecordLength)keyLen;
		indexDir.slotOffset = getFreeSpaceOffset(page);
		indexDir.nextPageNum = nextPageNum;
		rc = setSlotDir(page, indexDir, pos2Insert);
		if (rc != SUCC) {
			cerr << "insertEntryAtPosLeaf: error set slot dir " << rc << endl;
			return rc;
		}
		// move data
		memcpy(pos2InsertEntry, key, keyLen);
		pos2InsertEntry += keyLen;
		memcpy(pos2InsertEntry, &rid, sizeof(RID));
	} else {
		// otherwise, need to move data back
		// increase total slot number
		setSlotNum(page, totalSlotNum+1);
		// get the offset of the place to be inserted
		IndexDir indexDir;
		rc = getSlotDir(page, indexDir, slotNum);
		SlotOffset slotOffset2Insert = indexDir.slotOffset;
#ifdef __DEBUG__
		if (rc != SUCC) {
			cerr << "insertEntryAtPosLeaf: error get slot dir " << rc << endl;
			return rc;
		}
#endif
		// iteratively move the data

		for (SlotNum sn = totalSlotNum; sn >= slotNum; --sn) {
			// get the slot dir
			rc = getSlotDir(page, indexDir, sn);
#ifdef __DEBUG__
			if (rc != SUCC) {
				cerr << "insertEntryAtPosLeaf: error get slot dir " << rc << endl;
				return rc;
			}
#endif
			char *originPos = (char *)page + indexDir.slotOffset;
			char *newPos = originPos + keyLen + sizeof(RID);
			memcpy(Entry, originPos, indexDir.recordLength+sizeof(RID));
			memcpy(newPos, Entry, indexDir.recordLength+sizeof(RID));

			// reset index dir
			indexDir.slotOffset += keyLen + sizeof(RID);
			rc = setSlotDir(page, indexDir, sn+1);
#ifdef __DEBUG__
			if (rc != SUCC) {
				cerr << "insertEntryAtPosLeaf: error get slot dir " << rc << endl;
				return rc;
			}
#endif
		}
		// then insert the entry
		char *pos2Insert = (char *)page + slotOffset2Insert;
		memcpy(pos2Insert, key, keyLen);
		pos2Insert += keyLen;
		memcpy(pos2Insert, &rid, sizeof(RID));
		// set the index dir
		indexDir.recordLength = sizeof(RID) + keyLen;
		indexDir.slotOffset = slotOffset2Insert;
		indexDir.nextPageNum = nextPageNum;
		rc = setSlotDir(page, indexDir, slotNum);
#ifdef __DEBUG__
			if (rc != SUCC) {
				cerr << "insertEntryAtPosLeaf: error get slot dir " << rc << endl;
				return rc;
			}
#endif
	}
	return SUCC;
}
// delete an entry of page at pos n
RC IndexManager::deleteEntryAtPosLeaf(void *page, const SlotNum &slotNum) {
	//TODO
	return -1;
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





