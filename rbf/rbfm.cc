
#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;
VersionManager* VersionManager::_ver_manager = 0;

/*
 * Record based file manager
 */
RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();
    VersionManager::instance();
    return _rbf_manager;
}


RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}
// This method creates a record-based file called fileName.
RC RecordBasedFileManager::createFile(const string &fileName) {
	return PagedFileManager::instance()->createFile(fileName.c_str());
}
// This method destroys the record-based file whose name is fileName.
RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return PagedFileManager::instance()->destroyFile(fileName.c_str());
}
// This method opens the record-based file whose name is fileName.
RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return PagedFileManager::instance()->openFile(fileName.c_str(), fileHandle);
}
// This method closes the open file instance referred to by fileHandle.
RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return PagedFileManager::instance()->closeFile(fileHandle);
}
// Given a record descriptor, insert a record into a given file identifed by the provided handle.
// Given a record descriptor, insert a record into a given file identifed by the provided handle.
RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
	// TODO test
	return insertRecordWithVersion(fileHandle, recordDescriptor, data, rid, 0);
}
RC RecordBasedFileManager::insertRecordWithVersion(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor,
		const void *data, RID &rid, const char &ver) {
	// get record size to be inserted
	// translate the data to the raw data version
	unsigned recordSize = translateRecord2Raw(recordConent, data, recordDescriptor);
	// to determine whether a record can be inserted, must include the slot directory size
	unsigned recordDirectorySize = recordSize + sizeof(SlotDir);

	// get the first page that is available
	// first find the file handle. determine if it is existed
	PagedFileManager * pmfInstance = PagedFileManager::instance();
	MultipleFilesSpaceManager::iterator itr = pmfInstance->filesSpaceManager.find(fileHandle.fileName);
	if (itr == pmfInstance->filesSpaceManager.end()) {
		return RECORD_FILE_HANDLE_NOT_FOUND;
	}
	// obtain the page number
	PageNum pageNum = -1;
	RC rc = itr->second.getPageSpaceInfo(recordDirectorySize, pageNum);
	if (rc == FILE_SPACE_EMPTY || rc == FILE_SPACE_NO_SPACE) {
		// the record is too big to fit into the page
		// create a new page
		createEmptyPage(pageContent);
		rc = fileHandle.appendPage(pageContent);
		if (rc != SUCC) {
			cerr << "Insert record: cannot append a new page" << endl;
			return rc;
		}
		// obtain the newly allocated page's num
		pageNum = fileHandle.getNumberOfPages()-1;
		// push the page into the queue
		unsigned freeSpaceSize = getFreeSpaceSize(pageContent);
		itr->second.pushPageSpaceInfo(freeSpaceSize, pageNum);
	}
	// TODO Test
	//if (pageNum < TABLE_PAGES_NUM)
	//	exit(01);
	// load the page data
	rc = fileHandle.readPage(pageNum, pageContent);
	if (rc != SUCC) {
		cerr << "Insert record: Read page " << rc << endl;
		return rc;
	}

	// get the start point to write with
	char *startPoint = (char*)(getFreeSpaceStartPoint(pageContent));
	char *originalStartPoint = startPoint;

	//  write data onto the page memory
	memcpy(startPoint, recordConent, recordSize);

    // update the free space pointer of the page
	startPoint += recordSize;
	setFreeSpaceStartPoint(pageContent, startPoint);
	// get the # of slots
	SlotNum numSlots = getNumSlots(pageContent);
	// update the number of slots in this page
	rc = setNumSlots(pageContent, numSlots+1);
	if (rc != SUCC) {
		cerr << "Insert record: fail to set number of slots " << rc << endl;
		return rc;
	}
	// update the directory of the slot
	SlotDir slotDir;
	slotDir.recordLength = recordSize;
	slotDir.recordOffset = (FieldAddress)originalStartPoint -
			(FieldAddress)pageContent;
	slotDir.version = ver;
	rc = setSlotDir(pageContent, slotDir, numSlots+1);
	if (rc != SUCC) {
		cerr << "Insert record: fail to update the director of the slot " << rc << endl;
		return rc;
	}

	// write page into the file
	rc = fileHandle.writePage(pageNum, pageContent);
	if (rc != SUCC) {
		cerr << "Insert record: write page " << rc << endl;
		return rc;
	}

	// update the priority queue
	unsigned freeSpaceSize = getFreeSpaceSize(pageContent);
	itr->second.popPageSpaceInfo();
	itr->second.pushPageSpaceInfo(freeSpaceSize, pageNum);
	// update the record id
	rid.pageNum = pageNum;
	rid.slotNum = numSlots + 1;
	return SUCC;
}
// Given a record descriptor, read the record identified by the given rid.
RC RecordBasedFileManager::readRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	char ver;
	return readRecordWithVersion(fileHandle, recordDescriptor, rid, data, ver);
}
RC RecordBasedFileManager::readRecordWithVersion(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor,
		const RID &rid, void *data, char &ver) {
	RC rc = fileHandle.readPage(rid.pageNum, pageContent);
	if (rc != SUCC) {
		cerr << "Read Record: Reading Page error: " << rc << endl;
		return rc;
	}

	// get slot directory
	SlotDir slotDir;
	rc = getSlotDir(pageContent, slotDir, rid.slotNum);
	if (rc != SUCC) {
		cerr << "Read record: Reading slot directory error: " << rc << endl;
		return rc;
	}

	// check if the data has been deleted
	if (slotDir.recordLength == RECORD_DEL) {
		return RC_RECORD_DELETED;
	}

	// check if the data has been forwarded
	if (slotDir.recordLength == RECORD_FORWARD) {
		// get the forward RID
		RID forwardRID;
		getForwardRID(forwardRID, slotDir.recordOffset);
		// goto forwarded RID to read the data
		return readRecordWithVersion(fileHandle, recordDescriptor,
				forwardRID, data, ver);
	}

	// get the slot directory info
	char *slot = (char *)pageContent + slotDir.recordOffset;
	ver = slotDir.version;
	// translate the slot record to printable data
	translateRecord2Printable((void *)slot, data, recordDescriptor);
    return SUCC;
}
// delete all records
RC RecordBasedFileManager::deleteRecords(FileHandle &fileHandle) {
	PageNum totalPageNum = fileHandle.getNumberOfPages();
	RC rc;
	// TODO Test
	// NOTE: only delete records in data pages
	for (PageNum i = TABLE_PAGES_NUM; i < totalPageNum; ++i) {
		// read the page
		rc = fileHandle.readPage(i, pageContent);
		if (rc != SUCC){
			cerr << "delete records: read page error " << rc << endl;
			return rc;
		}
		// set the free space to the top of the page
		setFreeSpaceStartPoint(pageContent, pageContent);
		// set the number of slot directory to be zero
		rc = setNumSlots(pageContent, 0);
		if (rc != SUCC){
			cerr << "delete records: set num slots error " << rc << endl;
			return rc;
		}
		// write the page
		rc = fileHandle.writePage(i, pageContent);
		if (rc != SUCC){
			cerr << "delete records: write page error " << rc << endl;
			return rc;
		}
	}

	// refresh the space manager
	// get the first page that is available
	// first find the file handle. determine if it is existed
	PagedFileManager * pmfInstance = PagedFileManager::instance();
	MultipleFilesSpaceManager::iterator itr = pmfInstance->filesSpaceManager.find(fileHandle.fileName);
	if (itr == pmfInstance->filesSpaceManager.end()) {
		return RECORD_FILE_HANDLE_NOT_FOUND;
	}
	// empty the page space queue
	itr->second.clearPageSpaceInfo();
	// insert the new page space
	for (PageNum i = TABLE_PAGES_NUM; i < totalPageNum; ++i) {
		rc = itr->second.pushPageSpaceInfo(getEmptySpaceSize(), i);
		if (rc != SUCC){
			cerr << "delete records: find page space error " << rc << endl;
			return rc;
		}
	}
	return SUCC;
}
// delete specific record
RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid) {
	char page[PAGE_SIZE];
	// read the page into the memory
	RC rc = fileHandle.readPage(rid.pageNum, page);
	if (rc != SUCC) {
		cerr << "Delete record: Reading Page error " << rc << endl;
		return rc;
	}

	// get slot directory
	SlotDir slotDir;
	rc = getSlotDir(page, slotDir, rid.slotNum);
	if (rc != SUCC) {
		cerr << "Delete record: Reading slot directory error " << rc << endl;
		return rc;
	}

	// check if the data has been deleted
	if (slotDir.recordLength == RECORD_DEL) {
		return SUCC;
	}

	// check if the data has been forwarded
	if (slotDir.recordLength == RECORD_FORWARD) {
		// delete the record
		slotDir.recordLength = RECORD_DEL;
		// set the slot directory
		rc = setSlotDir(page, slotDir, rid.slotNum);
		if (rc != SUCC){
			cerr << "Delete record: set slot dir error " << rc << endl;
			return rc;
		}
		// write page back
		rc = fileHandle.writePage(rid.pageNum, page);
		if (rc != SUCC){
			cerr << "Delete record: write page error " << rc << endl;
			return rc;
		}

		// get the forward RID
		RID forwardRID;
		getForwardRID(forwardRID, slotDir.recordOffset);
		// goto forwarded RID to read the data
		return deleteRecord(fileHandle, recordDescriptor, forwardRID);
	}

	// delete the record
	slotDir.recordLength = RECORD_DEL;

	// set the slot directory
	rc = setSlotDir(page, slotDir, rid.slotNum);
	if (rc != SUCC){
		cerr << "Delete record 2: set slot dir error " << rc << endl;
		return rc;
	}

	// write page back
	rc = fileHandle.writePage(rid.pageNum, page);
	if (rc != SUCC){
		cerr << "Delete record 2: write page error " << rc << endl;
		return rc;
	}

	return SUCC;
}
// update the record without changing the rid
RC RecordBasedFileManager::RecordBasedFileManager::updateRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor,
		const void *data, const RID &rid) {
	// TODO Test
//	cerr << "never invoke this method" << endl;
//	exit(-1);
	RC rc = updateRecordWithVersion(fileHandle, recordDescriptor, data, rid, 0);
	return rc;
}
RC RecordBasedFileManager::updateRecordWithVersion(FileHandle &fileHandle,
		  const vector<Attribute> &recordDescriptor,
		  const void *data, const RID &rid, const char &ver) {
	RC rc;
	// char page[PAGE_SIZE];
	// read page
	rc = fileHandle.readPage(rid.pageNum, pageContent);
	if (rc != SUCC){
		cerr << "Update record: read page error " << rc << endl;
		return rc;
	}

	// get the # of slots
	SlotNum numSlots = getNumSlots(pageContent);
	// get the offset of the slot
	SlotDir curSlot;
	rc = getSlotDir(pageContent, curSlot, rid.slotNum);
	if (rc != SUCC){
		cerr << "Update record: get slot dir error " << rc << endl;
		return rc;
	}
	// determine whether the record is deleted
	if (curSlot.recordLength == RECORD_DEL) {
		return SUCC;
	}
	if (curSlot.recordLength == RECORD_FORWARD) {
		// get the forwarded RID
		RID newRID;
		getForwardRID(newRID, curSlot.recordOffset);
		return updateRecordWithVersion(fileHandle,
				recordDescriptor, data, newRID, ver);
	}

	// get record size to be inserted
	// translate the record to formatted version
	unsigned recordSize = translateRecord2Raw(recordConent, data, recordDescriptor);
	// determine if the record can be fitted into the original slot
	unsigned recordSpaceAvailable = 0;

	// get the next valid slot directory
	SlotDir nextSlotDir;
	nextSlotDir.recordLength = RECORD_DEL;
	SlotNum nextSlotNum = rid.slotNum + 1;
	do {
		rc = getSlotDir(pageContent, nextSlotDir, nextSlotNum);
		if (rc != SUCC) { // number of slot overflow quit
			break;
		}
		if (nextSlotDir.recordLength != RECORD_DEL &&
				nextSlotDir.recordLength != RECORD_FORWARD)
			break;
		++nextSlotNum;
	} while (nextSlotDir.recordLength == RECORD_DEL ||
			nextSlotDir.recordLength == RECORD_FORWARD);

	bool isLastRecord = false;
	if (rid.slotNum < numSlots && nextSlotNum <= numSlots) {
		// if slot is in the middle of a page
		// note the slot num starts from 1
		recordSpaceAvailable = nextSlotDir.recordOffset - curSlot.recordOffset;
		isLastRecord = false;
		/*
		 * Debug Info
		cout << "In the middle" << endl;
		cout << "# of slots " << numSlots << ", next slot #" << nextSlotNum << endl;
		cout << "Page num " << rid.pageNum << ", slot num " << rid.slotNum << endl;
		cout << "Available record space " << recordSpaceAvailable << endl;
		cout << "Record space needed " << recordSize << endl;
		*/
	} else { // if it is at last pos of the age
		// minus the used space
		recordSpaceAvailable = PAGE_SIZE - curSlot.recordOffset;
		// minus the directories & free space pointer & # of dirs
		recordSpaceAvailable -= (sizeof(FieldAddress)
				+ numSlots * sizeof(SlotDir) + sizeof(SlotNum));
		isLastRecord = true;
		/*
		 * Debug Info
		cout << "At the last" << endl;
		cout << "# of slots " << numSlots << ", next slot #" << nextSlotNum << endl;
		cout << "Page num " << rid.pageNum << ", slot num " << rid.slotNum << endl;
		cout << "Available record space " << recordSpaceAvailable << endl;
		cout << "Record space needed " << recordSize << endl;
		*/
	}

	// if it can be fitted
	if (recordSpaceAvailable >= recordSize) {
		char *writtenRecord = (char *)pageContent + curSlot.recordOffset;
		// copy the record
		memcpy(writtenRecord, recordConent, recordSize);
		// reset the slot directory
		curSlot.recordLength = recordSize;
		curSlot.version = ver;
		rc = setSlotDir(pageContent, curSlot, rid.slotNum);
		if (rc != SUCC){
			cerr << "Update record: set dir error " << rc << endl;
			return rc;
		}
		// set the free space for its the last record
		if (isLastRecord) {
			setFreeSpaceStartPoint(pageContent, writtenRecord+recordSize);
		}
		// write page
		rc = fileHandle.writePage(rid.pageNum, pageContent);
		if (rc != SUCC){
			cerr << "Update record: write page error " << rc << endl;
			return rc;
		}
	} else { // the updated record cannot be fitted into the page
		// insert the record
		RID newRID;
		rc = insertRecordWithVersion(fileHandle, recordDescriptor, data, newRID, ver);
		if (rc != SUCC){
			cerr << "Update record: insert record error " << rc << endl;
			return rc;
		}
		// reload the page in case the record is inserted into the same page
		rc = fileHandle.readPage(rid.pageNum, pageContent);

		// set the original slot in page as forwarded
		curSlot.recordLength = RECORD_FORWARD;
		// set the slot directory
		setForwardRID(newRID, curSlot.recordOffset);
		setSlotDir(pageContent, curSlot, rid.slotNum);
		// write page
		rc = fileHandle.writePage(rid.pageNum, pageContent);
		if (rc != SUCC){
			cerr << "Update record: write page error " << rc << endl;
			return rc;
		}
	}

	return SUCC;
}

// read values associating with the attribute
RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor,
		const RID &rid, const string attributeName, void *data) {
	//TODO

	return -1;
}

RC RecordBasedFileManager::reorganizePage(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const unsigned pageNumber) {
	RC rc;
	// read page
	rc = fileHandle.readPage(pageNumber, pageContent);
	if (rc != SUCC){
		cerr << "Reorganize record: read page error " << rc << endl;
		return rc;
	}

	// get the number of slots
	SlotNum slotsNum = getNumSlots(pageContent);
	char *curWrittenPoint = pageContent;
	char *curReadPoint = pageContent;
	SlotDir slotDir;

	// Note: as written in insertRecord, the slot num starts from 1
	for (SlotNum slot = 1; slot <= slotsNum; ++slot) {
		// get the slot directory
		rc = getSlotDir(pageContent, slotDir, slot);
		if (rc != SUCC){
			cerr << "Reorganize record: get slot dir error " << rc << endl;
			return rc;
		}

		// see if the slot neither deleted nor forwarded
		if (slotDir.recordLength != RECORD_DEL &&
				slotDir.recordLength != RECORD_FORWARD) {
			// set the reading start point
			curReadPoint = pageContent + slotDir.recordOffset;
			// get the record size
			unsigned recordSize = slotDir.recordLength;
			// update the offset of the slot directory
			slotDir.recordOffset =  (FieldAddress)curWrittenPoint -
					(FieldAddress)pageContent;
			rc = setSlotDir(pageContent, slotDir, slot);
			if (rc != SUCC){
				cerr << "Reorganize record: set dir error " << rc << endl;
				return rc;
			}
/*
 * Debug Info
			cout << "curWrittenPoint " << curWrittenPoint - pageContent;
			cout << ", written size " << recordSize << endl;
*/
			// move the data
			memcpy(curWrittenPoint, curReadPoint, recordSize);
			curWrittenPoint += recordSize;
		}
	}

	// set the free space size
	setFreeSpaceStartPoint(pageContent, curWrittenPoint);
	// write page
	return fileHandle.writePage(pageNumber, pageContent);
}
// scan returns an iterator to allow the caller to go through the results one by one.
RC RecordBasedFileManager::scan(FileHandle &fileHandle,
    const vector<Attribute> &recordDescriptor,
    const string &conditionAttribute,
    const CompOp compOp,                  // comparision type such as "<" and "="
    const void *value,                    // used in the comparison
    const vector<string> &attributeNames, // a list of projected attributes
    RBFM_ScanIterator &rbfm_ScanIterator) {
	return -1;
	// TODO
}
// reorganize the database
RC RecordBasedFileManager::reorganizeFile(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor) {
	return -1;
	// TODO
}

/*
 * Iterator
 */
RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
	// TODO
	return RBFM_EOF;
}
RC RBFM_ScanIterator::close() {
	// TODO
	return -1;
}

/*
 * Tools
 */
// This is a utility method that will be mainly used for debugging/testing.
// It should be able to interpret the bytes of each record using the passed record descriptor and then print its content to the screen.
RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor,
		const void *data) {
    char *slot = (char *)data;
    unsigned numFields = recordDescriptor.size();
    cout << "==============================================" << endl;
    cout << "Attribute\t\t\tAttribute Type\t\t\tAttribute Length\t\t\tValue" << endl;
    for (int i = 0; i < numFields; ++i) {
    	string type;
    	stringstream ss;
    	int length;
    	switch (recordDescriptor[i].type) {
    	case TypeInt:
    		type = "Int";
    		ss << *(int*)(slot);
    		slot += sizeof(int);
    		length = sizeof(int);
    		break;
    	case TypeReal:
    		type = "Real";
    		ss << *(float*)(slot);
    		slot += sizeof(float);
    		length = sizeof(int);
    		break;
    	case TypeVarChar:
    		type = "VarChar";
    		int len  = (int)(*slot);
    		slot += sizeof(int);
    		char *str = new char[len+1];
    		memcpy(str, slot, len);
    		str[len] = '\0';
    		ss << str;
    		slot += len;
    		length = len;
    		delete []str;
    	}
    	cout << recordDescriptor[i].name << "\t\t\t";
    	cout << type << "\t\t\t";
    	cout << length << "\t\t\t";
    	cout << ss.str() << endl;
    }
	return SUCC;
}
// Translate printable version to raw version
// Note: record size is the size of the record in the page exclude the rid size
unsigned RecordBasedFileManager::translateRecord2Raw(void *raw,
		const void *formatted,
		const vector<Attribute> &rD) {
	unsigned recordSize = 0;
	char *rawData = (char *)raw;
	char *rawField = (char *)raw;
	char *formattedData = (char *)formatted;

	unsigned numFields = rD.size();
	// Note here the start record address is an offset
	FieldOffset startPointAddress = (FieldOffset)(sizeof(FieldOffset) +
			numFields * sizeof(FieldOffset));
	memcpy(rawData, &startPointAddress, sizeof(FieldOffset));
	recordSize += sizeof(FieldOffset) + numFields * sizeof(FieldOffset);
	rawData += recordSize;
	rawField += sizeof(FieldOffset);

	FieldOffset offset = 0;
	for (int i = 0; i < numFields; ++i) {
		switch(rD[i].type) {
		case TypeInt:
			offset += sizeof(int);
			memcpy(rawField, &offset, sizeof(FieldOffset));
			rawField += sizeof(FieldOffset);
			memcpy(rawData, formattedData, sizeof(int));
			rawData += sizeof(int);
			formattedData += sizeof(int);
			recordSize += sizeof(int);
			break;
		case TypeReal:
			offset += sizeof(float);
			memcpy(rawField, &offset, sizeof(FieldOffset));
			rawField += sizeof(FieldOffset);
			memcpy(rawData, formattedData, sizeof(float));
			rawData += sizeof(float);
			formattedData += sizeof(float);
			recordSize += sizeof(float);
			break;
		case TypeVarChar:
			int len = *((int *)(formattedData));
			offset += len;
			memcpy(rawField, &offset, sizeof(FieldOffset));
			rawField += sizeof(FieldOffset);
			formattedData += sizeof(int);				// because of test file
			memcpy(rawData, formattedData, len);
			rawData += len;
			formattedData += len;
			recordSize += len;
			break;
		}
	}
	return recordSize;
}
// Translate record to printable version
void RecordBasedFileManager::translateRecord2Printable(const void *raw,
		void *formatted,
		const vector<Attribute> &rD) {
	char *rawData = (char *)raw;
	char *formattedData = (char *)formatted;
	rawData += sizeof(FieldOffset);
	FieldOffset *fieldSizes = new FieldOffset[rD.size()];
	FieldOffset lastOffset = 0;
	for (int i = 0; i < rD.size(); ++i) {
		fieldSizes[i] = *((FieldOffset *)rawData) - lastOffset;
		lastOffset = *((FieldOffset *)rawData);
		rawData += sizeof(FieldOffset);
	}
	for (int i = 0; i < rD.size(); ++i) {
		switch(rD[i].type) {
		case TypeInt:
			memcpy(formattedData, rawData, sizeof(int));
			rawData += sizeof(int);
			formattedData += sizeof(int);
			break;
		case TypeReal:
			memcpy(formattedData, rawData, sizeof(float));
			rawData += sizeof(float);
			formattedData += sizeof(float);
			break;
		case TypeVarChar:
			int size = (int)(fieldSizes[i]);  // because of test file prepareRecord
			memcpy(formattedData, &size, sizeof(int));
			formattedData += sizeof(int);
			memcpy(formattedData, rawData, size);
			formattedData += size;
			rawData += size;
			break;
		}
	}
	delete []fieldSizes;

}
// Create an empty page
void RecordBasedFileManager::createEmptyPage(void *page) {
	// set the page to be zeros
	memset(page, 0, PAGE_SIZE);
	// set the free space point to the start of the page
	setFreeSpaceStartPoint(page, page);
	// set number of slots as zero
	setNumSlots(page, 0);
}
// get the size of a record
unsigned RecordBasedFileManager::getRecordDirectorySize(const vector<Attribute> &recordDescriptor,
		unsigned &recordSize){
	unsigned len(0);
	recordSize = 0;
	for (int i = 0; i < recordDescriptor.size(); ++i) {
		recordSize += recordDescriptor[i].length;
		len += recordDescriptor[i].length + sizeof(FieldOffset); // add one more offset
	}
	return len + sizeof(FieldAddress) + sizeof(SlotDir); // add address and SlotDir
}
// get the free space start point
void * RecordBasedFileManager::getFreeSpaceStartPoint(void *page) {
	char *p = (char *)page + PAGE_SIZE - sizeof(FieldAddress);
	FieldAddress addr = *(FieldAddress *)(p);
	return (char *)page + addr;
}
void RecordBasedFileManager::setFreeSpaceStartPoint(void *page, void *startPoint) {
	FieldAddress addr = (FieldAddress)(startPoint) - (FieldAddress)(page);
	char *p = (char *)page + PAGE_SIZE - sizeof(FieldAddress);
	memcpy(p, &addr, sizeof(FieldAddress));
}
// calculate the size of free space
// Note this free space does NOT include the SLOT DIRECTORY
unsigned RecordBasedFileManager::getFreeSpaceSize(void *page) {
	char *beg = (char *)page + PAGE_SIZE - sizeof(FieldAddress);
	FieldAddress addr_beg = *((FieldAddress *)(beg));
	unsigned space = PAGE_SIZE - addr_beg;
	space = space - getNumSlots(page) * sizeof(SlotDir) - sizeof(SlotNum) - sizeof(FieldAddress);
	return space;
}
unsigned RecordBasedFileManager::getEmptySpaceSize() {
	return PAGE_SIZE - sizeof(SlotNum) - sizeof(FieldAddress);
}
// get/set the number of slots
SlotNum RecordBasedFileManager::getNumSlots(void *page) {
	char *p = (char *)page + PAGE_SIZE - sizeof(FieldAddress) - sizeof(SlotNum);
	return *((SlotNum *)p);
}
RC RecordBasedFileManager::setNumSlots(void *page, SlotNum num) {
	unsigned freeSpace = getFreeSpaceSize(page);
	unsigned infoLen = sizeof(SlotDir);
	if (freeSpace < infoLen)
		return RECORD_NOT_ENOUGH_SPACE_FOR_MORE_SLOTS;
	char *p = (char *)page + PAGE_SIZE - sizeof(FieldAddress) - sizeof(SlotNum);
	memcpy(p, &num, sizeof(SlotNum));
	return SUCC;
}

// get directory of nth slot
RC RecordBasedFileManager::getSlotDir(void *page, SlotDir &slotDir, const SlotNum &nth) {
	if (nth > getNumSlots(page))
		return RECORD_OVERFLOW;
	char *nthSlotDir =  (char *)(page) + PAGE_SIZE -
			sizeof(FieldAddress) - sizeof(SlotNum) - (nth)*sizeof(SlotDir);
	memcpy(&slotDir, nthSlotDir, sizeof(SlotDir));
	return SUCC;
}
// set directory of nth slot
RC RecordBasedFileManager::setSlotDir(void *page, const SlotDir &slotDir, const SlotNum &nth) {
	if (nth > getNumSlots(page)) {
		return RECORD_OVERFLOW;
	}
	char *nthSlotDir =  (char *)(page) + PAGE_SIZE -
				sizeof(FieldAddress) - sizeof(SlotNum) - (nth)*sizeof(SlotDir);
	memcpy(nthSlotDir, &slotDir, sizeof(SlotDir));
	return SUCC;
}
// get the forwarded page number and slot id
void RecordBasedFileManager::getForwardRID(RID &rid, const FieldAddress &offset) {
	// Note the size of a page num is sizeof(unsigned)
	char *data = (char *)&offset;
	memcpy(&(rid.pageNum), data, sizeof(unsigned));
	data += sizeof(unsigned);
	memcpy(&(rid.slotNum), data, sizeof(unsigned));
}
// set the forwarded page number and slot id
void RecordBasedFileManager::setForwardRID(const RID &rid, FieldAddress &offset) {
	char *offset2Filled = (char *)(&offset);
	memcpy(offset2Filled, &(rid.pageNum), sizeof(unsigned));
	offset2Filled += sizeof(unsigned);
	memcpy(offset2Filled, &(rid.slotNum), sizeof(unsigned));
}


/*
 *		Middleware: Version Manager
 */
VersionManager::VersionManager() {
	createAttrRecordDescriptor(recordAttributeDescriptor);
}
VersionManager* VersionManager::instance() {
    if(!_ver_manager)
    	_ver_manager = new VersionManager();

    return _ver_manager;
}
// set up a table's attributes and its version
RC VersionManager::insertTableVersionInfo(const string &tableName,
		FileHandle &fileHandle) {
	//TODO
	return -1;
}
// get the attributes of a version
RC VersionManager::getAttributes(const string &tableName, vector<Attribute> &attrs,
		const VersionNumber ver) {
	AttrMap::iterator itr = attrMap.find(tableName);
	if (itr == attrMap.end()) {
		return VERSION_TABLE_NOT_FOUND;
	}
	VersionMap::iterator itr_1 = versionMap.find(tableName);
	if (itr_1 == versionMap.end()) {
		return VERSION_TABLE_NOT_FOUND;
	}

	vector<RecordDescriptor> &recordDescriptorArray = itr->second;
	attrs = recordDescriptorArray[ver];

	return SUCC;
}
// add an attribute
RC VersionManager::addAttribute(const string &tableName,
		const Attribute &attr, FileHandle &fileHandle) {
	AttrMap::iterator itr = attrMap.find(tableName);
	if (itr == attrMap.end()) {
		return VERSION_TABLE_NOT_FOUND;
	}
	VersionMap::iterator itr_1 = versionMap.find(tableName);
	if (itr_1 == versionMap.end()) {
		return VERSION_TABLE_NOT_FOUND;
	}
	VersionNumber &currentVersion = itr_1->second;
	vector<RecordDescriptor> &recordDescriptorArray = itr->second;

	// update the version number
	currentVersion = (currentVersion + 1) % MAX_VER;

	// update record descriptor
	vector<Attribute> &recordDescriptor = recordDescriptorArray[currentVersion];
	recordDescriptor.push_back(attr);
	//TODO write to page
	return SUCC;
}
// drop an attribute
RC VersionManager::dropAttribute(const string &tableName,
		const string &attributeName, FileHandle &fileHandle) {
	AttrMap::iterator itr = attrMap.find(tableName);
	if (itr == attrMap.end()) {
		return VERSION_TABLE_NOT_FOUND;
	}
	VersionMap::iterator itr_1 = versionMap.find(tableName);
	if (itr_1 == versionMap.end()) {
		return VERSION_TABLE_NOT_FOUND;
	}
	VersionNumber &currentVersion = itr_1->second;
	vector<RecordDescriptor> &recordDescriptorArray = itr->second;
	vector<Attribute> recordDescriptor = recordDescriptorArray[currentVersion];

	// update the version number
	currentVersion = (currentVersion + 1) % MAX_VER;

	// update record descriptor
	for (vector<Attribute>::iterator itrAtt = recordDescriptor.begin();
			itrAtt != recordDescriptor.end(); ++itrAtt) {
		if (itrAtt->name == attributeName) {
			recordDescriptor.erase(itrAtt);
			break;
		}
	}
	recordDescriptorArray[currentVersion] = recordDescriptor;
	//TODO write to page
	return SUCC;
}

void VersionManager::eraseTableVersionInfo(const string &tableName) {
	AttrMap::iterator itr = attrMap.find(tableName);
	if (itr != attrMap.end()) {
		attrMap.erase(itr);
	}
	VersionMap::iterator itr_1 = versionMap.find(tableName);
	if (itr_1 != versionMap.end()) {
		versionMap.erase(itr_1);
	}
}
void VersionManager::eraseAllInfo() {
	attrMap.clear();
	attrMap.clear();
}
// get i'th version information
RC VersionManager::get_ithVersionInfo(void *page, VersionNumber ver,
		VersionInfoFrame &versionInfoFrame) {
	if (ver >= MAX_VER)
		return VERSION_OVERFLOW;
	char *data = (char *)page;
	data += sizeof(VersionNumber) + ver * sizeof(VersionInfoFrame);
	memcpy(&versionInfoFrame, data, sizeof(VersionInfoFrame));
	return SUCC;
}
// set i'th version Information
RC VersionManager::set_ithVersionInfo(void *page, VersionNumber ver,
		const VersionInfoFrame &versionInfoFrame) {
	if (ver >= MAX_VER)
		return VERSION_OVERFLOW;
	char *data = (char *)page;
	data += sizeof(VersionNumber) + ver * sizeof(VersionInfoFrame);
	memcpy(data, &versionInfoFrame, sizeof(VersionInfoFrame));
	return SUCC;
}

// set the version of the number
RC VersionManager::setVersionNumber(void *page, VersionNumber ver) {
	if (ver >= MAX_VER)
		return VERSION_OVERFLOW;
	memcpy(page, &ver, sizeof(VersionNumber));
	// TODO currentVersionNumber = ver;
	return SUCC;
}
// get the version number
RC VersionManager::getVersion(const string &tableName, VersionNumber &ver) {
	VersionMap::iterator itr_1 = versionMap.find(tableName);
	if (itr_1 != versionMap.end()) {
		ver = itr_1->second;
		return SUCC;
	} else {
		return VERSION_TABLE_NOT_FOUND;
	}
}

// translate an attribute into a record
unsigned VersionManager::translateAttribte2Record(const Attribute & attr,
		void *record) {
	// the attribute of the data attribute is clear
	// number of the attribute is 3
	// the first is a varchar
	// the rest are Int

	unsigned recordSize = 0;

	// set the offset to the record data
	FieldOffset len = sizeof(FieldOffset)*4; // the first offset of the record
	memcpy(record, &len, sizeof(FieldOffset));
	recordSize += len;

	// set the data start position
	char *data = (char *)record + sizeof(FieldOffset) * 4;
	// set the field start position
	char *field = (char *)record + sizeof(FieldOffset);

	// set the length of name field
	len = attr.name.length();
	memcpy(field, &len, sizeof(FieldOffset));
	field += sizeof(FieldOffset);
	// set the value of name field
	memcpy(data, attr.name.c_str(), len);
	data += len;
	recordSize += len;

	// set the length of the type field
	len = sizeof(AttrType);
	memcpy(field, &len, sizeof(FieldOffset));
	field += sizeof(FieldOffset);
	// set the value of the type field
	memcpy(data, &(attr.type), sizeof(AttrType));
	data += sizeof(AttrType);
	recordSize += sizeof(AttrType);

	// set the length of the length field
	len = sizeof(AttrLength);
	memcpy(field, &len, sizeof(FieldOffset));
	// set the value of the length field
	memcpy(data, &(attr.length), sizeof(AttrLength));
	recordSize += sizeof(AttrLength);

	return recordSize;
}
// translate a record into an attribute
void VersionManager::translateRecord2Attribte(Attribute & attr, const void *record) {
	char *data = (char *)record + sizeof(FieldOffset) * 4;
	char *field = (char *)record +sizeof(FieldOffset);

	unsigned len = *((unsigned *)field);
	char *name = new char[len + 1];
	memcpy(name, data, len);
	name[len] = '\0';

	attr.name.assign(name);
	delete []name;

	// get attr type
	data += len;
	attr.type = *((AttrType *)data);

	// get attr len
	data += sizeof(AttrLength);
	attr.length = *((AttrLength *)data);
}
RC VersionManager::formatFirst2Page(const string &tableName,
		const vector<Attribute> &attrs,
		FileHandle &fileHandle) {
	/*
	 * In first page:
	 * - current version
	 * - version information
	 * In second page:
	 * - current attributes
	 *
	 * Note: this function must be run at the file initialization only once
	 */
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc;

	// append TABLE_PAGES_NUM more pages
	for (int i = 0; i < TABLE_PAGES_NUM-1; ++i) {
		rbfm->createEmptyPage(page);
		rc = fileHandle.appendPage(page);
		if (rc != SUCC) {
			cerr << "formatFirst2Page: cannot append a new page" << endl;
			return rc;
		}
	}

	// prepare first page
	rc = fileHandle.readPage(0, page);
	if (rc != SUCC) {
		cerr << "formatFirst2Page: cannot read the page" << endl;
		return rc;
	}
	// set current version to zero
	rc = setVersionNumber(page, 0);
	if (rc != SUCC) {
		cerr << "formatFirst2Page: cannot set version number" << endl;
		return rc;
	}
	// set the free space such that users never use the page
	rbfm->setFreeSpaceStartPoint(page, page+PAGE_SIZE);
	// write page
	rc = fileHandle.writePage(0, page);
	if (rc != SUCC) {
		cerr << "formatFirst2Page: cannto write the page" << endl;
		return cerr;
	}

	// insert attribute data to the pages
	resetAttributePages(attrs, fileHandle);

	// set the free space such that users never use the page
	for (int i = 1; i < TABLE_PAGES_NUM; ++i) {
		// get the page
		rc = fileHandle.readPage(i, page);
		if (rc != SUCC) {
			cerr << "formatFirst2Page: cannot read the page" << endl;
			return cerr;
		}
		// set the free space such that users never use the page
		rbfm->setFreeSpaceStartPoint(page, page+PAGE_SIZE);
		// write page to the disk
		rc = fileHandle.writePage(i, page);
		if (rc != SUCC) {
			cerr << "formatFirst2Page: cannot write the page" << endl;
			return cerr;
		}
	}

	return SUCC;
}
// set page to be empty
RC VersionManager::setPageEmpty(void *page) {
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	// set free space to be empty
	RC rc;
	rbfm->setFreeSpaceStartPoint(page, page);
	// set slot num to be zero
	rc = rbfm->setNumSlots(page, 0);
	if (rc != SUCC) {
		cerr << "set page empty: " << rc << endl;
		return rc;
	}
	return SUCC;
}
// reset the attribute pages
RC VersionManager::resetAttributePages(const vector<Attribute> &attrs,
		FileHandle &fileHandle) {
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc;
	// set each page to be empty
	for (PageNum pageNum = 1; pageNum < TABLE_PAGES_NUM; ++pageNum) {
		rc = fileHandle.readPage(pageNum, page);
		if (rc != SUCC) {
			cerr << "reset attribute pages: " << rc << endl;
			return rc;
		}
		setPageEmpty(page);
		rc = fileHandle.writePage(pageNum, page);
		if (rc != SUCC) {
			cerr << "reset attribute pages: " << rc << endl;
			return rc;
		}
	}
	RID rid;
	// insert data to the file
	for (int i = 0; i < attrs.size(); ++i) {
		translateAttribte2Record(attrs[i], record);
		rc = rbfm->insertRecord(fileHandle, recordAttributeDescriptor, record, rid);
		if (rc != SUCC) {
			cerr << "reset attribute pages: " << rc << endl;
			return rc;
		}
	}
	return SUCC;
}

// create attribute descriptor for attribute
void VersionManager::createAttrRecordDescriptor(vector<Attribute> &recordDescriptor) {
	Attribute attr;
	attr.name = "AttrName";
	attr.type = TypeVarChar;
	attr.length = (AttrLength)8;
	recordDescriptor.push_back(attr);

	attr.name = "AttrType";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	recordDescriptor.push_back(attr);

	attr.name = "AttrLength";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	recordDescriptor.push_back(attr);
}
