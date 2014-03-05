
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
RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
	// get record size to be inserted
	// translate the data to the raw data version
	unsigned recordSize = getRecordSize(data, recordDescriptor);
	// to determine whether a record can be inserted, must include the slot directory size
	unsigned recordDirectorySize = recordSize + sizeof(SlotDir);

	// get the first page that is available
	// first find the file handle. determine if it is existed
	PagedFileManager * pmfInstance = PagedFileManager::instance();
	MultipleFilesSpaceManager::iterator itr = pmfInstance->filesSpaceManager.find(fileHandle.fileName);
	if (itr == pmfInstance->filesSpaceManager.end()) {
		cerr << "insertRecord: fail to find the space manager " << RECORD_FILE_HANDLE_NOT_FOUND << endl;
		return RECORD_FILE_HANDLE_NOT_FOUND;
	}
	// obtain the page number
	PageNum pageNum = -1;
	RC rc = itr->second.getPageSpaceInfo(recordDirectorySize, pageNum);
	if (rc == FILE_SPACE_EMPTY || rc == FILE_SPACE_NO_SPACE) {
		// the record is too big to fit into the page
		// create a new page
		setPageEmpty(pageContent);
		rc = fileHandle.appendPage(pageContent);
		if (rc != SUCC) {
			cerr << "Insert record: cannot append a new page" << endl;
			return rc;
		}
		// obtain the newly allocated page's num
		pageNum = fileHandle.getNumberOfPages()-1;
	} else {
		itr->second.popPageSpaceInfo();
	}

	// load the page data
	rc = fileHandle.readPage(pageNum, pageContent);
	if (rc != SUCC) {
		cerr << "Insert record: Read page " << rc << endl;
		return rc;
	}

	// available space
	unsigned spaceSize = getFreeSpaceSize(pageContent);
	if (spaceSize < recordDirectorySize)
		cerr << "not enough space " << spaceSize << "\t" << recordDirectorySize << endl;


	// get the start point to write with
	char *startPoint = (char*)(getFreeSpaceStartPoint(pageContent));
	char *originalStartPoint = startPoint;

	//  write data onto the page memory
	memcpy(startPoint, data, recordSize);

    // update the free space pointer of the page
	startPoint += recordSize;
	setFreeSpaceStartPoint(pageContent, startPoint);
	// get the # of slots
	SlotNum totalNumSlots = getNumSlots(pageContent);
	SlotNum nextAvailableSlot = getNextAvailableSlot(pageContent);
	// update the number of slots in this page
	if (totalNumSlots+1 == nextAvailableSlot) // must allocate new slot num
		rc = setNumSlots(pageContent, nextAvailableSlot);
	if (rc != SUCC) {
		cerr << "Insert record: fail to set number of slots " << rc << endl;
		return rc;
	}

	// update the directory of the slot
	SlotDir slotDir;
	slotDir.recordLength = recordSize;
	slotDir.recordOffset = (FieldAddress)originalStartPoint -
			(FieldAddress)pageContent;
	rc = setSlotDir(pageContent, slotDir, nextAvailableSlot);
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
	itr->second.pushPageSpaceInfo(freeSpaceSize, pageNum);

	// update the record id
	rid.pageNum = pageNum;
	rid.slotNum = nextAvailableSlot;
	return SUCC;
}
// Given a record descriptor, read the record identified by the given rid.
RC RecordBasedFileManager::readRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	RC rc = fileHandle.readPage(rid.pageNum, pageContent);
	if (rc != SUCC) {
		cerr << "Read Record: Reading Page error: " << rc << endl;
		return rc;
	}

	// get slot directory
	SlotDir slotDir;
	rc = getSlotDir(pageContent, slotDir, rid.slotNum);
	// cout << "read slot num " << rid.slotNum << endl;
	if (rc != SUCC) {
		cerr << "Read record: Reading slot directory error: " << rc << endl;
		cerr << "While try to read slot num " << rid.slotNum << " at page " << rid.pageNum << endl;
		SlotNum totalNumSlots = getNumSlots(pageContent);
		cerr << "The total number of slots is " << totalNumSlots << endl;
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
		return readRecord(fileHandle, recordDescriptor,
				forwardRID, data);
	}

	// get the slot directory info
	char *slot = pageContent + slotDir.recordOffset;

	// translate the slot record to printable data
	memcpy(data, slot, slotDir.recordLength);

    return SUCC;
}
// delete all records
RC RecordBasedFileManager::deleteRecords(FileHandle &fileHandle) {
	PageNum totalPageNum = fileHandle.getNumberOfPages();
	RC rc;
	// NOTE: only delete user records in data pages
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
		return updateRecord(fileHandle,
				recordDescriptor, data, newRID);
	}

	// get record size to be inserted
	// translate the record to formatted version
	unsigned recordSize = getRecordSize(data, recordDescriptor);
	// determine if the record can be fitted into the original slot
	unsigned recordSpaceAvailable = curSlot.recordLength;

	// if it can be fitted
	if (recordSpaceAvailable >= recordSize) {
		char *writtenRecord = (char *)pageContent + curSlot.recordOffset;
		// copy the record
		memcpy(writtenRecord, data, recordSize);
		// reset the slot directory
		curSlot.recordLength = recordSize;
		rc = setSlotDir(pageContent, curSlot, rid.slotNum);
		if (rc != SUCC){
			cerr << "Update record: set dir error " << rc << endl;
			return rc;
		}
		// set the free space for its the last record
		if (rid.slotNum == numSlots) {
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
		rc = insertRecord(fileHandle, recordDescriptor, data, newRID);
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
	// read the page
	RC rc = fileHandle.readPage(rid.pageNum, pageContent);
	if (rc != SUCC) {
		cerr << "readAttribute: read page error " << rc << endl;
		return rc;
	}
	// read the record
	// get the Slot Directory
	SlotDir slotDir;
	rc = getSlotDir(pageContent, slotDir, rid.slotNum);
	if (rc != SUCC) {
		cerr << "readAttribute: read slot direcotry error " << rc << endl;
		return rc;
	}
	// see if the record is deleted or forwarded
	if (slotDir.recordLength == RECORD_DEL) {
		return RC_RECORD_DELETED;
	}
	if (slotDir.recordLength == RECORD_FORWARD) {
		// get the forward RID
		RID forwardRID;
		getForwardRID(forwardRID, slotDir.recordOffset);
		return readAttribute(fileHandle, recordDescriptor, forwardRID,
				attributeName, data);
	}

	// read the record
	char *record = ((char *)pageContent) + slotDir.recordOffset;
	// get the current record's version
	VersionNumber curVer = *((VersionNumber *)record);

	// see if the attribute exists in current version
	VersionManager *vm = VersionManager::instance();
	vector<Attribute> currentRD;
	rc = vm->getAttributes(fileHandle.fileName, currentRD, curVer);
	if (rc != SUCC) {
		cerr << "readAttribute: get attribute error " << rc << endl;
		return rc;
	}

	int offset = 0;	// offset to the attribute
	int lenAttr = 0; // attribute length
	int index = 0;
	// go through the record descriptor until find the attribute name
	for (; index < int(currentRD.size()); ++index) {
		if (attributeName == currentRD[index].name) {
			// locate the attribute
			break;
		} else {
			switch(currentRD[index].type) {
			case TypeInt:
				offset += sizeof(int);
				break;
			case TypeReal:
				offset += sizeof(float);
				break;
			case TypeVarChar:
				int len = *((int *)(record + offset));
				offset += len + sizeof(int);
				break;
			}
		}
	}

	if (index == int(currentRD.size())) {
		// no such attribute found
		// return a default value
		// the default value for Varchar is (int)0NULL
		// the default value for Int is (int)0
		// the default value for Real is (float)0.0
		int defaultVal = 0;
		memcpy(data, &defaultVal, sizeof(int));
	} else {
		// got the attribute hit
		// read length of data
		switch(currentRD[index].type) {
		case TypeInt:
			lenAttr = sizeof(int);
			break;
		case TypeReal:
			lenAttr = sizeof(float);
			break;
		case TypeVarChar:
			lenAttr = *((int *)(record + offset))+sizeof(int);
			break;
		}
		// read the attribute
		memcpy(data, record+offset, lenAttr);
	}

	return SUCC;
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

// reorganize the database
RC RecordBasedFileManager::reorganizeFile(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor) {
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc;
	PagedFileManager * pfm = PagedFileManager::instance();
	MultipleFilesSpaceManager::iterator itr = pfm->filesSpaceManager.find(fileHandle.fileName);
	if (itr == pfm->filesSpaceManager.end()) {
		cerr << "reorganizedFile: fail to find the space manager " << RECORD_FILE_HANDLE_NOT_FOUND << endl;
		return RECORD_FILE_HANDLE_NOT_FOUND;
	}
	itr->second.clearPageSpaceInfo();

	// create buffer in a list
	list<char *> buffer;
	// read ptr is always ahead of write ptr
	PageNum readPagePtr(TABLE_PAGES_NUM), writePagePtr(TABLE_PAGES_NUM);
	// get the total number of pages
	PageNum totalPageNum = fileHandle.getNumberOfPages();

	// current operating page
	char *bufferPage = new char[PAGE_SIZE];
	// set page free space to the head of the page
	rbfm->setPageEmpty(bufferPage);
	// set the bufferOffset of the buffer page
	unsigned bufferOffset(0);

	char curReadPage[PAGE_SIZE];

	// read ptr from first to end
	while (readPagePtr < totalPageNum) {
		while (buffer.size() > 0 &&
				writePagePtr < readPagePtr) {
			// there is non written page in the buffer and
			// there is available processed page to write
			cout << "a page is rewritten " <<  writePagePtr << endl;
			char *page2Write = buffer.front();

			// add the page size to the space manager
			unsigned space = rbfm->getFreeSpaceSize(page2Write);
			itr->second.pushPageSpaceInfo(space, writePagePtr);

			rc = fileHandle.writePage(writePagePtr, page2Write);
			if (rc != SUCC) {
				cerr << "RecordBasedFileManager::reorganizeFile: write page error " << rc << endl;
				return rc;
			}
			// pop the page from the buffer
			buffer.pop_front();
			// free the page
			delete []page2Write;
			// inc the write page pointer
			++writePagePtr;
		} //while (buffer.size() > 0 && writePagePtr < readPagePtr)

		// load next page to read in
		rc = fileHandle.readPage(readPagePtr, curReadPage);
		if (rc != SUCC) {
			cerr << "RecordBasedFileManager::reorganizeFile: read page error " << rc << endl;
			return rc;
		}

		// get the number of slots
		SlotNum totalSlotNum = rbfm->getNumSlots(curReadPage);
		// the offset starts from 0
		FieldAddress curOffset(0);

		// from first to last slot: start from 1
		for (SlotNum readSlotPtr = 1;
				readSlotPtr <= totalSlotNum; ++readSlotPtr) {
			// get the slot directory
			SlotDir slotDir;
			rc = rbfm->getSlotDir(curReadPage, slotDir, readSlotPtr);
			if (rc != SUCC) {
				cerr << "RecordBasedFileManager::reorganizeFile: get slot dir error " << rc << endl;
				return rc;
			}
			if (slotDir.recordLength == RECORD_DEL ||
					slotDir.recordLength == RECORD_FORWARD) {
				// it is deleted or forwarded, just ignore
				continue;
			}
			// get the record size
			unsigned recordSize = slotDir.recordLength;
			curOffset = slotDir.recordOffset;

			// if not move data to buffer page
			// first verify the free space available for the buffer page
			int availableSpace = rbfm->getFreeSpaceSize(bufferPage);
			if (availableSpace < int(recordSize)) {
				// new a page for reorganizing
				buffer.push_back(bufferPage);
				bufferPage = new char[PAGE_SIZE];
				rbfm->setPageEmpty(bufferPage);
				bufferOffset = 0;
			}
			// begin move
			memcpy(bufferPage + bufferOffset,
					curReadPage + curOffset, recordSize);
			// save the slot dir for later saving dir
			slotDir.recordLength = recordSize;
			slotDir.recordOffset = bufferOffset;
			// set the free space of the buffer page
			bufferOffset += recordSize;
			rbfm->setFreeSpaceStartPoint(bufferPage, bufferPage+bufferOffset);
			// set the slot number
			SlotNum inc_slotNum = rbfm->getNumSlots(bufferPage)+1;
			rc = rbfm->setNumSlots(bufferPage, inc_slotNum);
			if (rc != SUCC) {
				cerr << "RecordBasedFileManager::reorganizeFile: set num slot error " << rc << endl;
				return rc;
			}
			// set the slot dir
			rc = rbfm->setSlotDir(bufferPage, slotDir, inc_slotNum);
			if (rc != SUCC) {
				cerr << "RecordBasedFileManager::reorganizeFile: set slot dir error " << rc << endl;
				return rc;
			}
		} // for

		// increase the read page pointer to the next page to process
		++readPagePtr;
	}

	// set the rest page to the buffer
	buffer.push_back(bufferPage);

	// there is non written page in the buffer
	while (buffer.size() > 0) {
		char *page2Write = buffer.front();

		// add the page size to the space manager
		unsigned space = rbfm->getFreeSpaceSize(page2Write);
		itr->second.pushPageSpaceInfo(space, writePagePtr);

		rc = fileHandle.writePage(writePagePtr, page2Write);
		if (rc != SUCC) {
			cerr << "RecordBasedFileManager::reorganizeFile: write page error " << rc << endl;
			return rc;
		}
		// pop the page from the buffer
		buffer.pop_front();
		// free the page
		delete []page2Write;
		// inc the write page pointer
		++writePagePtr;
	} // while (buffer.size() > 0)

	// set the rest page to be empty
	rbfm->setPageEmpty(curReadPage);
	unsigned space = rbfm->getFreeSpaceSize(curReadPage);
	while (writePagePtr < totalPageNum) {
		// add the page size to the space manager
		itr->second.pushPageSpaceInfo(space, writePagePtr);

		rc = fileHandle.writePage(writePagePtr, curReadPage);
		if (rc != SUCC) {
			cerr << "RecordBasedFileManager::reorganizeFile: write empty page error " << rc << endl;
			return rc;
		}
		++writePagePtr;
	}

	return SUCC;
}

// scan returns an iterator to allow the caller to go through the results one by one.
RC RecordBasedFileManager::scan(FileHandle &fileHandle,
    const vector<Attribute> &recordDescriptor,
    const string &conditionAttribute,
    const CompOp compOp,                  // comparision type such as "<" and "="
    const void *value,                    // used in the comparison
    const vector<string> &attributeNames, // a list of projected attributes
    RBFM_ScanIterator &rbfm_ScanIterator) {

	// clear state of all the internal variables
	if (rbfm_ScanIterator.value != NULL)
		delete []rbfm_ScanIterator.value;

	VersionManager *vm = VersionManager::instance();
	vector<Attribute> rd;
	RC rc;
	AttrType conditionAttrType(TypeInt);
	vector<AttrType> projectedType(attributeNames.size());

	unordered_map<string, int> projAttrMap;
	for (int i = 0; i < int(attributeNames.size()); ++i) {
		projAttrMap[attributeNames[i]] = i;
	}


	// get the type of the conditionAttribute
	bool hitFlag = false;
	int hitTimes = 0;
	unordered_map<string, int>::iterator itr;
	for (int i = 0; i < int(recordDescriptor.size()); ++i) {
		if(recordDescriptor[i].name == conditionAttribute) {
			conditionAttrType = recordDescriptor[i].type;
			hitFlag = true;
		}
		itr = projAttrMap.find(recordDescriptor[i].name);
		if (itr != projAttrMap.end()) {
			projectedType[itr->second] = recordDescriptor[i].type;
			++hitTimes;
		}
	}
	if ((value != NULL && !conditionAttribute.empty() ) &&
			(!hitFlag || hitTimes != int(attributeNames.size()))) {
		cerr << hitFlag << "\t" << "projected number " << hitTimes << endl;
		cerr << "scan: cannot find the condition attribute" << endl;
		return SCANITER_COND_PROJ_NOT_FOUND;
	}

	// get current version
	VersionNumber curVersion;
	rc = vm->getVersionNumber(fileHandle.fileName, curVersion);
	if (rc != SUCC) {
		cerr << "RecordBasedFileManager::scan: get version number error " << rc << endl;
		return rc;
	}

	// set the comp operator
	rbfm_ScanIterator.compOp = compOp;

	// set the value of comp attribute
	unsigned recordLen = 0;
	if (value != NULL) {
		switch(conditionAttrType) {
		case TypeInt:
			recordLen = sizeof(int);
			break;
		case TypeReal:
			recordLen = sizeof(float);
			break;
		case TypeVarChar:
			recordLen = sizeof(int) + *((int *)value);
			break;
		}
		rbfm_ScanIterator.value = new char[recordLen+1];
		memcpy(rbfm_ScanIterator.value, value, recordLen);
		rbfm_ScanIterator.value[recordLen] = '\0'; // in case the value is a string
	} else {
		rbfm_ScanIterator.value = NULL;
	}

	// set the initial state of iterator
	rbfm_ScanIterator.curRid.pageNum = TABLE_PAGES_NUM; // start from user page
	rbfm_ScanIterator.curRid.slotNum = 0; // start from 1

	// set the total page number
	rbfm_ScanIterator.totalPageNum = fileHandle.getNumberOfPages();
	// set the table Name
	rbfm_ScanIterator.tableName = fileHandle.fileName;
	// set the condition name
	rbfm_ScanIterator.conditionName = conditionAttribute;
	// set the conditon attr type
	rbfm_ScanIterator.conditionType = conditionAttrType;
	// set the projected name
	rbfm_ScanIterator.projectedName = attributeNames;
	// set the projected attr type
	rbfm_ScanIterator.projectedType = projectedType;


	return SUCC;
}

/*
 * Iterator
 */
RBFM_ScanIterator::RBFM_ScanIterator() :
		compOp(NO_OP), value(NULL), totalPageNum(0), conditionType(TypeInt){

}
RBFM_ScanIterator::~RBFM_ScanIterator() {
	if (value != NULL)
		delete []value;
}
RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	VersionManager *vm = VersionManager::instance();
	RC rc;

	FileHandle fileHandle;
	// open file handle
	rc = rbfm->openFile(tableName, fileHandle);
	if (rc != SUCC) {
		cerr << "RBFM_ScanIterator::getNextRecord: open file error " << rc << endl;
		return rc;
	}

	bool flag_NOT_EOF = true;
	vector<Attribute> attrs;
	if(curRid.pageNum >= totalPageNum)
		flag_NOT_EOF = false;
	// iterate to each page
	for (PageNum pageNum = curRid.pageNum;
			flag_NOT_EOF && pageNum < totalPageNum; ++pageNum) {
		// read the page
		rc = fileHandle.readPage(pageNum, page);
		if (rc != SUCC) {
			cerr << "RBFM_ScanIterator::getNextRecord: read page error " << rc << endl;
			return rc;
		}

		// get the total number slots
		SlotNum totalSlotNum = rbfm->getNumSlots(page);
		if (curRid.slotNum + 1 > totalSlotNum &&
				curRid.pageNum >= totalPageNum - 1)
			flag_NOT_EOF = false;
		for (SlotNum slotNum = curRid.slotNum+1;
				flag_NOT_EOF && slotNum <= totalSlotNum; ++slotNum) {
			// read the current data
			RID rid;
			rid.pageNum = pageNum;
			rid.slotNum = slotNum;

			//  see if the data is deleted or forwarded by checking its directory
			SlotDir slotDir;
			rc = rbfm->getSlotDir(page, slotDir, slotNum);
			if (rc != SUCC) {
				cerr << "RBFM_ScanIterator::getNextRecord: get slot directory error " << rc << endl;
				return rc;
			}
			if (slotDir.recordLength == RECORD_DEL || slotDir.recordLength == RECORD_FORWARD)
				continue;

			rc = rbfm->readRecord(fileHandle, attrs, rid, tuple);
			if (rc != SUCC) {
				cerr << "RBFM_ScanIterator::getNextRecord: read record error " << rc << endl;
				return rc;
			}
			// get the version number of current data
			VersionNumber curVer = VersionNumber(*((int *)tuple));
			// get the current attribute descriptor of  data
			rc = vm->getAttributes(tableName, attrs, curVer);
			if (rc != SUCC) {
				cerr << "RBFM_ScanIterator::getNextRecord: read attribute error " << rc << endl;
				return rc;
			}

			// get the condition value
			rc = rbfm->readAttribute(fileHandle, attrs, rid, conditionName, attrData);
			if (rc != SUCC) {
				cerr << "RBFM_ScanIterator::getNextRecord: read attribute value error " << rc << endl;
				return rc;
			}
			// the value meets the requirement, prepare output data
			if (value == NULL || conditionName.empty() ||
					compareValue(attrData, conditionType)) {
				rc = prepareData(fileHandle, attrs, rid, data);
				if (rc != SUCC) {
					cerr << "RBFM_ScanIterator::getNextRecord: prepare data error " << rc << endl;
					return rc;
				}

				// save the current RID
				if (totalSlotNum == rid.slotNum){
					curRid.pageNum = rid.pageNum+1, curRid.slotNum = 0;
				} else {
					curRid.pageNum = rid.pageNum, curRid.slotNum = rid.slotNum;
				}
				// exit
				rc = rbfm->closeFile(fileHandle);
				if (rc != SUCC) {
					cerr << "RBFM_ScanIterator::getNextRecord: open file error " << rc << endl;
					return rc;
				}
				return SUCC;
			}
		}
		if (curRid.slotNum + 1 > totalSlotNum &&
				curRid.pageNum >= totalPageNum - 1)
			flag_NOT_EOF = false;
	}

	rc = rbfm->closeFile(fileHandle);
	if (rc != SUCC) {
		cerr << "RBFM_ScanIterator::getNextRecord: open file error " << rc << endl;
		return rc;
	}

	return RBFM_EOF;

}
RC RBFM_ScanIterator::prepareData(FileHandle &fileHandle,
		  const vector<Attribute> &recordDescriptor,
		  const RID &rid, void *data) {
	char *returnedData = (char *)data;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc;

	for (int i = 0; i < int(projectedName.size()); ++i) {
		// get the condition value
		rc = rbfm->readAttribute(fileHandle, recordDescriptor,
				rid, projectedName[i], returnedData);
		if (rc != SUCC) {
			cerr << "RBFM_ScanIterator::getNextRecord: read attribute value error " << rc << endl;
			return rc;
		}
		switch(projectedType[i]) {
		case TypeInt:
			returnedData += sizeof(int);
			break;
		case TypeReal:
			returnedData += sizeof(float);
			break;
		case TypeVarChar:
			int len = *((int *)returnedData);
			returnedData += sizeof(int) + len;
			break;
		}
	}
	return SUCC;
}
// compare the two attribute data
bool RBFM_ScanIterator::compareValue(const void *record, const AttrType &type) {
	if (compOp == NO_OP)
		return true;
	int lhs_int(0), rhs_int(0), len_lhs(0), len_rhs(0);
	float lhs_float(0.), rhs_float(0.);
	char *lhs_cstr(NULL), *rhs_cstr(NULL);
	string lhs_str, rhs_str;
	switch(type) {
	case TypeInt:
		lhs_int = *((int *)record);
		rhs_int = *((int *)value);
		return compareValueTemplate(lhs_int, rhs_int);
		break;
	case TypeReal:
		lhs_float = *((float *)record);
		rhs_float = *((float *)value);
		return compareValueTemplate(lhs_float, rhs_float);
		break;
	case TypeVarChar:
		len_lhs = *((int *)record);
		len_rhs = *((int *)value);
		lhs_cstr = new char[len_lhs+1];
		rhs_cstr = new char[len_rhs+1];
		memcpy(lhs_cstr, (char *)record+sizeof(int), len_lhs);
		memcpy(rhs_cstr, (char *)value+sizeof(int), len_rhs);
		lhs_cstr[len_lhs] = '\0';
		rhs_cstr[len_rhs] = '\0';
		lhs_str.assign(lhs_cstr);
		rhs_str.assign(rhs_cstr);
		delete []lhs_cstr;
		delete []rhs_cstr;
		return compareValueTemplate(lhs_str, rhs_str);
		break;
	}
	return false;
}

bool RBFM_ScanIterator::compareString(const string &lhs, const string &rhs) {
	switch(compOp) {
	case EQ_OP:
		return lhs.compare(rhs) == 0;
		break;
	case LT_OP:
		return lhs.compare(rhs) < 0;
		break;
	case GT_OP:
		return lhs.compare(rhs) > 0;
		break;
	case LE_OP:
		return lhs.compare(rhs) <= 0;
		break;
	case GE_OP:
		return lhs.compare(rhs) >= 0;
		break;
	case NE_OP:
		return lhs.compare(rhs) != 0;
		break;
	default:
		return true;
	}
	return true;
}
RC RBFM_ScanIterator::close() {
	if (value != NULL)
		delete []value;
	value = NULL;
	return SUCC;
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
    for (int i = 0; i < int(numFields); ++i) {
    	string type;
    	stringstream ss;
    	int length(0);
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
unsigned RecordBasedFileManager::getRecordSize(
		const void *formatted,
		const vector<Attribute> &rD) {
	unsigned recordSize = 0;
	char *formattedData = (char *)formatted;

	unsigned numFields = rD.size();

	for (int i = 0; i < int(numFields); ++i) {
		switch(rD[i].type) {
		case TypeInt:
			recordSize += sizeof(int);
			formattedData += sizeof(int);
			break;
		case TypeReal:
			recordSize += sizeof(int);
			formattedData += sizeof(int);
			break;
		case TypeVarChar:
			int len = *((int *)(formattedData));
			recordSize += sizeof(int) + len;
			formattedData += sizeof(int) + len;
			break;
		}
	}
	return recordSize;
}
// Create an empty page
void RecordBasedFileManager::setPageEmpty(void *page) {
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
	for (int i = 0; i < int(recordDescriptor.size()); ++i) {
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
int RecordBasedFileManager::getFreeSpaceSize(void *page) {
	char *beg = (char *)page + PAGE_SIZE - sizeof(FieldAddress);
	FieldAddress addr_beg = *((FieldAddress *)(beg));
	int space = PAGE_SIZE - addr_beg;
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
// get next available num slots
SlotNum RecordBasedFileManager::getNextAvailableSlot(void *page) {
	SlotNum totalNumSLots = getNumSlots(page);
	SlotDir slotDir;
	for (SlotNum slot = 1; slot <= totalNumSLots; ++slot) {
		getSlotDir(page, slotDir, slot);
		if (slotDir.recordLength == RECORD_DEL) {
			return slot;
		}
	}
	return totalNumSLots+1;
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

// open up a Table
RC VersionManager::initTableVersionInfo(const string &tableName,
		FileHandle &fileHandle) {
	RC rc;
	vector<Attribute> attrs;
	VersionInfoFrame verInfoFrame;
	Attribute attr;
	// load attribute pages
	for (int pageNum = 1; pageNum < TABLE_PAGES_NUM; ++pageNum) {
		// read the specific page
		rc = fileHandle.readPage(pageNum, page);
		if (rc != SUCC) {
			cerr << "initTableVersionInfo: error to read page " << rc << endl;
			return rc;
		}
		// get number of attributes
		unsigned numAttrs = getNumberAttributes(page);
		// for all attribute saved in the page
		// read them and translate them into attributes
		for (unsigned i = 0; i < numAttrs; ++i) {
			get_ithVersionInfo(page, i, verInfoFrame);
			attr.length = verInfoFrame.attrLength;
			attr.name.assign(verInfoFrame.name);
			attr.type = verInfoFrame.attrType;
			attrs.push_back(attr);
		}
	}

	// read the first page
	// load the first page with versions
	rc = fileHandle.readPage(0, page);
	if (rc != SUCC) {
		cerr << "insertTableVersionInfo: read page" << rc << endl;
		return rc;
	}
	// get the version number
	VersionNumber verNum;
	getVersionNumber(tableName, page, verNum);

	// insert the version number to the version map
	versionMap[tableName] = verNum;

	// retrieve the history of the attributes
	RecordDescriptor recordDescriptor;
	vector<RecordDescriptor> recordDescriptorArray(verNum+1, recordDescriptor);

	recordDescriptorArray[verNum] = attrs;
	// now iteratively get the history of attributes
	// move to the att info area
	char *data = (char *)page;
	for (int i = verNum-1; i >= 0; --i) {
		// read in the data
		get_ithVersionInfo(data, i, verInfoFrame);
		attr.name.assign(verInfoFrame.name);
		attr.length = verInfoFrame.attrLength;
		attr.type = verInfoFrame.attrType;
		/*
		 * 			Debug info
		cout << "name: " << attr.name << "\tlength: " << attr.length <<
				"\ttype: " << attr.type << "\tcolumn: " << int(verInfoFrame.AttrColumn) <<
				"\taction: " << int(verInfoFrame.verChangeAction) << endl;
				*/
		if (verInfoFrame.verChangeAction == ADD_ATTRIBUTE) {
			// this means one attribute must be deleted to fit older attribute
			attrs.erase(attrs.begin() + verInfoFrame.AttrColumn);
		} else { // this means one attribute must be add back
			attrs.insert(attrs.begin() + verInfoFrame.AttrColumn, attr);
		}
		recordDescriptorArray[i] = attrs;
	}
	// insert the history attributes to the attribute map
	attrMap[tableName] = recordDescriptorArray;
	return SUCC;
}

RC VersionManager::loadTable(const string &tableName) {
	FileHandle fileHandle;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc;
	rc = rbfm->openFile(tableName, fileHandle);
	if (rc != SUCC) {
		cerr << "RelationManager::createTable: load file error " << rc << endl;
		return rc;
	}
	rc = initTableVersionInfo(tableName,fileHandle);
	if (rc != SUCC) {
		cerr << "RelationManager::createTable: load file error " << rc << endl;
		return rc;
	}
	rc = rbfm->closeFile(fileHandle);
	if (rc != SUCC) {
		cerr << "RelationManager::createTable: load file error " << rc << endl;
		return rc;
	}
	return SUCC;
}

// get version number publicly
RC VersionManager::getVersionNumber(const string &tableName,
		VersionNumber &ver) {
	VersionMap::iterator itr = versionMap.find(tableName);
	if(itr == versionMap.end()) {
		if (loadTable(tableName) != SUCC)
			return VERSION_TABLE_NOT_FOUND;
		else {
			itr = versionMap.find(tableName);
		}
	}
	ver = itr->second;

	return SUCC;
}

// get the attributes of a version
// Note that once load a table, the history attributes are cached
RC VersionManager::getAttributes(const string &tableName, vector<Attribute> &attrs,
		const VersionNumber ver) {
	AttrMap::iterator itr = attrMap.find(tableName);

	if (itr == attrMap.end()) {
		if (loadTable(tableName) != SUCC)
			return VERSION_TABLE_NOT_FOUND;
		else {
			itr = attrMap.find(tableName);
		}
	}

	vector<RecordDescriptor> &recordDescriptorArray = itr->second;
	attrs = recordDescriptorArray[ver];

	return SUCC;
}
// add an attribute
RC VersionManager::addAttribute(const string &tableName,
		const Attribute &attr, FileHandle &fileHandle) {
	AttrMap::iterator itr = attrMap.find(tableName);
	VersionMap::iterator itr_1 = versionMap.find(tableName);
	if (itr == attrMap.end() ||
			itr_1 == versionMap.end()) {
		if (loadTable(tableName) != SUCC)
			return VERSION_TABLE_NOT_FOUND;
		else {
			itr = attrMap.find(tableName);
			itr_1 = versionMap.find(tableName);
		}
	}

	// get version info to read and write
	VersionNumber &currentVersion = itr_1->second;
	vector<RecordDescriptor> &recordDescriptorArray = itr->second;
	vector<Attribute> &recordDescriptor = recordDescriptorArray[currentVersion];

	// update the version history in page 0
	// read page 0
	RC rc = fileHandle.readPage(0, page);
	if (rc != SUCC) {
		cerr << "add attribute: read page error." << endl;
		return rc;
	}

	// set the frame
	VersionInfoFrame verInfoFrame;
	verInfoFrame.attrLength = attr.length;
	verInfoFrame.attrType = attr.type;
	memcpy(verInfoFrame.name, attr.name.c_str(), attr.name.length());
	verInfoFrame.name[attr.name.length()] = '\0';
	verInfoFrame.verChangeAction = ADD_ATTRIBUTE;
	verInfoFrame.AttrColumn = recordDescriptor.size();
	// save the frame to the page
	set_ithVersionInfo(page, currentVersion, verInfoFrame);
	// update the version number
	currentVersion = (currentVersion + 1) % MAX_VER;
	// save the new version
	setVersionNumber(tableName, page, currentVersion);
	// write the page
	rc = fileHandle.writePage(0, page);
	if (rc != SUCC) {
		cerr << "add attribute: write page error." << rc << endl;
		return rc;
	}

	// update record descriptor
	recordDescriptorArray.push_back(recordDescriptor);
	recordDescriptorArray[currentVersion].push_back(attr);

	// save the attribute changes to the page
	rc = resetAttributePages(recordDescriptorArray[currentVersion], fileHandle);
	if (rc != SUCC) {
		cerr << "add attribute: resetAttributePages." << rc << endl;
		return rc;
	}

	return SUCC;
}
// drop an attribute
RC VersionManager::dropAttribute(const string &tableName,
		const string &attributeName, FileHandle &fileHandle) {
	AttrMap::iterator itr = attrMap.find(tableName);
	VersionMap::iterator itr_1 = versionMap.find(tableName);
	if (itr == attrMap.end() ||
			itr_1 == versionMap.end()) {
		if (loadTable(tableName) != SUCC)
			return VERSION_TABLE_NOT_FOUND;
		else {
			itr = attrMap.find(tableName);
			itr_1 = versionMap.find(tableName);
		}
	}

	// get version info to read and write
	VersionNumber &currentVersion = itr_1->second;
	vector<RecordDescriptor> &recordDescriptorArray = itr->second;
	vector<Attribute> recordDescriptor = recordDescriptorArray[currentVersion];

	// update the version history in page 0
	// read page 0
	RC rc = fileHandle.readPage(0, page);
	if (rc != SUCC) {
		cerr << "add attribute: read page error." << endl;
		return rc;
	}
	// format VersionInfo Frame
	int deleteIndex = 0;
	vector<Attribute>::iterator deleteItr = recordDescriptor.begin();
	for (deleteIndex = 0; deleteIndex < int(recordDescriptor.size()); ++deleteIndex) {
		if (recordDescriptor[deleteIndex].name == attributeName)
			break;
		++deleteItr;
	}
	if (deleteIndex == int(recordDescriptor.size()))
		return ATTR_NOT_FOUND;

	// set the frame
	VersionInfoFrame verInfoFrame;
	verInfoFrame.attrLength = recordDescriptor[deleteIndex].length;
	verInfoFrame.attrType = recordDescriptor[deleteIndex].type;
	memcpy(verInfoFrame.name, recordDescriptor[deleteIndex].name.c_str(),
			recordDescriptor[deleteIndex].name.length());
	verInfoFrame.name[recordDescriptor[deleteIndex].name.length()] = '\0';
	verInfoFrame.verChangeAction = DROP_ATTRIBUTE;
	verInfoFrame.AttrColumn = deleteIndex;
	// save the frame to the page
	set_ithVersionInfo(page, currentVersion, verInfoFrame);
	// update the version number
	currentVersion = (currentVersion + 1) % MAX_VER;
	// save the new version
	setVersionNumber(tableName, page, currentVersion);
	// write the page
	rc = fileHandle.writePage(0, page);
	if (rc != SUCC) {
		cerr << "add attribute: write page error." << rc << endl;
		return rc;
	}

	// update record descriptor
	recordDescriptor.erase(deleteItr);
	recordDescriptorArray.push_back(recordDescriptor);

	// save the attribute changes to the page
	rc = resetAttributePages(recordDescriptorArray[currentVersion], fileHandle);
	if (rc != SUCC) {
		cerr << "add attribute: resetAttributePages." << rc << endl;
		return rc;
	}

	return SUCC;
}

// translate a data to latest version
RC VersionManager::translateData2LastedVersion(const string &tableName,
		const VersionNumber &currentVersion,
		const void *oldData, void *latestData) {
	AttrMap::iterator itrAttr = attrMap.find(tableName);
	VersionMap::iterator itrVer = versionMap.find(tableName);
	if (itrAttr == attrMap.end() ||
			itrVer == versionMap.end()) {
		if (loadTable(tableName) != SUCC)
			return VERSION_TABLE_NOT_FOUND;
		else {
			itrAttr = attrMap.find(tableName);
			itrVer = versionMap.find(tableName);
		}
	}

	if (currentVersion > itrVer->second) {
		return VERSION_OVERFLOW;
	}

	// get the attribute of the old data
	vector<Attribute> oldAttrs;
	RC rc = getAttributes(tableName, oldAttrs, currentVersion);
	if (rc != SUCC) {
		cerr << "translateData2LastedVersion: get attribute " << rc << endl;
		return rc;
	}

	// if the currentVersion is the lasted version
	// copy and return
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	if (currentVersion == itrVer->second) {
		unsigned oldDataSize = rbfm->getRecordSize(oldData, oldAttrs);
		memcpy(latestData, oldData, oldDataSize);
		return SUCC;
	}

	// if not, need to load the first page to get the version history
	FileHandle fileHandle;
	rc = rbfm->openFile(tableName, fileHandle);
	if (rc != SUCC) {
		cerr << "translateData2LastedVersion: open file " << rc << endl;
		return rc;
	}
	// load page 0
	rc = fileHandle.readPage(0, page);
	if (rc != SUCC) {
		cerr << "translateData2LastedVersion: load page 0 " << rc << endl;
		return rc;
	}
	VersionInfoFrame verInfoFrame;
	vector<Attribute> latestAttrs;
	rc = getAttributes(tableName, latestAttrs, itrVer->second);
	if (rc != SUCC) {
		cerr << "translateData2LastedVersion: get attribute 2 " << rc << endl;
		return rc;
	}
	// initial change log,
	// if it is 'n', stop;
	// it it is 'a', add default;
	// if it is 'r', original real; if it is 'i', original int; if it is 'v', original varchar;
	// if it is 'R', delete real; if it is 'I', delete int; if it is 'V', delete varchar;
	vector<int> historyChangeLog(oldAttrs.size() + latestAttrs.size(), 'n');
	for (int i = 0; i < int(oldAttrs.size()); ++i) {
		switch(oldAttrs[i].type) {
		case TypeInt:
			historyChangeLog[i] = 'i';
			break;
		case TypeVarChar:
			historyChangeLog[i] = 'v';
			break;
		case TypeReal:
			historyChangeLog[i] = 'r';
			break;
		}
	}
	for (VersionNumber ver = currentVersion; ver < itrVer->second; ++ver) {
		// get the change log of the version
		get_ithVersionInfo(page, ver, verInfoFrame);
		if (verInfoFrame.verChangeAction == ADD_ATTRIBUTE) {
			// if an attribute is added
			// the attribute is added to the tail
			historyChangeLog[verInfoFrame.AttrColumn] = 'a';
		} else {
			// if the attribute is dropped
			switch(verInfoFrame.attrType) {
			case TypeInt:
				historyChangeLog[verInfoFrame.AttrColumn] = 'I';
				break;
			case TypeVarChar:
				historyChangeLog[verInfoFrame.AttrColumn] = 'V';
				break;
			case TypeReal:
				historyChangeLog[verInfoFrame.AttrColumn] = 'R';
				break;
			}
		}
	}

	// restore data to latest version with help of history change log
	char *dest = (char *)latestData;
	char *src = (char *)oldData;
	int defaultVal = 0;
	int len = 0;
	for (int i = 0; i < int(historyChangeLog.size()); ++i) {
		if (historyChangeLog[i] == 'n') // reach the end quit
			break;
		switch(historyChangeLog[i]) {
		case 'i':
			memcpy(dest, src, sizeof(int));
			dest += sizeof(int);
			src += sizeof(int);
			break;
		case 'r':
			memcpy(dest, src, sizeof(float));
			dest += sizeof(float);
			src += sizeof(float);
			break;
		case 'v':
			len = *((int *)src) + sizeof(int);
			memcpy(dest, src,  len);
			dest += len;
			src += len;
			break;
		case 'a':
			memcpy(dest, &defaultVal, sizeof(int));
			dest += sizeof(int);
			src += sizeof(int);
			break;
		case 'R':
			src += sizeof(float);
			break;
		case 'V':
			len = *((int *)src) + sizeof(int);
			src += len;
			break;
		case 'I':
			src += sizeof(int);
			break;
		}
	}

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

void VersionManager::printAttributes(const string &tableName) {
	AttrMap::iterator itrMap = attrMap.find(tableName);
	if (itrMap == attrMap.end()) {
		cout << "Table cannot be found!" << endl;
		return;
	}
	VersionMap::iterator itrVer = versionMap.find(tableName);
	if (itrVer == versionMap.end()) {
		cout << "Table cannot be found!" << endl;
		return;
	}
	vector<RecordDescriptor> &recordDescriptorArray = attrMap[tableName];
	VersionNumber ver = versionMap[tableName];
	cout << "=================================================" << endl;
	cout << "No.\tAttribute Name\tAttribute Length\tAttribute Type" << endl;
	for (int i = 0; i < int(recordDescriptorArray.size()); ++i) {
		for (int j = 0; j < int(recordDescriptorArray[i].size()); ++j) {
			cout << j <<"\t" <<
					recordDescriptorArray[i][j].name << "\t" <<
					recordDescriptorArray[i][j].length << "\t" <<
					recordDescriptorArray[i][j].type << endl;
		}
		cout << "-------------------------------------------------" << endl;
	}
	cout << "=================================================" << endl;
	cout << "There are " << int(ver)+1 << " version(s)" << endl;
}
// tools below

// get/set number of attributes
unsigned VersionManager::getNumberAttributes(void *page) {
	char *data = (char *)page;
	data += (PAGE_SIZE - sizeof(unsigned));
	return *((unsigned *)data);
}
void VersionManager::setNumberAttributes(void *page, const unsigned numAttrs) {
	char *data = (char *)page;
	data += (PAGE_SIZE - sizeof(unsigned));
	memcpy(data, &numAttrs, sizeof(unsigned));
}

// set the version of the number
RC VersionManager::setVersionNumber(const string &tableName, void *page,
		const VersionNumber &ver) {
	if (ver >= MAX_VER)
		return VERSION_OVERFLOW;
	char *data = (char *)page + PAGE_SIZE - sizeof(VersionNumber);
	memcpy(data, &ver, sizeof(VersionNumber));
	return SUCC;
}
// get the version number
void VersionManager::getVersionNumber(const string &tableName, void *page,
		VersionNumber &ver) {
	char *data = (char *)page + PAGE_SIZE - sizeof(VersionNumber);
	ver = *((VersionNumber *)data);
}
// get i'th version information
void VersionManager::get_ithVersionInfo(void *page, VersionNumber ver,
		VersionInfoFrame &versionInfoFrame) {
	char *data = (char *)page;
	data += ver * sizeof(VersionInfoFrame);
	memcpy(&versionInfoFrame, data, sizeof(VersionInfoFrame));
}
// set i'th version Information
void VersionManager::set_ithVersionInfo(void *page, VersionNumber ver,
		const VersionInfoFrame &versionInfoFrame) {
	char *data = (char *)page;
	data += ver * sizeof(VersionInfoFrame);
	memcpy(data, &versionInfoFrame, sizeof(VersionInfoFrame));
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
	RC rc;

	// append TABLE_PAGES_NUM more pages
	unsigned totalNumPages = fileHandle.getNumberOfPages();
	if (totalNumPages < TABLE_PAGES_NUM) {
		// must new pages for table
		for (int i = 0; i < TABLE_PAGES_NUM; ++i) {
			rc = fileHandle.appendPage(page);
			if (rc != SUCC) {
				cerr << "formatFirst2Page: cannot append a new page" << endl;
				return rc;
			}
		}
	}


	// prepare first page
	rc = fileHandle.readPage(0, page);
	if (rc != SUCC) {
		cerr << "formatFirst2Page: cannot read the page" << endl;
		return rc;
	}
	// set current version to zero
	rc = setVersionNumber(tableName, page, 0);
	if (rc != SUCC) {
		cerr << "formatFirst2Page: cannot set version number" << endl;
		return rc;
	}
	// write page
	rc = fileHandle.writePage(0, page);
	if (rc != SUCC) {
		cerr << "formatFirst2Page: cannto write the page" << rc << endl;
		return rc;
	}

	// insert attribute data to the pages
	rc = resetAttributePages(attrs, fileHandle);
	if (rc != SUCC) {
		cerr << "formatFirst2Page: resetAttributePages" << rc << endl;
		return rc;
	}

	return SUCC;
}

// reset the attribute pages
RC VersionManager::resetAttributePages(const vector<Attribute> &attrs,
		FileHandle &fileHandle) {
	RC rc;
	// open page
	rc = fileHandle.readPage(1, page);
	if (rc != SUCC) {
		cerr << "resetAttributePages: read page error" << rc << endl;
		return rc;
	}
	// set the number of attributes
	setNumberAttributes(page, attrs.size());

	// insert data to the file
	VersionInfoFrame verInfoFrame;
	for (int i = 0; i < int(attrs.size()); ++i) {
		// set the info frame
		verInfoFrame.attrLength = attrs[i].length;
		verInfoFrame.attrType = attrs[i].type;
		memcpy(verInfoFrame.name, attrs[i].name.c_str(), attrs[i].name.length());
		verInfoFrame.name[attrs[i].name.length()] = '\0';
		verInfoFrame.verChangeAction = NO_ACTION_ATTRIBUTE;
		verInfoFrame.AttrColumn = i;

		// save the info frame to page
		set_ithVersionInfo(page, i, verInfoFrame);
	}

	// save to the disk
	rc = fileHandle.writePage(1, page);
	if (rc != SUCC) {
		cerr << "resetAttributePages: write page error" << rc << endl;
		return rc;
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

	attr.name = "AttrAction";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	recordDescriptor.push_back(attr);
}
