
#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

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
	PageNum pageNum = 0;
	RC rc = itr->second.getPageSpaceInfo(recordDirectorySize, pageNum);
	if (rc == FILE_SPACE_EMPTY || rc == FILE_SPACE_NO_SPACE) {
		// the record is too big to fit into the page
		// create a new page
		createEmptyPage(pageContent);
		rc = fileHandle.appendPage(pageContent);
		if (rc != SUCC)
			cerr << "cannot append a new page" << endl;
		// obtain the newly allocated page's num
		pageNum = fileHandle.getNumberOfPages()-1;
	}
	// load the page data
	rc = fileHandle.readPage(pageNum, pageContent);

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
		cerr << "fail to set number of slots " << rc << endl;
		return rc;
	}
	// update the directory of the slot
	SlotDir slotDir;
	slotDir.recordLength = recordSize;
	slotDir.recordOffset = originalStartPoint;
	rc = setSlotDir(pageContent, slotDir, numSlots+1);
	if (rc != SUCC) {
		cerr << "fail to update the director of the slot " << rc << endl;
		return rc;
	}

	// write page into the file
	fileHandle.writePage(pageNum, pageContent);

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
	RC rc = fileHandle.readPage(rid.pageNum, pageContent);
	if (rc != SUCC) {
		cerr << "Reading Page error: " << rc << endl;
		return rc;
	}
	// get slot directory
	SlotDir slotDir;
	rc = getSlotDir(pageContent, slotDir, rid.slotNum);
	if (rc != SUCC) {
		cerr << " Reading slot directory error: " << rc << endl;
		return rc;
	}
	char *slot = (char *)slotDir.recordOffset;
	// translate the slot record to printable data
	translateRecord2Printable((void *)slot, data, recordDescriptor);
    return SUCC;
}
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
// Note: without header address
unsigned RecordBasedFileManager::translateRecord2Raw(void *raw,
		const void *formatted,
		const vector<Attribute> &rD) {
	unsigned recordSize = 0;
	char *rawData = (char *)raw;
	char *rawField = (char *)raw;
	char *formattedData = (char *)formatted;

	unsigned numFields = rD.size();
	FieldAddress startPointAddress = (FieldAddress)(rawData + sizeof(FieldAddress) +
			numFields * sizeof(FieldOffset));
	memcpy(rawData, &startPointAddress, sizeof(FieldAddress));
	recordSize += sizeof(FieldAddress) + numFields * sizeof(FieldOffset);
	rawData += recordSize;
	rawField += sizeof(FieldAddress);

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
	return recordSize + 4;
}
// Translate record to printable version
void RecordBasedFileManager::translateRecord2Printable(const void *raw,
		void *formatted,
		const vector<Attribute> &rD) {
	char *rawData = (char *)raw;
	char *formattedData = (char *)formatted;
	rawData += sizeof(FieldAddress);
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
	return (void *)addr;
}
void RecordBasedFileManager::setFreeSpaceStartPoint(void *page, void *startPoint) {
	FieldAddress addr = (FieldAddress)(startPoint);
	char *p = (char *)page + PAGE_SIZE - sizeof(FieldAddress);
	memcpy(p, &addr, sizeof(FieldAddress));
}
// calculate the size of free space
// Note this free space does NOT include the SLOT DIRECTORY
unsigned RecordBasedFileManager::getFreeSpaceSize(void *page) {
	char *beg = (char *)page + PAGE_SIZE - sizeof(FieldAddress);
	FieldAddress addr_beg = *((FieldAddress *)(beg));
	char *end = (char *)page + PAGE_SIZE;
	unsigned space = (FieldAddress)(end) - addr_beg;
	space -= getNumSlots(page) * sizeof(SlotDir) - sizeof(SlotNum);
	return space;
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
