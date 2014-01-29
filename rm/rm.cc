
#include "rm.h"

RelationManager* RelationManager::_rm = 0;

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
	return RM_EOF;
}
RC RM_ScanIterator::close() {
	return -1;
}

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
	// prepare the data of attributes
	createAttrRecordDescriptor(recordAttributeDescriptor);
}

RelationManager::~RelationManager()
{
}

RC RelationManager::formatFirst2Page(const string &tableName,
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
	// prepare first page
	// set current version to zero
	RC rc = setVersionNumber(page, 0);
	if (rc != SUCC) {
		return rc;
	}

	// record RID
	RID rid;

	// append TABLE_PAGES_NUM more pages
	for (int i = 0; i < TABLE_PAGES_NUM-1; ++i) {
		rbfm->createEmptyPage(page);
		rc = fileHandle.appendPage(page);
		if (rc != SUCC) {
			cerr << "cannot append a new page" << endl;
			return rc;
		}
	}

	// insert data to the file
	for (int i = 0; i < attrs.size(); ++i) {
		translateAttribte2Record(attrs[i], record);
		rbfm->insertRecord(fileHandle, recordAttributeDescriptor, record, rid);
	}

	// set the free space such that users never use the page
	for (int i = 0; i < TABLE_PAGES_NUM; ++i) {
		// get the page
		fileHandle.readPage(i, page);
		// set the free space such that users never use the page
		rbfm->setFreeSpaceStartPoint(page, page+PAGE_SIZE);
		// write page to the disk
		fileHandle.writePage(i, page);
	}

	return SUCC;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc;
	// create the file
	rc = rbfm->createFile(tableName);
	if (rc != SUCC)
		return rc;

	// open the file
	FileHandle fileHandle;
	rc = rbfm->openFile(tableName, fileHandle);
	if (rc != SUCC)
		return rc;

	// create the first two pages
	rc = formatFirst2Page(tableName, attrs, fileHandle);
	if (rc != SUCC)
		return rc;

	// close the file
	rc = rbfm->closeFile(fileHandle);
	if (rc != SUCC)
		return rc;

	// save the retrieved record descriptor
	currentRecordDescriptor = attrs;

    return SUCC;
}

RC RelationManager::deleteTable(const string &tableName)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc;

	// just destroy the table
	rc = rbfm->destroyFile(tableName);

    return rc;
}
// TODO NOTE: this method must be invoked when adding/dropping attributes
RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc;

	// open the file
	FileHandle fileHandle;
	rc = rbfm->openFile(tableName, fileHandle);
	if (rc != SUCC)
		return rc;

	RID rid;
	unsigned totalSlotNum = -1;
	for (int i = 1; i < TABLE_PAGES_NUM; ++i) { // for all pages that stores attributes
		rid.pageNum = i;

		// read the specific page
		rc = fileHandle.readPage(rid.pageNum, page);
		if (rc != SUCC)
			return rc;
		// read the total number of slots
		totalSlotNum = rbfm->getNumSlots(page);

		// for all slot numbers in the page
		// read them and translate them into attributes
		for (int j = 0; j < totalSlotNum; ++j) {
			rid.slotNum = j;
			rc = rbfm->readRecord(fileHandle, recordAttributeDescriptor, rid, record);
			if (rc != SUCC)
				return rc;
			Attribute attr;
			translateRecord2Attribte(attr, record);
			attrs.push_back(attr);
		}
	}

	// close the file
	rc = rbfm->closeFile(fileHandle);
	if (rc != SUCC)
		return rc;

	// save the retrieved record descriptor
	currentRecordDescriptor = attrs;

    return SUCC;
}

RC RelationManager::openTable(const string &tableName, FileHandle &fileHandle) {
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc = rbfm->openFile(tableName, fileHandle);
	if(rc != SUCC) {
		cerr << "Open Table: error open file" << endl;
		return rc;
	}
	rc = fileHandle.readPage(0, page);
	if (rc != SUCC) {
		cerr << "Open Table: error read the page" << endl;
		return rc;
	}
	// get the current version
	currentVersionNumber = getVersionNumber(page);
	// get the current record descriptor

}

RC RelationManager::closeTable(const string &tableName, FileHandle &fileHandle) {

}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc;

	// open the file
	FileHandle fileHandle;
	rc = rbfm->openFile(tableName, fileHandle);
	if (rc != SUCC)
		return rc;

	// insert the record
	rc = rbfm->insertRecordWithVersion(fileHandle,
			currentRecordDescriptor, data, rid, currentVersionNumber);
	if (rc != SUCC)
		return rc;

	// close the file
	rc = rbfm->closeFile(fileHandle);
	if (rc != SUCC)
		return rc;

    return SUCC;
}

RC RelationManager::deleteTuples(const string &tableName)
{
    return -1;
    // TODO
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    return -1;
    // TODO
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    return -1;
    // TODO
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    return -1;
    // TODO
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    return -1;
    // TODO
}

RC RelationManager::reorganizePage(const string &tableName, const unsigned pageNumber)
{
    return -1;
    // TODO
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    return -1;
    // TODO
}

// Extra credit
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
    // TODO
}

// Extra credit
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
    // TODO
}

// Extra credit
RC RelationManager::reorganizeTable(const string &tableName)
{
	// TODO
    return -1;
}


// get the version of the number
VersionNumber RelationManager::getVersionNumber(void *page) {
	return *((VersionNumber *)page);
}
VersionNumber RelationManager::getVersionNumber() {
	return currentVersionNumber;
}
// set the version of the number
RC RelationManager::setVersionNumber(void *page, VersionNumber ver) {
	if (ver >= MAX_VER)
		return VERSION_OVERFLOW;
	memcpy(page, &ver, sizeof(VersionNumber));
	currentVersionNumber = ver;
	return SUCC;
}
// get i'th version information
RC RelationManager::get_ithVersionInfo(void *page, VersionNumber ver,
		VersionInfoFrame &versionInfoFrame) {
	// TODO data is stored as an record Need change
	if (ver >= MAX_VER)
		return VERSION_OVERFLOW;
	char *data = (char *)page;
	data += sizeof(VersionNumber) + ver * sizeof(VersionInfoFrame);
	memcpy(&versionInfoFrame, data, sizeof(VersionInfoFrame));
	return SUCC;
}
// set i'th version Information
RC RelationManager::set_ithVersionInfo(void *page, VersionNumber ver,
		const VersionInfoFrame &versionInfoFrame) {
	// TODO data is stored as an record Need change
	if (ver >= MAX_VER)
		return VERSION_OVERFLOW;
	char *data = (char *)page;
	data += sizeof(VersionNumber) + ver * sizeof(VersionInfoFrame);
	memcpy(data, &versionInfoFrame, sizeof(VersionInfoFrame));
	return SUCC;
}

// translate an attribute into a record
unsigned RelationManager::translateAttribte2Record(const Attribute & attr,
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
void RelationManager::translateRecord2Attribte(Attribute & attr, const void *record) {
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

// create attribute descriptor for attribute
void RelationManager::createAttrRecordDescriptor(vector<Attribute> &recordDescriptor) {
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


