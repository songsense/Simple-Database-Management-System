
#include "rm.h"

RelationManager* RelationManager::_rm = 0;

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
	if (rbfm_si.getNextRecord(rid, data) == RBFM_EOF)
		return RM_EOF;
	else
		return SUCC;
}
RC RM_ScanIterator::close() {
	return rbfm_si.close();
}

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();
    VersionManager::instance();
    return _rm;
}

RelationManager::RelationManager()
{
	headVersionAttribute.length = sizeof(int);
	headVersionAttribute.name = "Ver";
	headVersionAttribute.type = TypeInt;
}

RelationManager::~RelationManager()
{
}



RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	VersionManager *vm = VersionManager::instance();
	RC rc;
	// create the file
	rc = rbfm->createFile(tableName);
	if (rc != SUCC) {
		cerr << "RelationManager::createTable: error create file " << rc << endl;
		return rc;
	}

	// open the file
	FileHandle fileHandle;
	rc = rbfm->openFile(tableName, fileHandle);
	if (rc != SUCC) {
		cerr << "RelationManager::createTable: error open file " << rc << endl;
		return rc;
	}

	// create the first two pages
	// add an attribute to the front of the att desriptor
	vector<Attribute> verAttrs;
	verAttrs.push_back(headVersionAttribute);
	for (int i = 0; i < attrs.size(); ++i) {
		verAttrs.push_back(attrs[i]);
	}
	rc = vm->formatFirst2Page(tableName, verAttrs, fileHandle);
	if (rc != SUCC) {
		cerr << "RelationManager::createTable: error format pages " << rc << endl;
		return rc;
	}

	rc = vm->initTableVersionInfo(tableName,fileHandle);
	if (rc != SUCC) {
		cerr << "RelationManager::createTable: error initialize table " << rc << endl;
		return rc;
	}

	rc = rbfm->closeFile(fileHandle);
	if (rc != SUCC) {
		cerr << "RelationManager::createTable: error close file " << rc << endl;
		return rc;
	}

    return SUCC;
}

RC RelationManager::deleteTable(const string &tableName)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	VersionManager *vm = VersionManager::instance();
	RC rc;

	// just destroy the table
	rc = rbfm->destroyFile(tableName);
	if (rc != SUCC) {
		cerr << "RelationManager::deleteTable: destroy file error " << rc << endl;
		return rc;
	}

	vm->eraseTableVersionInfo(tableName);

    return rc;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	VersionManager *vm = VersionManager::instance();
	VersionNumber curVer(0);
	RC rc;

	rc = vm->getVersionNumber(tableName, curVer);
	if (rc != SUCC) {
		cerr << tableName << ": current version requested: " << curVer << endl;
		cerr << "getAttribute: get version number error " << rc << endl;
		return rc;
	}

	rc = vm->getAttributes(tableName, attrs, curVer);
	if (rc != SUCC) {
		cerr << "getAttribute: get attribute error " << rc << endl;
		return rc;
	}

	return SUCC;
}

RC RelationManager::openTable(const string &tableName, FileHandle &fileHandle) {
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc;
	VersionManager *vm = VersionManager::instance();

	rc = rbfm->openFile(tableName, fileHandle);
	if(rc != SUCC) {
		cerr << "Open Table: error open file" << rc << endl;
		return rc;
	}

	rc = vm->initTableVersionInfo(tableName,fileHandle);
	if(rc != SUCC) {
		cerr << "Open Table: error initialize catelog" << rc << endl;
		return rc;
	}

	return SUCC;
}

RC RelationManager::closeTable(const string &tableName, FileHandle &fileHandle) {
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc = rbfm->closeFile(fileHandle);
	if (rc != SUCC) {
		cerr << "Open Table: error close file" << rc << endl;
		return rc;
	}
	return SUCC;
}


RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	VersionManager *vm = VersionManager::instance();
	RC rc;

	// open the file
	FileHandle fileHandle;
	rc = rbfm->openFile(tableName, fileHandle);
	if (rc != SUCC) {
		cerr << "insertTuple: open file error " << rc << endl;
		return rc;
	}
	// get the current version
	VersionNumber curVer;
	rc = vm->getVersionNumber(tableName, curVer);
	if (rc != SUCC) {
		cerr << "insertTuple: get version error " << rc << endl;
		return rc;
	}
	// get the current version attribute
	vector<Attribute> attrs;
	rc = vm->getAttributes(tableName, attrs, curVer);
	if (rc != SUCC) {
		cerr << "insertTuple: get attribute error " << rc << endl;
		return rc;
	}

	// get the size of the record
	unsigned recordSize = getRecordSize(data, attrs);

	// add version to the tuple
	addVersion2Data(tuple, data, curVer, recordSize);

	// insert the tuple
	rc = rbfm->insertRecord(fileHandle, attrs, tuple, rid);
	if (rc != SUCC) {
		cerr << "insertTuple: insert record " << rc << endl;
		return rc;
	}
	// close the file
	rc = rbfm->closeFile(fileHandle);
	if (rc != SUCC) {
		cerr << "insertTuple: close file " << rc << endl;
		return rc;
	}

    return SUCC;
}

RC RelationManager::deleteTuples(const string &tableName)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc;

	// open the file
	FileHandle fileHandle;
	rc = rbfm->openFile(tableName, fileHandle);
	if (rc != SUCC) {
		cerr << "deleteTuples: open file error " << rc << endl;
		return rc;
	}

	// delete records
	rc = rbfm->deleteRecords(fileHandle);
	if (rc != SUCC) {
		cerr << "deleteTuples: delete records " << rc << endl;
		return rc;
	}

	// close the file
	rc = rbfm->closeFile(fileHandle);
	if (rc != SUCC) {
		cerr << "deleteTuples: close file " << rc << endl;
		return rc;
	}

    return SUCC;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc;

	// open the file
	FileHandle fileHandle;
	rc = rbfm->openFile(tableName, fileHandle);
	if (rc != SUCC) {
		cerr << "deleteTuple: open file error " << rc << endl;
		return rc;
	}

	// get the current version attribute
	vector<Attribute> attrs;
	rc = getAttributes(tableName, attrs);
	if (rc != SUCC) {
		cerr << "deleteTuple: get attribute error " << rc << endl;
		return rc;
	}

	rc = rbfm->deleteRecord(fileHandle, attrs, rid);
	if (rc != SUCC) {
		cerr << "deleteTuple: delete record error " << rc << endl;
		return rc;
	}

	// close the file
	rc = rbfm->closeFile(fileHandle);
	if (rc != SUCC) {
		cerr << "deleteTuple: close file error " << rc << endl;
		return rc;
	}

    return SUCC;
}

RC RelationManager::updateTuple(const string &tableName, const void *data,
		const RID &rid)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	VersionManager *vm = VersionManager::instance();
	RC rc;

	// open the file
	FileHandle fileHandle;
	rc = rbfm->openFile(tableName, fileHandle);
	if (rc != SUCC) {
		cerr << "updateTuple: open file error " << rc << endl;
		return rc;
	}
	// get the current version
	VersionNumber curVer;
	rc = vm->getVersionNumber(tableName, curVer);
	if (rc != SUCC) {
		cerr << "updateTuple: get version error " << rc << endl;
		return rc;
	}
	// get the current version attribute
	vector<Attribute> attrs;
	rc = vm->getAttributes(tableName, attrs, curVer);
	if (rc != SUCC) {
		cerr << "updateTuple: get attribute error " << rc << endl;
		return rc;
	}

	// get the size of the record
	unsigned recordSize = getRecordSize(data, attrs);

	// add version to the tuple
	addVersion2Data(tuple, data, curVer, recordSize);

	// update the tuple
	rc = rbfm->updateRecord(fileHandle, attrs, tuple, rid);
	if (rc != SUCC) {
		cerr << "updateTuple: update tuple " << rc << endl;
		return rc;
	}

	// close the file
	rc = rbfm->closeFile(fileHandle);
	if (rc != SUCC) {
		cerr << "updateTuple: close file " << rc << endl;
		return rc;
	}

    return SUCC;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	// in rbfm, there is no need to read a tuple with attrs
	// therefore, here we feed rbfm with an empty attrs desriptor
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	VersionManager *vm = VersionManager::instance();
	RC rc;

	// open the file
	FileHandle fileHandle;
	rc = rbfm->openFile(tableName, fileHandle);
	if (rc != SUCC) {
		cerr << "readTuple: open file error " << rc << endl;
		return rc;
	}

	// get the current version attribute
	vector<Attribute> attrs;
	rc = getAttributes(tableName, attrs);
	if (rc != SUCC) {
		cerr << "readTuple: read the attribute " << rc << endl;
		return rc;
	}

	rc = rbfm->readRecord(fileHandle, attrs, rid, tuple);
	if (rc != SUCC) {
		cerr << "readTuple: read the record " << rc << endl;
		return rc;
	}

	// get the version of the data
	VersionNumber ver = VersionNumber(*((int *)tuple));

	// translate the data to the latest version
	char latestTuple[PAGE_SIZE];
	rc = vm->translateData2LastedVersion(tableName, ver, tuple, latestTuple);
	// minus sizeof(int) for it is a TypeInt that holds the version number
	unsigned recordSize = rbfm->getRecordSize(latestTuple, attrs)-sizeof(int);
	memcpy(data, latestTuple + sizeof(int), recordSize);

	if (rc != SUCC) {
		cerr << "readTuple: translate tuple to latest version " << rc << endl;
		return rc;
	}

	// close the file
	rc = rbfm->closeFile(fileHandle);
	if (rc != SUCC) {
		cerr << "readTuple: close file " << rc << endl;
		return rc;
	}

    return SUCC;
}

RC RelationManager::readAttribute(const string &tableName,
		const RID &rid, const string &attributeName, void *data)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc;

	// open the file
	FileHandle fileHandle;
	rc = rbfm->openFile(tableName, fileHandle);
	if (rc != SUCC) {
		cerr << "RelationManager::readAttribute: open file error " << rc << endl;
		return rc;
	}

	// get the current version attribute
	vector<Attribute> attrs;
	rc = getAttributes(tableName, attrs);
	if (rc != SUCC) {
		cerr << "RelationManager::readAttribute: get attribute error " << rc << endl;
		return rc;
	}

	// read the attribute
	rc = rbfm->readAttribute(fileHandle, attrs, rid, attributeName, data);
	if (rc != SUCC) {
		cerr << "RelationManager::readAttribute: read attribute error " << rc << endl;
		return rc;
	}

	// close the file
	rc = rbfm->closeFile(fileHandle);
	if (rc != SUCC) {
		cerr << "RelationManager::readAttribute: close file " << rc << endl;
		return rc;
	}
    return SUCC;
}

RC RelationManager::reorganizePage(const string &tableName, const unsigned pageNumber)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc;

	// open the file
	FileHandle fileHandle;
	rc = rbfm->openFile(tableName, fileHandle);
	if (rc != SUCC) {
		cerr << "RelationManager::reorganizePage: open file error " << rc << endl;
		return rc;
	}
	// get the current version attribute
	vector<Attribute> attrs;
	rc = getAttributes(tableName, attrs);
	if (rc != SUCC) {
		cerr << "RelationManager::reorganizePage: get attribute error " << rc << endl;
		return rc;
	}

	// reorganizePage
	rc = rbfm->reorganizePage(fileHandle, attrs, pageNumber);
	if (rc != SUCC) {
		cerr << "RelationManager::reorganizePage: reorganize page erro " << rc << endl;
		return rc;
	}

	// close the file
	rc = rbfm->closeFile(fileHandle);
	if (rc != SUCC) {
		cerr << "RelationManager::reorganizePage: close file error " << rc << endl;
		return rc;
	}
    return SUCC;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	VersionManager *vm = VersionManager::instance();
	RC rc;
	FileHandle fileHandle;

	// open file handle;
	rc = rbfm->openFile(tableName, fileHandle);
	if (rc != SUCC) {
		cerr << "RelationManager::scan open file error " << rc << endl;
		return rc;
	}

	// get attributes and versions
	vector<Attribute> recordDesriptor;
	VersionNumber ver;
	rc = vm->getVersionNumber(tableName, ver);
	if (rc != SUCC) {
		cerr << "RelationManager::scan: get version error " << rc << endl;
		return rc;
	}
	rc = vm->getAttributes(tableName, recordDesriptor, ver);
	if (rc != SUCC) {
		cerr << "RelationManager::scan: get attributes error " << rc << endl;
		return rc;
	}

	rc = rbfm->scan(fileHandle, recordDesriptor,
			conditionAttribute, compOp, value,
			attributeNames, rm_ScanIterator.rbfm_si);
	if (rc != SUCC) {
		cerr << "RelationManager::scan: scan initialization error " << rc << endl;
		return rc;
	}

	rc = rbfm->closeFile(fileHandle);
	if (rc != SUCC) {
		cerr << "RelationManager::scan close file error " << rc << endl;
		return rc;
	}

	return SUCC;
}

// Extra credit
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	VersionManager *vm = VersionManager::instance();
	RC rc;

	// open the file
	FileHandle fileHandle;
	rc = rbfm->openFile(tableName, fileHandle);
	if (rc != SUCC) {
		cerr << "dropAttribute: open file error " << rc << endl;
		return rc;
	}

	// drop attribute
	rc = vm->dropAttribute(tableName, attributeName, fileHandle);
	if (rc != SUCC) {
		cerr << "dropAttribute: drop attribute error " << rc << endl;
		return rc;
	}

	// close the file
	rc = rbfm->closeFile(fileHandle);
	if (rc != SUCC) {
		cerr << "dropAttribute: close file error " << rc << endl;
		return rc;
	}

	return SUCC;
}

// Extra credit
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	VersionManager *vm = VersionManager::instance();
	RC rc;

	// open the file
	FileHandle fileHandle;
	rc = rbfm->openFile(tableName, fileHandle);
	if (rc != SUCC) {
		cerr << "addAttribute: open file error " << rc << endl;
		return rc;
	}

	// drop attribute
	rc = vm->addAttribute(tableName, attr, fileHandle);
	if (rc != SUCC) {
		cerr << "addAttribute: drop attribute error " << rc << endl;
		return rc;
	}

	// close the file
	rc = rbfm->closeFile(fileHandle);
	if (rc != SUCC) {
		cerr << "addAttribute: close file error " << rc << endl;
		return rc;
	}

	return SUCC;
}

// Extra credit
RC RelationManager::reorganizeTable(const string &tableName)
{
	// TODO
    return -1;
}

// add the version to data
void RelationManager::addVersion2Data(void *verData, const void *data,
		const VersionNumber &ver, const unsigned &recordSize) {
	int verInt = (int)ver;
	memcpy(verData, &verInt, sizeof(int));
	memcpy((char *)verData + sizeof(int), data, recordSize);
}

// get the record size: start from the second attr excluding the Ver
unsigned RelationManager::getRecordSize(const void *buf,
		  const vector<Attribute> &rD) {
	unsigned recordSize = 0;
	char *buffer = (char *)buf;

	unsigned numFields = rD.size();

	for (int i = 1; i < numFields; ++i) {
		switch(rD[i].type) {
		case TypeInt:
			recordSize += sizeof(int);
			buffer += sizeof(int);
			break;
		case TypeReal:
			recordSize += sizeof(int);
			buffer += sizeof(int);
			break;
		case TypeVarChar:
			int len = *((int *)(buffer));
			recordSize += sizeof(int) + len;
			buffer += sizeof(int) + len;
			break;
		}
	}
	return recordSize;
}


