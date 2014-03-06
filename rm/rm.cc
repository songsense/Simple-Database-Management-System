
#include "rm.h"

RelationManager* RelationManager::_rm = 0;

RC RelationManager::createIndex(const string &tableName, const string &attributeName) {
	RC rc;
	IndexManager *ix = IndexManager::instance();
	string indexName;
	makeIndexName(tableName, attributeName, indexName);

	rc = ix->createFile(indexName);
	if (rc != SUCC) {
		cerr << "createIndex: createFile error " << rc << endl;
		return rc;
	}

	FileHandle *indexFileHandle;
	rc = openIndex(tableName, attributeName, indexFileHandle);
	if (rc != SUCC) {
		cerr << "createIndex: open index error " << rc << endl;
		return rc;
	}

	FileHandle *tableFileHandle;
	rc = openTable(tableName, tableFileHandle);
	if (rc != SUCC) {
		cerr << "createIndex: open table error " << rc << endl;
		return rc;
	}

	vector<string> attributeNames;
	attributeNames.push_back(attributeName);

	RM_ScanIterator rmsi;

	rc = scan(tableName, "", NO_OP, NULL, attributeNames, rmsi);
	if (rc != SUCC) {
		cerr << "createIndex: scan table error " << rc << endl;
		return rc;
	}

	RID rid;
	char returnedData[PAGE_SIZE];
	Attribute attr;
	rc = getSpecificAttribute(tableName, attributeName, attr);
	if (rc != SUCC) {
		cerr << "createIndex: getSpecificAttribute error " << rc << endl;
		return rc;
	}
	while(rmsi.getNextTuple(rid, returnedData) != RM_EOF) {
		rc = ix->insertEntry(*indexFileHandle, attr, returnedData, rid);
		if (rc != SUCC) {
			cerr << "createIndex: insertEntry error " << rc << endl;
			return rc;
		}
	}

	return SUCC;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName) {
	RC rc;
	IndexManager *ix = IndexManager::instance();
	string indexName;
	makeIndexName(tableName, attributeName, indexName);

	rc = closeIndex(tableName, attributeName);
	if (rc != SUCC) {
		cerr << "destroyIndex: closeIndex error " << rc << endl;
		return rc;
	}

	rc = ix->destroyFile(indexName);
	if (rc != SUCC) {
		cerr << "destroyIndex: destroyFile error " << rc << endl;
		return rc;
	}

	return SUCC;
}

// get specific attribute
RC RelationManager::getSpecificAttribute(const string &tableName, const string &attributeName, Attribute &attr) {
	RC rc;
	vector<Attribute> attrs;
	rc = getAttributes(tableName, attrs);
	if (rc != SUCC) {
		cerr << "getSpecificAttribute: get attribute error " << rc << endl;
		return rc;
	}

	bool flag = false;
	for (unsigned i = 0; i < attrs.size(); ++i) {
		if (attrs[i].name == attributeName) {
			attr.length = attrs[i].length;
			attr.name = attrs[i].name;
			attr.type = attrs[i].type;
			flag = true;
			break;
		}
	}

	if (flag == false) {
		rc = RM_CANNOT_FIND_ATTRIBUTE;
		cerr << "getSpecificAttribute: get attribute error " << rc << endl;
		return rc;
	}

	return SUCC;
}

// indexScan returns an iterator to allow the caller to go through qualified entries in index
RC RelationManager::indexScan(const string &tableName,
		const string &attributeName,
		const void *lowKey,
		const void *highKey,
		bool lowKeyInclusive,
		bool highKeyInclusive,
		RM_IndexScanIterator &rm_IndexScanIterator) {
	RC rc;
	IndexManager *ix = IndexManager::instance();
	// open the file
	string indexName;
	makeIndexName(tableName, attributeName, indexName);
	FileHandle *fileHandle;
	rc = openIndex(tableName, attributeName, fileHandle);
	if (rc != SUCC) {
		cerr << "indexScan: open index file error " << rc << endl;
		return rc;
	}

	Attribute attr;
	rc = getSpecificAttribute(tableName, attributeName, attr);
	if (rc != SUCC) {
		cerr << "indexScan: get specific attribute error " << rc << endl;
		return rc;
	}

	rc = ix->scan(*fileHandle, attr, lowKey, highKey, lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator.ix_scanIterator);
	if (rc != SUCC) {
		cerr << "indexScan: scan error " << rc << endl;
		return rc;
	}

	return SUCC;
}

// Get next matching entry
RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key) {
	return ix_scanIterator.getNextEntry(rid, key);
}
// Terminate index scan
RC RM_IndexScanIterator::close() {
	return ix_scanIterator.close();
}


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
	// add an attribute to the front of the attr desriptor
	vector<Attribute> verAttrs;
	verAttrs.push_back(headVersionAttribute);
	for (int i = 0; i < int(attrs.size()); ++i) {
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

	closeTable(tableName);

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

RC RelationManager::openTable(const string &tableName, FileHandle *&fileHandle) {
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc;
	VersionManager *vm = VersionManager::instance();

	if (cachedTableFileHandles.count(tableName) > 0) {
		fileHandle = cachedTableFileHandles[tableName];
		return SUCC;
	}
	FileHandle *fhandle = new FileHandle();

	rc = rbfm->openFile(tableName, *fhandle);
	if(rc != SUCC) {
		cerr << "Open Table: error open file " << rc << endl;
		return rc;
	}

	rc = vm->initTableVersionInfo(tableName, *fhandle);
	if(rc != SUCC) {
		cerr << "Open Table: error initialize catelog" << rc << endl;
		return rc;
	}

	fileHandle = fhandle;
	cachedTableFileHandles[tableName] = fhandle;

	return SUCC;
}

RC RelationManager::closeTable(const string &tableName) {
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

	if (cachedTableFileHandles.count(tableName) == 0) {
		return SUCC;
	}

	FileHandle *fhandle = cachedTableFileHandles[tableName];

	RC rc = rbfm->closeFile(*fhandle);
	if (rc != SUCC) {
		cerr << "Open Table: error close file " << rc << endl;
		return rc;
	}

	cachedTableFileHandles.erase(tableName);
	delete fhandle;

	return SUCC;
}

void RelationManager::makeIndexName(const string &tableName, const string &attributeName,
		  string &indexName) {
	indexName = tableName + "_" + attributeName;
}

RC RelationManager::openIndex(const string &tableName,
		  const string &attributeName, FileHandle *&fileHandle) {
	RC rc;
	IndexManager *ix = IndexManager::instance();
	string indexName;
	makeIndexName(tableName, attributeName, indexName);
	if (cachedIndexFileHandles.count(indexName) > 0) {
		fileHandle = cachedIndexFileHandles[indexName];
		return SUCC;
	}

	// cannot find a file handle, must new one
	FileHandle *fhandle = new FileHandle();
	rc = ix->openFile(indexName, *fhandle);
	if(rc != SUCC) {
		cerr << "Open Index: error open file " << rc << endl;
		return rc;
	}
	// cache the file handle of the index file
	fileHandle = fhandle;
	cachedIndexFileHandles[indexName] = fhandle;

	// cache the attribute of the index file
	Attribute attr;
	rc = getSpecificAttribute(tableName, attributeName, attr);
	if(rc != SUCC) {
		cerr << "Open Index: error getSpecificAttribute " << rc << endl;
		return rc;
	}
	cachedAttributes[indexName] = attr;

	return SUCC;
}
RC RelationManager::closeIndex(const string &tableName, const string &attributeName) {
	IndexManager *ix = IndexManager::instance();
	string indexName;
	makeIndexName(tableName, attributeName, indexName);

	if (cachedIndexFileHandles.count(indexName) == 0) {
		return SUCC;
	}

	FileHandle *fhandle = cachedTableFileHandles[indexName];

	RC rc = ix->closeFile(*fhandle);
	if (rc != SUCC) {
		cerr << "closeIndex: error close file " << rc << endl;
		return rc;
	}

	cachedIndexFileHandles.erase(indexName);
	delete fhandle;

	cachedAttributes.erase(indexName);

	return SUCC;
}

bool RelationManager::isIndexExist(const string &tableName, const string &attributeName) {
	string indexName;
	makeIndexName(tableName, attributeName, indexName);
	PagedFileManager *pfm = PagedFileManager::instance();
	if (pfm->fileExist(indexName.c_str())) {
		return true;
	} else {
		return false;
	}
}

RC RelationManager::insertIndex(const string &tableName, const RID &rid) {
	RC rc;
	vector<Attribute> attrs;
	IndexManager *ix = IndexManager::instance();

	// get attribute
	rc = getAttributes(tableName, attrs);
	if (rc != SUCC) {
		cerr << "insertIndex: getAttributes error " << rc << endl;
		return rc;
	}

	char key[PAGE_SIZE];

	// iterate to see it there exists an index file
	for (Attribute attr : attrs) {
		if (isIndexExist(tableName, attr.name)) {
			// open the index file
			FileHandle *fileHandle;
			rc = openIndex(tableName, attr.name, fileHandle);
			if (rc != SUCC) {
				cerr << "insertIndex: openIndex error " << rc << endl;
				return rc;
			}
			// read the key
			rc = readAttribute(tableName, rid, attr.name, key);
			if (rc != SUCC) {
				cerr << "insertIndex: readAttribute error " << rc << endl;
				return rc;
			}
			// insert the key into the index file
			rc = ix->insertEntry(*fileHandle, attr, key, rid);
			if (rc != SUCC) {
				cerr << "insertIndex: insertEntry error " << rc << endl;
				return rc;
			}
		}
	}
	return SUCC;
}
RC RelationManager::deleteIndex(const string &tableName, const RID &rid) {
	RC rc;
	vector<Attribute> attrs;
	IndexManager *ix = IndexManager::instance();

	// get attribute
	rc = getAttributes(tableName, attrs);
	if (rc != SUCC) {
		cerr << "deleteIndex: getAttributes error " << rc << endl;
		return rc;
	}

	char key[PAGE_SIZE];

	// iterate to see it there exists an index file
	for (Attribute attr : attrs) {
		if (isIndexExist(tableName, attr.name)) {
			// open the index file
			FileHandle *fileHandle;
			rc = openIndex(tableName, attr.name, fileHandle);
			if (rc != SUCC) {
				cerr << "deleteIndex: openIndex error " << rc << endl;
				return rc;
			}
			// read the key
			rc = readAttribute(tableName, rid, attr.name, key);
			if (rc != SUCC) {
				cerr << "insertIndex: readAttribute error " << rc << endl;
				return rc;
			}
			// insert the key into the index file
			rc = ix->deleteEntry(*fileHandle, attr, key, rid);
			if (rc != SUCC) {
				cerr << "insertIndex: insertEntry error " << rc << endl;
				return rc;
			}
		}
	}
	return SUCC;
}
RC RelationManager::deleteIndices(const string &tableName) {
	RC rc;
	vector<Attribute> attrs;
	IndexManager *ix = IndexManager::instance();

	// get attribute
	rc = getAttributes(tableName, attrs);
	if (rc != SUCC) {
		cerr << "deleteIndices: getAttributes error " << rc << endl;
		return rc;
	}

	// iterate to see it there exists an index file
	for (Attribute attr : attrs) {
		if (isIndexExist(tableName, attr.name)) {
			// open the index file
			FileHandle *fileHandle;
			rc = openIndex(tableName, attr.name, fileHandle);
			if (rc != SUCC) {
				cerr << "deleteIndices: openIndex error " << rc << endl;
				return rc;
			}
			// delete all
			rc = ix->deleteEntries(*fileHandle);
			if (rc != SUCC) {
				cerr << "deleteIndices: deleteEntries error " << rc << endl;
				return rc;
			}
		}
	}
	return SUCC;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	VersionManager *vm = VersionManager::instance();
	RC rc;

	// open the file
	FileHandle *fileHandle;
	rc = openTable(tableName, fileHandle);
	if (rc != SUCC) {
		cerr << "insertTuple: open table " << tableName << " error " << rc << endl;
		return rc;
	}

	rc = rbfm->openFile(tableName, *fileHandle);
	if (rc != SUCC) {
		cerr << "insertTuple: open file " << tableName << " error " << rc << endl;
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
	rc = rbfm->insertRecord(*fileHandle, attrs, tuple, rid);
	if (rc != SUCC) {
		cerr << "insertTuple: insert record " << rc << endl;
		return rc;
	}

	// insert to the index
	rc = insertIndex(tableName, rid);
	if (rc != SUCC) {
		cerr << "insertTuple: insert index error " << rc << endl;
		return rc;
	}

    return SUCC;
}

RC RelationManager::deleteTuples(const string &tableName)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc;

	deleteIndices(tableName);

	// open the file
	FileHandle *fileHandle;
	rc = openTable(tableName, fileHandle);
	if (rc != SUCC) {
		cerr << "deleteTuples: open table " << tableName << " error " << rc << endl;
		return rc;
	}

	// delete records
	rc = rbfm->deleteRecords(*fileHandle);
	if (rc != SUCC) {
		cerr << "deleteTuples: delete records " << rc << endl;
		return rc;
	}

    return SUCC;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc;

	// delete from the index
	rc = deleteIndex(tableName, rid);
	if (rc != SUCC) {
		cerr << "deleteTuples: insert index error " << rc << endl;
		return rc;
	}

	// open the file
	FileHandle *fileHandle;
	rc = openTable(tableName, fileHandle);
	if (rc != SUCC) {
		cerr << "deleteTuple: open table " << tableName << " error " << rc << endl;
		return rc;
	}

	// get the current version attribute
	vector<Attribute> attrs;
	rc = getAttributes(tableName, attrs);
	if (rc != SUCC) {
		cerr << "deleteTuple: get attribute error " << rc << endl;
		return rc;
	}

	rc = rbfm->deleteRecord(*fileHandle, attrs, rid);
	if (rc != SUCC) {
		cerr << "deleteTuple: delete record error " << rc << endl;
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
	FileHandle *fileHandle;
	rc = openTable(tableName, fileHandle);
	if (rc != SUCC) {
		cerr << "updateTuple: open table " << tableName << " error " << rc << endl;
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
	rc = rbfm->updateRecord(*fileHandle, attrs, tuple, rid);
	if (rc != SUCC) {
		cerr << "updateTuple: update tuple " << rc << endl;
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
	FileHandle *fileHandle;
	rc = openTable(tableName, fileHandle);
	if (rc != SUCC) {
		cerr << "readTuple: open table " << tableName << " error " << rc << endl;
		return rc;
	}

	// get the current version attribute
	vector<Attribute> attrs;
	rc = getAttributes(tableName, attrs);
	if (rc != SUCC) {
		cerr << "readTuple: read the attribute " << rc << endl;
		return rc;
	}

	rc = rbfm->readRecord(*fileHandle, attrs, rid, tuple);
	if (rc != SUCC) {
		if (rc != RC_RECORD_DELETED)
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


    return SUCC;
}

RC RelationManager::readAttribute(const string &tableName,
		const RID &rid, const string &attributeName, void *data)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc;

	// open the file
	FileHandle *fileHandle;
	rc = openTable(tableName, fileHandle);
	if (rc != SUCC) {
		cerr << "RelationManager::readAttribute: open table " << tableName << " error " << rc << endl;
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
	rc = rbfm->readAttribute(*fileHandle, attrs, rid, attributeName, data);
	if (rc != SUCC) {
		cerr << "RelationManager::readAttribute: read attribute error " << rc << endl;
		return rc;
	}
    return SUCC;
}

RC RelationManager::reorganizePage(const string &tableName, const unsigned pageNumber)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc;

	// open the file
	FileHandle *fileHandle;
	rc = openTable(tableName, fileHandle);
	if (rc != SUCC) {
		cerr << "RelationManager::reorganizePage: open table " << tableName << " error " << rc << endl;
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
	rc = rbfm->reorganizePage(*fileHandle, attrs, pageNumber);
	if (rc != SUCC) {
		cerr << "RelationManager::reorganizePage: reorganize page erro " << rc << endl;
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

	// open file handle;
	FileHandle *fileHandle;
	rc = openTable(tableName, fileHandle);
	if (rc != SUCC) {
		cerr << "RelationManager::scan: open table " << tableName << " error " << rc << endl;
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

	rc = rbfm->scan(*fileHandle, recordDesriptor,
			conditionAttribute, compOp, value,
			attributeNames, rm_ScanIterator.rbfm_si);
	if (rc != SUCC) {
		cerr << "RelationManager::scan: scan initialization error " << rc << endl;
		return rc;
	}

	return SUCC;
}

// Extra credit
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
	VersionManager *vm = VersionManager::instance();
	RC rc;

	// open the file
	FileHandle *fileHandle;
	rc = openTable(tableName, fileHandle);
	if (rc != SUCC) {
		cerr << "RelationManager::dropAttribute: open table " << tableName << " error " << rc << endl;
		return rc;
	}

	// drop attribute
	rc = vm->dropAttribute(tableName, attributeName, *fileHandle);
	if (rc != SUCC) {
		cerr << "dropAttribute: drop attribute error " << rc << endl;
		return rc;
	}

	return SUCC;
}

// Extra credit
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
	VersionManager *vm = VersionManager::instance();
	RC rc;

	// open the file
	FileHandle *fileHandle;
	rc = openTable(tableName, fileHandle);
	if (rc != SUCC) {
		cerr << "RelationManager::addAttribute: open table " << tableName << " error " << rc << endl;
		return rc;
	}

	// drop attribute
	rc = vm->addAttribute(tableName, attr, *fileHandle);
	if (rc != SUCC) {
		cerr << "addAttribute: drop attribute error " << rc << endl;
		return rc;
	}

	return SUCC;
}

// Extra credit
RC RelationManager::reorganizeTable(const string &tableName)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc;

	FileHandle *fileHandle;
	rc = openTable(tableName, fileHandle);
	if (rc != SUCC) {
		cerr << "RelationManager::reorganizeTable: open table " << tableName << " error " << rc << endl;
		return rc;
	}

	vector<Attribute> recordDescriptor; // no use in our implementation
	rc = rbfm->reorganizeFile(*fileHandle, recordDescriptor);
	if (rc != SUCC) {
		cerr << "reorganizeTable: reorganize file error " << rc << endl;
		return rc;
	}

    return SUCC;
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

	for (int i = 1; i < int(numFields); ++i) {
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



