
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
}

RelationManager::~RelationManager()
{
}



RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	VersionManager *verManager = VersionManager::instance();
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
	rc = verManager->formatFirst2Page(tableName, attrs, fileHandle);
	if (rc != SUCC)
		return rc;

	// close the file
	rc = rbfm->closeFile(fileHandle);
	if (rc != SUCC)
		return rc;

	// TODO save the retrieved record descriptor
	// currentRecordDescriptor = attrs;

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
	return -1;
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
	// TODO
	return -1;
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

	// TODO insert the record
	//rc = rbfm->insertRecordWithVersion(fileHandle,
	//		currentRecordDescriptor, data, rid, currentVersionNumber);
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







