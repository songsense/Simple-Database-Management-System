
#include <fstream>
#include <iostream>
#include <cassert>

#include "../rbf/pfm.h"
#include "../rbf/rbfm.h"
#include "rm.h"
#include "test_util.h"

using namespace std;

char tuple[30][PAGE_SIZE];
char returnedTuple[30][PAGE_SIZE];
int tupleSize[30];
RID rid[30];
void printTuples(const string& tableName, RelationManager* rm, const vector<int> &index) {
	// begin raed tuple
	char data[PAGE_SIZE];
	int strLen = 0;
	char str[PAGE_SIZE];
	// get attribute
	vector<Attribute> attrs;
	rm->getAttributes(tableName, attrs);
	for (int i = 0; i < index.size(); ++i) {
		for (int j = 0; j < attrs.size(); ++j) {
			rm->readAttribute(tableName, rid[index[i]], attrs[j].name, data);
			cout << attrs[j].name << "\t";
			switch (attrs[j].type) {
			case TypeInt:
				cout << *((int*) (data)) << "\t";
				break;
			case TypeReal:
				cout << *((float*) (data)) << "\t";
				break;
			case TypeVarChar:
				strLen = *((int*) (data));
				memcpy(str, data + sizeof(int), strLen + 1);
				str[strLen] = '\0';
				cout << str << "\t";
				break;
			}
		}
		cout << "\n-------------------------" << endl;
	}
}
void rmTest()
{
  // RM *rm = RM::instance();

  // write your own testing cases here
}
// Function to prepare the data in the correct form to be inserted/read
void prepareRecord(const int nameLength, const string &name, const int age, const float height, const int salary, void *buffer, int *recordSize)
{
    int offset = 0;

    memcpy((char *)buffer + offset, &nameLength, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)buffer + offset, name.c_str(), nameLength);
    offset += nameLength;

    memcpy((char *)buffer + offset, &age, sizeof(int));
    offset += sizeof(int);

    memcpy((char *)buffer + offset, &height, sizeof(float));
    offset += sizeof(float);

    memcpy((char *)buffer + offset, &salary, sizeof(int));
    offset += sizeof(int);

    *recordSize = offset;
}
void prepareTuple_AfterDropSalary(const int nameLength,
		const string &name, const int age, const float height,
		const int aliasLength, const string &alias, const int level, const float access,
		void *buffer, int *recordSize) {
	int offset = 0;

    memcpy((char *)buffer + offset, &nameLength, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)buffer + offset, name.c_str(), nameLength);
    offset += nameLength;

    memcpy((char *)buffer + offset, &age, sizeof(int));
    offset += sizeof(int);

    memcpy((char *)buffer + offset, &height, sizeof(float));
    offset += sizeof(float);

    memcpy((char *)buffer + offset, &aliasLength, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)buffer + offset, alias.c_str(), aliasLength);
    offset += aliasLength;

    memcpy((char *)buffer + offset, &level, sizeof(int));
    offset += sizeof(int);

    memcpy((char *)buffer + offset, &access, sizeof(float));
    offset += sizeof(float);

    *recordSize = offset;
}
void prepareTuple_AfterDropSalaryAlias(const int nameLength,
		const string &name, const int age, const float height,
		const int level, const float access,
		void *buffer, int *recordSize) {
	int offset = 0;

    memcpy((char *)buffer + offset, &nameLength, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)buffer + offset, name.c_str(), nameLength);
    offset += nameLength;

    memcpy((char *)buffer + offset, &age, sizeof(int));
    offset += sizeof(int);

    memcpy((char *)buffer + offset, &height, sizeof(float));
    offset += sizeof(float);

    memcpy((char *)buffer + offset, &level, sizeof(int));
    offset += sizeof(int);

    memcpy((char *)buffer + offset, &access, sizeof(float));
    offset += sizeof(float);

    *recordSize = offset;
}
void createRecordDescriptor(vector<Attribute> &recordDescriptor) {

    Attribute attr;
    attr.name = "EmpName";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)30;
    recordDescriptor.push_back(attr);

    attr.name = "Age";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);

    attr.name = "Height";
    attr.type = TypeReal;
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);

    attr.name = "Salary";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);

}
void testRBFM_1() {
	/*
	 * create database
	 * insert an entry
	 * insert multiple entry
	 * delete one of them
	 * delete all of them
	 * close database
	 * destroy database
	 */
	RC rc;
	string fileName = "test.db";
	remove(fileName.c_str());

	// create database
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	rc = rbfm->createFile(fileName);
	assert(rc == success);

	// open database
	rc = rbfm->openFile(fileName, fileHandle);
	assert(rc == success);

	// prepare the attributers
	RID rid[100];
	int recordSize = 0;
	void *record = malloc(100);
	void *returnedData = malloc(100);

	vector<Attribute> recordDescriptor;
	createRecordDescriptor(recordDescriptor);

	// prepare an entry
	prepareRecord(6, "Peters", 24, 170.1, 5000, record, &recordSize);
    cout << "Print Data:" << endl;
    rc = rbfm->printRecord(recordDescriptor, record);
    assert(rc == success);

    // insert data
    cout << "Insert Data:" << endl;
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid[0]);
    assert(rc == success);
    // read data
    cout << "Read Data:" << endl;
    // Given the rid, read the record from file
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid[0], returnedData);
    assert(rc == success);

    cout << "Returned Data:" << endl;
    rbfm->printRecord(recordDescriptor, returnedData);

    // Compare whether the two memory blocks are the same
    if(memcmp(record, returnedData, recordSize) != 0)
    {
       cout << "RBFM Test Case 1 Failed!" << endl << endl;
        free(record);
        free(returnedData);
        return;
    }

    // insert more data
    // 1.
    cout << "insert 4 more records" << endl;
    prepareRecord(3, "Tom", 25, 171.1, 6000, record, &recordSize);
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid[1]);
    assert(rc == success);
    // 2.
    prepareRecord(4, "John", 26, 172.1, 7000, record, &recordSize);
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid[2]);
    assert(rc == success);
    // 3.
    prepareRecord(6, "Albert", 28, 181.1, 8000, record, &recordSize);
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid[3]);
    assert(rc == success);
    // 4.
    prepareRecord(6, "Jimmy", 27, 151.1, 9000, record, &recordSize);
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid[4]);
    assert(rc == success);

    // delete data 2
    cout << "delete the 3rd record" << endl;
    rc = rbfm->deleteRecord(fileHandle, recordDescriptor, rid[2]);
    // try read 3rd record
    cout << "try to read the 3rd record" << endl;
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid[2], returnedData);
    assert(rc == RC_RECORD_DELETED);
    // read 4th record
    cout << "read the 4th record" << endl;
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid[3], returnedData);
    rc = rbfm->printRecord(recordDescriptor, returnedData);
    // read 5th record
    cout << "read the 5th record" << endl;
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid[4], returnedData);
    rc = rbfm->printRecord(recordDescriptor, returnedData);
    // Compare whether the two memory blocks are the same
    if(memcmp(record, returnedData, recordSize) != 0)
    {
       cout << "RBFM Test Case 1 Failed!" << endl << endl;
        free(record);
        free(returnedData);
        return;
    }


    /*
     * Now update the 2nd, 4th and 5th data
     */
    cout << "Print the RID of 2nd, 4th and 5th data" << endl;
    cout << rid[0].pageNum << "\t" << rid[1].pageNum << "\t"
    		<< rid[3].pageNum << "\t" << rid[4].pageNum << endl;
    cout << rid[0].slotNum << "\t" << rid[1].slotNum << "\t"
    		<< rid[3].slotNum << "\t" << rid[4].slotNum << endl;
    cout << "Now update the 2nd, 4th and 5th data" << endl;

    cout << "2nd" << endl;
    prepareRecord(10, "TomTomTomT", 25, 171.1, 6000, record, &recordSize);
    rc = rbfm->updateRecord(fileHandle, recordDescriptor, record, rid[1]);
    assert(rc == success);
    cout << "4th" << endl;
    prepareRecord(10, "Albertsons", 28, 181.1, 8000, record, &recordSize);
    rc = rbfm->updateRecord(fileHandle, recordDescriptor, record, rid[3]);
    assert(rc == success);
    cout << "5th" << endl;
    prepareRecord(13, "JimAlbertsons", 28, 155.1, 10000, record, &recordSize);
    rc = rbfm->updateRecord(fileHandle, recordDescriptor, record, rid[4]);
    assert(rc == success);

    // print the updated data
    cout << "After update, print the RID of 2nd, 4th and 5th data" << endl;
    cout << rid[0].pageNum << "\t" << rid[1].pageNum << "\t"
    		<< rid[3].pageNum << "\t" << rid[4].pageNum << endl;
    cout << rid[0].slotNum << "\t" << rid[1].slotNum << "\t"
    		<< rid[3].slotNum << "\t" << rid[4].slotNum << endl;
    cout << "Print the updated data" << endl;
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid[0], returnedData);
    assert(rc == SUCC);
    rc = rbfm->printRecord(recordDescriptor, returnedData);
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid[1], returnedData);
    assert(rc == SUCC);
    rc = rbfm->printRecord(recordDescriptor, returnedData);
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid[3], returnedData);
    assert(rc == SUCC);
    rc = rbfm->printRecord(recordDescriptor, returnedData);
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid[4], returnedData);
    assert(rc == SUCC);
    rc = rbfm->printRecord(recordDescriptor, returnedData);

    /*
     * 		Test read attribute of several entry
     */
    cout << "begin read attribute of entries" << endl;
    char empName[100];
    int age, salary;
    float height;
    string attName[4];
    attName[0] = "EmpName", attName[1] = "Age", attName[2] = "Height", attName[3] = "Salary";
    for (int i = 0; i < 5; ++i) {
    	cout << "read record's EmpName, Age, Height and Salary" << endl;
    	rc = rbfm->readAttribute(fileHandle, recordDescriptor, rid[i], attName[0], returnedData);
    	assert(rc == SUCC || rc == RC_RECORD_DELETED);
    	if (rc == RC_RECORD_DELETED)
    		cout << rc << endl;
    	int len = *((int *)returnedData);
    	memcpy(empName, (char *)returnedData+sizeof(int), len);
    	empName[len] = '\0';
    	rc = rbfm->readAttribute(fileHandle, recordDescriptor, rid[i], attName[1], returnedData);
    	assert(rc == SUCC || rc == RC_RECORD_DELETED);
    	memcpy(&age, returnedData, sizeof(int));
    	rc = rbfm->readAttribute(fileHandle, recordDescriptor, rid[i], attName[2], returnedData);
    	assert(rc == SUCC || rc == RC_RECORD_DELETED);
    	memcpy(&height, returnedData, sizeof(float));
    	rc = rbfm->readAttribute(fileHandle, recordDescriptor, rid[i], attName[3], returnedData);
    	assert(rc == SUCC || rc == RC_RECORD_DELETED);
    	memcpy(&salary, returnedData, sizeof(int));
    	cout << "EmpName:\t" << empName << "\tAge:\t" << age << "\theight:\t" << height <<
    			"\tsalary:\t" << salary << endl;
    }



    /*
     * test delete a updated record
     */
    cout << "Delete a updated record" << endl;
    rc = rbfm->deleteRecord(fileHandle, recordDescriptor, rid[1]);
    assert(rc == success);
    cout << "Try read the deleted record" << endl;
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid[1], returnedData);
    assert(rc == RC_RECORD_DELETED);
    /*
     * test Organized
     */

    // test reorganize data
    cout << "test reorganized page" << endl;
    char page[PAGE_SIZE];
    rc = fileHandle.readPage(rid[3].pageNum, page);
    unsigned freeSpace = rbfm->getFreeSpaceSize(page);
    cout << "Before reorganizing, the free space size is " << freeSpace << endl;
    rc = rbfm->reorganizePage(fileHandle, recordDescriptor, rid[3].pageNum);
    assert(rc == success);
    rc = fileHandle.readPage(rid[3].pageNum, page);
    freeSpace = rbfm->getFreeSpaceSize(page);
    cout << "After reorganizing, the free space size is " << freeSpace << endl;


	// close database
    rc = rbfm->closeFile(fileHandle);
    assert(rc == success);
	// destroy database
    rc = rbfm->destroyFile(fileName);
    assert(rc == success);


    free(record);
    free(returnedData);
    cout << "RBFM Test Case 1 Passed!" << endl << endl;
}
/*
 * 		Test Version Control
 */
void testRBFM_2() {
	RC rc;
	string tableName = "test.db";
	remove(tableName.c_str());

	// create database
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	VersionManager *vm = VersionManager::instance();

	FileHandle fileHandle;
	rc = rbfm->createFile(tableName);
	assert(rc == success);

	// open database
	rc = rbfm->openFile(tableName, fileHandle);
	assert(rc == success);

	// prepare attributes
	vector<Attribute> attrs;
	Attribute attr;
	attr.length = 40;
	attr.name = "EmpName";
	attr.type = TypeVarChar;
	attrs.push_back(attr);

    cout << "VM Test Case 1 Begins!" << endl << endl;

	// initialize version manager
    cout << "initialize version" << endl;
	rc = vm->formatFirst2Page(tableName, attrs, fileHandle);
	assert(rc == success);

	// read versions
	cout << "read version" << endl;
	rc = vm->initTableVersionInfo(tableName, fileHandle);
	assert(rc == success);

	// print versions
	vm->printAttributes(tableName);

	// add one verion with adding one more attribute
	attr.length = 4;
	attr.name = "Age";
	attr.type = TypeInt;
	cout << "add one more attribute" << endl;
	rc = vm->addAttribute(tableName, attr, fileHandle);
	assert(rc == success);
	// print version
	vm->printAttributes(tableName);

	// add one version with deleting one attribute
	string attrName = "EmpName";
	cout << "delete one attribute " << attrName << endl;
	rc = vm->dropAttribute(tableName, attrName, fileHandle);
	assert(rc == success);
	// print version
	vm->printAttributes(tableName);

	// add one verion with adding one more attribute
	attr.length = 4;
	attr.name = "Sex";
	attr.type = TypeInt;
	cout << "add one more attribute" << endl;
	rc = vm->addAttribute(tableName, attr, fileHandle);
	assert(rc == success);
	// print version
	vm->printAttributes(tableName);

	// add one version with deleting one attribute
	attrName = "Age";
	cout << "delete one attribute " << attrName << endl;
	rc = vm->dropAttribute(tableName, attrName, fileHandle);
	assert(rc == success);
	// print version
	vm->printAttributes(tableName);

	// close database
    rc = rbfm->closeFile(fileHandle);
    assert(rc == success);

	// reopen database
    cout << "reopen the database" << endl;
	rc = rbfm->openFile(tableName, fileHandle);
	assert(rc == success);
	// read versions
	cout << "read version" << endl;
	rc = vm->initTableVersionInfo(tableName, fileHandle);
	assert(rc == success);
	// print version
	vm->printAttributes(tableName);

	// close database
    rc = rbfm->closeFile(fileHandle);
    assert(rc == success);
	// destroy database
    rc = rbfm->destroyFile(tableName);
    assert(rc == success);

    cout << "VM Test Case 1 Passed!" << endl << endl;
}

void testRM_createTable(const string &tableName, vector<Attribute> &attrs) {
	//bookmark
	cout << "test createTable starts!" << endl;
	RelationManager *rm = RelationManager::instance();
	// create table
	cout << "create table " << tableName << endl;
	RC rc = rm->createTable(tableName, attrs);
	assert(rc == success);
	cout << "test createTable finishes!" << endl;
	cout << "===============================================" << endl;
}

void testRM_getAttribute(const string &tableName){
	//bookmark
	cout << "test getAttribute starts!" << endl;
	RelationManager *rm = RelationManager::instance();
	VersionManager *vm = VersionManager::instance();
	RC rc;

	// print the attribute in version manager
	vm->printAttributes(tableName);

	// read in attribute
	vector<Attribute> attrs;
	rc = rm->getAttributes(tableName, attrs);
    for(unsigned i = 0; i < attrs.size(); i++)
    {
        cout << "Attribute Name: " << attrs[i].name << endl;
        cout << "Attribute Type: " << (AttrType)attrs[i].type << endl;
        cout << "Attribute Length: " << attrs[i].length << endl << endl;
    }

	cout << "test getAttribute finishes!" << endl;
	cout << "===============================================" << endl;
}

void testRM_deleteTable(const string &tableName) {
	//bookmark
	RelationManager *rm = RelationManager::instance();
	cout << "test deleteTable starts!" << endl;
	cout << "delete table " << tableName << endl;
	RC rc = rm->deleteTable(tableName);
	assert(rc == success);
	cout << "test deleteTable finishes!" << endl;
	cout << "===============================================" << endl;
}

void testRM_insertTuple_readTuple(const string &tableName) {
	//bookmark
	RelationManager *rm = RelationManager::instance();
	RC rc;
	cout << "test insertTuple starts!" << endl;

	// get attribute
	vector<Attribute> attrs;
	rm->getAttributes(tableName, attrs);

	// prepare tuple
	prepareTuple(3, "Tom", 24, 179.1, 6500, tuple[0], &(tupleSize[0]));

	// insert tuple
	cout << "insert tuple" << endl;
	rc = rm->insertTuple(tableName, tuple[0], rid[0]);
	assert(rc == SUCC);

	// read tuple
	cout << "read tuple" << endl;
	rc = rm->readTuple(tableName, rid[0], returnedTuple[0]);
	assert(rc == success);

	// print tuple
	printTuple(returnedTuple[0], tupleSize[0]);

	// compare tuple
	assert(memcmp(tuple, returnedTuple[0], tupleSize[0]) == 0);

	cout << "test insertTuple finishes!" << endl;
	cout << "===============================================" << endl;
}

void testRM_InsertMultipleTuples(const string &tableName) {
	//bookmark
	RelationManager *rm = RelationManager::instance();
	RC rc;
	cout << "test InsertMultipleTuples starts!" << endl;
	// get attribute
	vector<Attribute> attrs;
	rm->getAttributes(tableName, attrs);

	// prepare tuple
	prepareTuple(5, "Jimmy", 25, 169.1, 5500, tuple[1], &(tupleSize[1]));
	prepareTuple(5, "Teddy", 26, 189.1, 9500, tuple[2], &(tupleSize[2]));
	prepareTuple(6, "Albert", 27, 176.1, 15500, tuple[3], &(tupleSize[3]));
	prepareTuple(4, "Lucy", 28, 188.1, 3200, tuple[4], &(tupleSize[4]));

	// insert tuple
	cout << "insert tuple" << endl;
	rc = rm->insertTuple(tableName, tuple[1], rid[1]);
	assert(rc == SUCC);
	rc = rm->insertTuple(tableName, tuple[2], rid[2]);
	assert(rc == SUCC);
	rc = rm->insertTuple(tableName, tuple[3], rid[3]);
	assert(rc == SUCC);
	rc = rm->insertTuple(tableName, tuple[4], rid[4]);
	assert(rc == SUCC);

	// read tuple
	cout << "read tuple" << endl;
	rc = rm->readTuple(tableName, rid[1], returnedTuple[1]);
	assert(rc == success);
	rc = rm->readTuple(tableName, rid[2], returnedTuple[2]);
	assert(rc == success);
	rc = rm->readTuple(tableName, rid[3], returnedTuple[3]);
	assert(rc == success);
	rc = rm->readTuple(tableName, rid[4], returnedTuple[4]);
	assert(rc == success);

	// compare tuple
	assert(memcmp(tuple[1], returnedTuple[1], tupleSize[1]) == 0);
	assert(memcmp(tuple[2], returnedTuple[2], tupleSize[2]) == 0);
	assert(memcmp(tuple[3], returnedTuple[3], tupleSize[3]) == 0);
	assert(memcmp(tuple[4], returnedTuple[4], tupleSize[4]) == 0);

	cout << "test InsertMultipleTuples finishes!" << endl;
	cout << "===============================================" << endl;
}

void testRM_DeleteUpdateInsertTuple(const string &tableName) {
	//bookmark
	RelationManager *rm = RelationManager::instance();
	RC rc;

	// delete 2nd tuple
	cout << "delete 2nd tuple" << endl;
	rc = rm->deleteTuple(tableName, rid[1]);
	assert(rc == success);
	// update 3rd tuple
	cout << "update 3rd tuple" << endl;
	prepareTuple(8, "Jr.Teddy", 27, 189.2, 9501, tuple[2], &(tupleSize[2]));
	rc = rm->updateTuple(tableName, tuple[2], rid[2]);
	assert(rc == success);
	// delete 4th tuple
	cout << "delete 4th tuple" << endl;
	rc = rm->deleteTuple(tableName, rid[3]);
	assert(rc == success);
	// delete 3rd tuple
	cout << "delete 3rd tuple" << endl;
	rc = rm->deleteTuple(tableName, rid[2]);
	assert(rc == success);
	// insert 6th tuple
	cout << "insert 6th tuple" << endl;
	prepareTuple(4, "John", 39, 178.5, 4300, tuple[5], &(tupleSize[5]));
	rc = rm->insertTuple(tableName, tuple[5], rid[5]);
	assert(rc == success);
	// update 5th tuple
	cout << "update 5th tuple" << endl;
	prepareTuple(7, "Sr.Lucy", 29, 188.5, 3231, tuple[4], &(tupleSize[4]));
	rc = rm->updateTuple(tableName, tuple[4], rid[4]);
	assert(rc == success);
	// read 1, 5, 6 tuple
	cout << "read 1, 5, 6 tuple" << endl;
	rc = rm->readTuple(tableName, rid[0], returnedTuple[0]);
	assert(rc == success);
	rc = rm->readTuple(tableName, rid[4], returnedTuple[4]);
	assert(rc == success);
	rc = rm->readTuple(tableName, rid[5], returnedTuple[5]);
	assert(rc == success);
	// test consistency
	assert(memcmp(tuple[0], returnedTuple[0], tupleSize[0]) == 0);
	assert(memcmp(tuple[4], returnedTuple[4], tupleSize[4]) == 0);
	assert(memcmp(tuple[5], returnedTuple[5], tupleSize[5]) == 0);
	// print 1, 5, 6 tuple
	printTuple(returnedTuple[0], tupleSize[0]);
	printTuple(returnedTuple[4], tupleSize[4]);
	printTuple(returnedTuple[5], tupleSize[5]);

	cout << "test DeleteUpdateInsertTuple starts!" << endl;
	cout << "test DeleteUpdateInsertTuple finishes!" << endl;
	cout << "===============================================" << endl;
}
void testRM_insert_readAttribute(const string &tableName) {
	//bookmark
	RelationManager *rm = RelationManager::instance();
	RC rc;
	cout << "test insert_readAttribute starts!" << endl;

	// get attribute
	vector<Attribute> attrs;
	rm->getAttributes(tableName, attrs);

	// prepare tuple
	prepareTuple(1, "a", 25, 169.1, 5500, tuple[6], &(tupleSize[6]));
	prepareTuple(2, "aa", 26, 189.1, 9500, tuple[7], &(tupleSize[7]));
	prepareTuple(1, "b", 27, 176.1, 15500, tuple[8], &(tupleSize[8]));
	prepareTuple(2, "bb", 28, 188.1, 3200, tuple[9], &(tupleSize[9]));

	// insert tuple
	cout << "insert tuple" << endl;
	rc = rm->insertTuple(tableName, tuple[6], rid[6]);
	assert(rc == SUCC);
	rc = rm->insertTuple(tableName, tuple[7], rid[7]);
	assert(rc == SUCC);
	rc = rm->insertTuple(tableName, tuple[8], rid[8]);
	assert(rc == SUCC);
	rc = rm->insertTuple(tableName, tuple[9], rid[9]);
	assert(rc == SUCC);

	// read tuple
	cout << "read tuple" << endl;
	rc = rm->readTuple(tableName, rid[6], returnedTuple[6]);
	assert(rc == success);
	rc = rm->readTuple(tableName, rid[7], returnedTuple[7]);
	assert(rc == success);
	rc = rm->readTuple(tableName, rid[8], returnedTuple[8]);
	assert(rc == success);
	rc = rm->readTuple(tableName, rid[9], returnedTuple[9]);
	assert(rc == success);

	// compare tuple
	assert(memcmp(tuple[6], returnedTuple[6], tupleSize[6]) == 0);
	assert(memcmp(tuple[7], returnedTuple[7], tupleSize[7]) == 0);
	assert(memcmp(tuple[8], returnedTuple[8], tupleSize[8]) == 0);
	assert(memcmp(tuple[9], returnedTuple[9], tupleSize[9]) == 0);

	// begin raed tuple
	vector<int> index;
	index.push_back(0);index.push_back(4);index.push_back(5);index.push_back(6);
	index.push_back(7);index.push_back(8);index.push_back(9);
	printTuples(tableName, rm, index);


	cout << "test insert_readAttribute finishes!" << endl;
	cout << "===============================================" << endl;
}

void testRM_addAttribute(const string &tableName) {
	//bookmark
	RelationManager *rm = RelationManager::instance();
	RC rc;
	cout << "test addAttribute starts!" << endl;

	cout << "add a varchar" << endl;
	Attribute attr;
	attr.type = TypeVarChar;
	attr.name = "Alias";
	attr.length = 20;
	rc = rm->addAttribute(tableName,attr);
	assert(rc == success);
	cout << "add an int" << endl;
	attr.type = TypeInt;
	attr.name = "Level";
	attr.length = sizeof(int);
	rc = rm->addAttribute(tableName,attr);
	assert(rc == success);
	cout << "add a real" << endl;
	attr.type = TypeReal;
	attr.name = "Assess";
	attr.length = sizeof(float);
	rc = rm->addAttribute(tableName,attr);
	assert(rc == success);


	// begin raed tuple
	vector<int> index;
	index.push_back(0);index.push_back(4);index.push_back(5);index.push_back(6);
	index.push_back(7);index.push_back(8);index.push_back(9);
	printTuples(tableName, rm, index);
	cout << "test addAttribute finishes!" << endl;
	cout << "===============================================" << endl;
}

void testRM_dropAttribute(const string &tableName) {
	//bookmark
	RelationManager *rm = RelationManager::instance();

	RC rc;
	cout << "test dropAttribute starts!" << endl;

	cout << "drop attribute" << endl;
	rc = rm->dropAttribute(tableName, "Salary");
	assert(rc == SUCC);
	prepareTuple_AfterDropSalary(5, "Mancy", 22, 169.6, 3, "Man", 3, 2.1, tuple[10], &(tupleSize[10]));
	prepareTuple_AfterDropSalary(4, "Mike", 29, 179.6, 2, "Mk", 4, 5.1, tuple[6], &(tupleSize[6]));
	cout << "add one entry" << endl;
	rc = rm->insertTuple(tableName, tuple[10], rid[10]);
	assert(rc == success);
	cout << "read the entry" << endl;
	rc = rm->readTuple(tableName, rid[10], returnedTuple[10]);
	assert(rc == success);
	cout << "compare the entries" << endl;
	assert(memcmp(tuple[10], returnedTuple[10], tupleSize[10]) == 0);
	cout << "update entry" << endl;
	rc = rm->updateTuple(tableName, tuple[6], rid[6]);
	assert(rc == success);
	cout << "read entry" << endl;
	rc = rm->readTuple(tableName, rid[6], returnedTuple[6]);
	assert(rc == success);
	cout << "compare the entries" << endl;
	cout << tupleSize[6] << endl;
	assert(memcmp(tuple[6], returnedTuple[6], tupleSize[6]) == 0);

	cout << "drop attribute" << endl;
	rc = rm->dropAttribute(tableName, "Alias");
	assert(rc == SUCC);
	rc = rm->deleteTuple(tableName, rid[4]);
	assert(rc == SUCC);

	vector<int> index;
	index.push_back(0);
	index.push_back(5);
	index.push_back(6);
	index.push_back(7);
	index.push_back(8);
	index.push_back(9);
	index.push_back(10);
	// begin raed tuple
	printTuples(tableName, rm, index);
	cout << "test dropAttribute finishes!" << endl;
	cout << "===============================================" << endl;
}

void testRM_scan(const string &tableName) {
	//bookmark
	RelationManager *rm = RelationManager::instance();
	RC rc;
	cout << "test scan starts!" << endl;

	RM_ScanIterator rmsi;
	int age = 26;
	vector<string>attrNames;
	attrNames.push_back("EmpName");
	attrNames.push_back("Height");
	attrNames.push_back("Assess");
	attrNames.push_back("Age");
	// initialize scan
	cout << "initialize scan" << endl;
	rc = rm->scan(tableName, "Age", LT_OP, &age, attrNames, rmsi);
	assert(rc == success);

	RID scanRID;
	char data[PAGE_SIZE];
	char *result = data;
	cout << "begin get next" << endl;
	while(rmsi.getNextTuple(scanRID, data) != RM_EOF) {
		result = data;
		int strLen(0);
		char empName[PAGE_SIZE];
		strLen = *((int *)result);
		result += sizeof(int);
		memcpy(empName, result, strLen+1);
		result += strLen;
		empName[strLen] = '\0';
		cerr << "EmpName: " << empName << "\t";

		float height(0.0);
		memcpy(&height, result, sizeof(float));
		result += sizeof(float);
		cerr << "Height: " << height << "\t";

		float access(0.0);
		memcpy(&access, result, sizeof(float));
		result += sizeof(float);
		cerr << "Access: " << access << "\t";

		int age_(0);
		memcpy(&age_, result, sizeof(int));
		result += sizeof(int);
		cerr << "Age: " << age_ << "\t" << endl;
	}

	cout << "test scan finishes!" << endl;
	cout << "===============================================" << endl;
}

void testRM_(const string &tableName) {
	//bookmark
	RelationManager *rm = RelationManager::instance();
	RC rc;
	cout << "test  starts!" << endl;
	cout << "test  finishes!" << endl;
	cout << "===============================================" << endl;
}
int main() 
{
  cout << "test..." << endl;

  rmTest();
  //testRBFM_2();
  //testRBFM_1();
  // other tests go here
  // test the newly added rbfm features
  vector<Attribute> attrs;
  createRecordDescriptor(attrs);
  const string tableName = "test";
  remove(tableName.c_str());

  testRM_createTable(tableName, attrs);
  testRM_getAttribute(tableName);
  testRM_insertTuple_readTuple(tableName);
  testRM_InsertMultipleTuples(tableName);
  testRM_DeleteUpdateInsertTuple(tableName); 	// only 1,5,6 kept
  testRM_insert_readAttribute(tableName); 	// insert 7,8,9,10
  testRM_addAttribute(tableName);
  testRM_dropAttribute(tableName);
  testRM_scan(tableName);
  testRM_deleteTable(tableName);
  memProfile();
  cout << "O.K." << endl;
}

