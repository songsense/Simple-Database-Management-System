
#include <fstream>
#include <iostream>
#include <cassert>

#include "../rbf/pfm.h"
#include "../rbf/rbfm.h"
#include "rm.h"

const int success = 0;
using namespace std;

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

void testRM_1(){
	RelationManager *rm = RelationManager::instance();
	string tableName = "test.db";

	// create table
	vector<Attribute> attrs;
	createRecordDescriptor(atts);
	RC rc = rm->createTable(tableName, attrs);
	assert(rc == success);


	rc = rm->deleteTable(tableName);
	assert(rc == success);

	cout << "test relation manager finish!" << endl;
}

int main() 
{
  cout << "test..." << endl;

  rmTest();
  // other tests go here
  // test the newly added rbfm features
  testRBFM_2();
  testRBFM_1();

  testRM_1();
  cout << "OK" << endl;
}

