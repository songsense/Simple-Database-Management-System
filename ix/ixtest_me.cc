#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <cassert>
#include "ix.h"
#include "ixtest_util.h"

using namespace std;
IndexManager *ix = IndexManager::instance();
unsigned prepareKey(void *key, const string &str) {
	int len(str.size());
	memcpy(key, &len, sizeof(int));
	memcpy((char *)key+sizeof(int), str.c_str(), len);
	return len+sizeof(int);
}

string generateRandomString(int lower, int extLen) {
	string str;
	int strLen = rand() % extLen + lower;
	str.clear();
	for (int j = 0; j < strLen; ++j) {
		str.push_back('a' + (rand()%26));
	}
	return str;
}
void basic_test() {
	RC rc;
	cout << "******************begin basic test" << endl;
	Attribute attr;
	attr.length = 35;
	attr.name = "EmpName";
	attr.type = TypeVarChar;
	char page[PAGE_SIZE];
	ix->setPageEmpty(page);
	ix->setPageLeaf(page, CONST_NOT_LEAF);
	// prepare data
	char key[PAGE_SIZE];
	unsigned keyLen = prepareKey(key, "Peter");
	SlotNum slotNum(1);
	RID	rid;
	rid.pageNum = 0, rid.slotNum = 1;
	rc = ix->insertEntryAtPos(page, slotNum,
			attr,key, keyLen,
			rid, false,
			0, 0);
	assert(rc == SUCC);
	int indexTimes(19);
	for (int i = 0; i < indexTimes; ++i) {
		slotNum = 4;
        int strLen = (rand()%5) + 5;
        string empName;
        int num = i;
        do {
        	empName.push_back('0'+num%10);
        	num /= 10;
        } while(num > 0);
        empName.push_back(':');
        for (int i = 0; i < strLen; ++i) {
        	empName.push_back(rand()%26+'a');
        }
        keyLen = prepareKey(key, empName);
        rid.slotNum = i;
        rc = ix->insertEntryAtPos(page, slotNum,
        		attr, key, keyLen,
        		rid, true,
        		0,i+1);
        assert(rc == SUCC);
	}

	ix->printPage(page, attr);

	cout << "deleting 1, 3, 18" << endl;
	rc = ix->deleteEntryAtPos(page, 1);
	assert(rc == SUCC);
	rc = ix->deleteEntryAtPos(page, 3);
	assert(rc == SUCC);
	rc = ix->deleteEntryAtPos(page, 18);
	assert(rc == SUCC);
	rc = ix->deleteEntryAtPos(page, 18);
	assert(rc != SUCC);
	ix->printPage(page, attr);

	cout << "deleting the rest" << endl;
	for (int i = 17; i >=1; --i) {
		int delIndex = rand()%i+1;
		rc = ix->deleteEntryAtPos(page, delIndex);
		assert(rc == SUCC);
		ix->printPage(page, attr);
	}

	cout << "******************end basic test" << endl;
}

void basic_test_search_int() {
	cout << "******************begin search test" << endl;
	cout << "insert entry" << endl;
	Attribute attr;
	attr.length = sizeof(int);
	attr.name = "age";
	attr.type = TypeInt;
	char page[PAGE_SIZE];
	ix->setPageEmpty(page);
	ix->setPageLeaf(page, CONST_IS_LEAF);
	// prepare data
	char key[sizeof(int)];
	SlotNum slotNum(1);
	RID	rid;
	rid.pageNum = 0, rid.slotNum = 1;
	int indexNum(100);
	int ageVal(20);
	for (int i = 0; i < indexNum; ++i) {
		if (i < 50)
			ageVal = 20 + rand()%100;
		else
			ageVal = 20 + rand()%60;
		memcpy(key, &ageVal, sizeof(int));
		ix->binarySearchEntry(page, attr, key, slotNum);

		ix->insertEntryAtPos(page, slotNum,
				attr,key, sizeof(int),
				rid, false,
				0, 0);
	}
	ix->printPage(page, attr);

	cout << "******************end search test" << endl;
}
void basic_test_search_float() {
	cout << "******************begin search test" << endl;
	cout << "insert entry" << endl;
	Attribute attr;
	attr.length = sizeof(float);
	attr.name = "salary";
	attr.type = TypeReal;
	char page[PAGE_SIZE];
	ix->setPageEmpty(page);
	ix->setPageLeaf(page, CONST_NOT_LEAF);
	// prepare data
	char key[sizeof(float)];
	SlotNum slotNum(1);
	RID	rid;
	rid.pageNum = 0, rid.slotNum = 1;
	int indexNum(100);
	float salaryVal(20.0);
	for (int i = 0; i < indexNum; ++i) {
		salaryVal = 170.0 + (float(rand()))/RAND_MAX*10.0;
		memcpy(key, &salaryVal, sizeof(float));
		ix->binarySearchEntry(page, attr, key, slotNum);

		ix->insertEntryAtPos(page, slotNum,
				attr,key, sizeof(float),
				rid, false,
				0, 0);
	}
	ix->printPage(page, attr);

	cout << "******************end search test" << endl;
}
void basic_test_search_string() {
	cout << "******************begin search test" << endl;
	cout << "insert entry" << endl;
	Attribute attr;
	attr.length = 40;
	attr.name = "empName";
	attr.type = TypeVarChar;
	char page[PAGE_SIZE];
	ix->setPageEmpty(page);
	ix->setPageLeaf(page, CONST_IS_LEAF);
	// prepare data
	char key[PAGE_SIZE];
	SlotNum slotNum(1);
	RID	rid;
	rid.pageNum = 0, rid.slotNum = 1;
	int indexNum(100);
	for (int i = 0; i < indexNum; ++i) {
		string str;
		int len = 5+(rand()%5);
		for (int j = 0; j < len; ++j) {
			str.push_back('a' + rand()%26);
		}
		len = str.size();
		memcpy(key, &len, sizeof(int));
		memcpy(key+sizeof(int), str.c_str(), len);
		ix->binarySearchEntry(page, attr, key, slotNum);
		ix->insertEntryAtPos(page, slotNum,
				attr,key, sizeof(int)+len,
				rid, false,
				0, 0);
	}
	ix->printPage(page, attr);

	cout << "******************end search test" << endl;
}

void basic_test_space_manager() {
	cout << "******************begin space manager test" << endl;
	SpaceManager *sm = SpaceManager::instance();
	IndexManager *ix = IndexManager::instance();
	string indexFileName = "test";
	FileHandle fileHandle;
	Attribute attr;
	RC rc;
	remove(indexFileName.c_str());

	rc = ix->createFile(indexFileName);
	assert(rc == success);
	rc = sm->initIndexFile(indexFileName);
	assert(rc == success);
	rc = ix->openFile(indexFileName, fileHandle);
	assert(rc == success);

	// insert dup record
	RID dupHeadRID, dataRID, dupAssignedRID;
	int dupNumber = 210;

	dupHeadRID.pageNum = DUP_PAGENUM_END;
	dupHeadRID.slotNum = 1;
	dataRID.pageNum = 1;
	dataRID.slotNum = 2;
	rc = sm->insertDupRecord(fileHandle,
			dupHeadRID, dataRID, dupAssignedRID);
	assert(rc == success);
	dupHeadRID.pageNum = dupAssignedRID.pageNum;
	dupHeadRID.slotNum = dupAssignedRID.slotNum;
	for (int i = 1; i < dupNumber; ++i) {
		dataRID.pageNum += 1;
		dataRID.slotNum += 2;
		rc = sm->insertDupRecord(fileHandle,
				dupHeadRID, dataRID, dupAssignedRID);
		assert(rc == success);
		dupHeadRID.pageNum = dupAssignedRID.pageNum;
		dupHeadRID.slotNum = dupAssignedRID.slotNum;
	}
	// delete half of them
	cout << "delete half of them" << endl;
	dataRID.pageNum = 1;
	dataRID.slotNum = 2;
	for (int i = 0; i < dupNumber; i+=2) {
		rc = sm->deleteDupRecord(fileHandle,
				dupHeadRID, dataRID);
		assert(rc == success);
		dataRID.pageNum += 2;
		dataRID.slotNum += 4;
	}

	cout << "delete quarter of them" << endl;
	dataRID.pageNum = 2;
	dataRID.slotNum = 4;
	for (int i = 0; i < dupNumber; i+=4) {
		rc = sm->deleteDupRecord(fileHandle,
				dupHeadRID, dataRID);
		assert(rc == success);
		dataRID.pageNum += 4;
		dataRID.slotNum += 8;
	}

	cout << "delete the rest of them" << endl;
	dataRID.pageNum = 1;
	dataRID.slotNum = 2;
	for (int i = 0; i < dupNumber; ++i) {
		rc = sm->deleteDupRecord(fileHandle,
				dupHeadRID, dataRID);
		assert(rc == success);
		dataRID.pageNum += 1;
		dataRID.slotNum += 2;
	}

	char page[PAGE_SIZE];
	PageNum totalPageNum = fileHandle.getNumberOfPages();
	for (PageNum pn = 0; pn < totalPageNum; ++pn) {
		rc = fileHandle.readPage(pn, page);
		assert(rc == success);
		ix->printDupPage(page);
	}

	cout << "==========================================" << endl;
	cout << "===================" <<
			dupHeadRID.pageNum << "\t" <<
			dupHeadRID.slotNum <<
			"=================" << endl;
	cout << "==========================================" << endl;


	// test empty page
	cout << "test empty page module" << endl;
	// by now we should have at least three empty pages
	PageNum emptyPageNum;
	for (int i = 0; i < 4; ++i) {
		rc = sm->getEmptyPage(fileHandle, emptyPageNum);
		assert(rc == success);
		cout << emptyPageNum << "\t";
	}
	cout << endl;

	rc = ix->closeFile(fileHandle);
	assert(rc == success);


	cout << "******************end space manager test" << endl;
}

void basic_test_space_manager_2() {
	cout << "******************begin space manager test 2" << endl;
	SpaceManager *sm = SpaceManager::instance();
	IndexManager *ix = IndexManager::instance();
	string indexFileName = "test";
	FileHandle fileHandle;
	Attribute attr;
	RC rc;

	rc = sm->initIndexFile(indexFileName);
	assert(rc == success);
	rc = ix->openFile(indexFileName, fileHandle);
	assert(rc == success);

	// print
	char page[PAGE_SIZE];
	PageNum totalPageNum = fileHandle.getNumberOfPages();
	totalPageNum = fileHandle.getNumberOfPages();
	for (PageNum pn = 0; pn < totalPageNum; ++pn) {
		rc = fileHandle.readPage(pn, page);
		assert(rc == success);
		ix->printDupPage(page);
	}

	// empty
	PageNum emptyPageNum;
	for (int i = 0; i < 4; ++i) {
		rc = sm->getEmptyPage(fileHandle, emptyPageNum);
		assert(rc == success);
		cout << emptyPageNum << "\t";
	}
	cout << endl;

	rc = ix->closeFile(fileHandle);
	assert(rc == success);

	cout << "******************end space manager test 2" << endl;
}

void basic_test_split_int() {
	cout << "******************begin split test int" << endl;

	RC rc;
//	SpaceManager *sm = SpaceManager::instance();
	IndexManager *ix = IndexManager::instance();
	string indexFileName = "test";
	remove(indexFileName.c_str());
	FileHandle fileHandle;
	Attribute attr;
	attr.length = 4;
	attr.name = "age";
	attr.type = TypeInt;
	// prepare data
	int key = 20;
	// test data size
	int numTuple = 10;

	// test leaf page
	char leafPage[PAGE_SIZE];
	ix->setPageEmpty(leafPage);
	ix->setPageLeaf(leafPage, CONST_IS_LEAF);
	RID rid;
	for (int i = 0; i < numTuple; ++i) {
		key = 20 + i*2;
		rid.pageNum = i;
		rid.slotNum = i+1;
		rc = ix->insertEntryAtPos(leafPage, i+1, attr,
				&key, sizeof(int),
				rid, true, 0, 0);
		assert(rc == success);
	}

	cout << "now split the leaf page into two" << endl;
	char pageLeft[PAGE_SIZE];
	char pageRight[PAGE_SIZE];
	int keyCopiedUp;
	key = 27;
	rid.pageNum = 123, rid.slotNum = 123;
	rc = ix->splitPageLeaf(leafPage, pageLeft, pageRight, attr,
			&key, rid, &keyCopiedUp);
	assert(rc == success);
	cout << "key copied up is " << keyCopiedUp << endl;

	ix->printPage(pageLeft, attr);
	ix->printPage(pageRight, attr);

	char nonLeafPage[PAGE_SIZE];
	ix->setPageEmpty(nonLeafPage);
	ix->setPageLeaf(nonLeafPage, CONST_NOT_LEAF);
	PageNum prevPageNum(0), nextPageNum(1);
	for (int i = 0; i < numTuple; ++i) {
		key = 20 + i*2;
		nextPageNum = i+1;
		rc = ix->insertEntryAtPos(nonLeafPage, i+1, attr,
				&key, sizeof(int),
				rid, true, prevPageNum, nextPageNum);
		assert(rc == success);
	}

	cout << "now split the nonleaf page into two" << endl;
	ix->setPageEmpty(pageLeft);
	ix->setPageEmpty(pageRight);
	ix->setPageLeaf(pageLeft, CONST_NOT_LEAF);
	ix->setPageLeaf(pageRight, CONST_NOT_LEAF);
	int keyMovedUp;
	key = 41;
	nextPageNum = 15;
	rc = ix->splitPageNonLeaf(nonLeafPage, pageLeft, pageRight,
			attr, &key, nextPageNum, &keyMovedUp);
	assert(rc == success);
	cout << "key moved up is " << keyMovedUp << endl;
	ix->printPage(pageLeft, attr);
	ix->printPage(pageRight, attr);

	cout << "******************end split test int" << endl;
}

void basic_test_split_float() {
	cout << "******************begin split test float" << endl;

	RC rc;
//	SpaceManager *sm = SpaceManager::instance();
	IndexManager *ix = IndexManager::instance();
	string indexFileName = "test";
	remove(indexFileName.c_str());
	FileHandle fileHandle;
	Attribute attr;
	attr.length = 4;
	attr.name = "height";
	attr.type = TypeReal;
	// prepare data
	float key = 170.0;
	// test data size
	int numTuple = 10;

	// test leaf page
	char leafPage[PAGE_SIZE];
	ix->setPageEmpty(leafPage);
	ix->setPageLeaf(leafPage, CONST_IS_LEAF);
	RID rid;
	for (int i = 0; i < numTuple; ++i) {
		key = 170.0 + (double)rand()/RAND_MAX * 10;
		rid.pageNum = i;
		rid.slotNum = i+1;
		SlotNum slotToInsert;
		rc = ix->binarySearchEntry(leafPage, attr, &key, slotToInsert);
		rc = ix->insertEntryAtPos(leafPage, slotToInsert, attr,
				&key, sizeof(float),
				rid, true, 0, 0);
		assert(rc == success);
	}

	cout << "now split the leaf page into two" << endl;
	char pageLeft[PAGE_SIZE];
	char pageRight[PAGE_SIZE];
	float keyCopiedUp;
	key = 169.0;
	rid.pageNum = 123, rid.slotNum = 123;
	rc = ix->splitPageLeaf(leafPage, pageLeft, pageRight, attr,
			&key, rid, &keyCopiedUp);
	assert(rc == success);
	cout << "key copied up is " << keyCopiedUp << endl;

	ix->printPage(pageLeft, attr);
	ix->printPage(pageRight, attr);

	char nonLeafPage[PAGE_SIZE];
	ix->setPageEmpty(nonLeafPage);
	ix->setPageLeaf(nonLeafPage, CONST_NOT_LEAF);
	PageNum prevPageNum(0), nextPageNum(1);
	for (int i = 0; i < numTuple; ++i) {
		key = 170.0 + (double)rand()/RAND_MAX * 10;
		nextPageNum = i+1;
		SlotNum slotToInsert;
		rc = ix->binarySearchEntry(nonLeafPage, attr, &key, slotToInsert);
		rc = ix->insertEntryAtPos(nonLeafPage, slotToInsert, attr,
				&key, sizeof(float),
				rid, true, prevPageNum, nextPageNum);
		assert(rc == success);
	}

	cout << "now split the nonleaf page into two" << endl;
	ix->setPageEmpty(pageLeft);
	ix->setPageEmpty(pageRight);
	ix->setPageLeaf(pageLeft, CONST_NOT_LEAF);
	ix->setPageLeaf(pageRight, CONST_NOT_LEAF);
	float keyMovedUp;
	key = 182;
	nextPageNum = 15;
	rc = ix->splitPageNonLeaf(nonLeafPage, pageLeft, pageRight,
			attr, &key, nextPageNum, &keyMovedUp);
	assert(rc == success);
	cout << "key moved up is " << keyMovedUp << endl;
	ix->printPage(pageLeft, attr);
	ix->printPage(pageRight, attr);

	cout << "******************end split test float" << endl;
}

void basic_test_split_string() {
	cout << "******************begin split test string" << endl;

	RC rc;
//	SpaceManager *sm = SpaceManager::instance();
	IndexManager *ix = IndexManager::instance();
	string indexFileName = "test";
	remove(indexFileName.c_str());
	FileHandle fileHandle;
	Attribute attr;
	attr.length = 35;
	attr.name = "name";
	attr.type = TypeVarChar;
	// prepare data
	string str;
	char key[PAGE_SIZE];
	// test data size
	int numTuple = 10;

	// test leaf page
	char leafPage[PAGE_SIZE];
	ix->setPageEmpty(leafPage);
	ix->setPageLeaf(leafPage, CONST_IS_LEAF);
	RID rid;
	cout << "insert data to the leaf page"<< endl;
	for (int i = 0; i < numTuple; ++i) {
		int strLen = rand() % 10 + 5;
		str.clear();
		for (int j = 0; j < strLen; ++j) {
			str.push_back('a' + (rand()%26));
		}
		prepareKey(key, str);
		int keyLen = ix->getKeySize(attr, key);
		rid.pageNum = i;
		rid.slotNum = i+1;
		SlotNum slotToInsert;
		rc = ix->binarySearchEntry(leafPage, attr, key, slotToInsert);
		rc = ix->insertEntryAtPos(leafPage, slotToInsert, attr,
				key, keyLen,
				rid, true, 0, 0);
		assert(rc == success);
	}

	cout << "now split the leaf page into two" << endl;
	char pageLeft[PAGE_SIZE];
	char pageRight[PAGE_SIZE];
	char keyCopiedUp[PAGE_SIZE];
	prepareKey(key, "helen");
	rid.pageNum = 123, rid.slotNum = 123;
	rc = ix->splitPageLeaf(leafPage, pageLeft, pageRight, attr,
			key, rid, keyCopiedUp);
	assert(rc == success);
	cout << "key copied up is ";
	ix->printKey(attr,keyCopiedUp);
	cout << endl;

	ix->printPage(pageLeft, attr);
	ix->printPage(pageRight, attr);

	char nonLeafPage[PAGE_SIZE];
	ix->setPageEmpty(nonLeafPage);
	ix->setPageLeaf(nonLeafPage, CONST_NOT_LEAF);
	PageNum prevPageNum(0), nextPageNum(1);
	cout << "insert data to the non-leaf page"<< endl;
	for (int i = 0; i < numTuple; ++i) {
		int strLen = rand() % 10 + 5;
		str.clear();
		for (int j = 0; j < strLen; ++j) {
			str.push_back('a' + (rand()%26));
		}
		prepareKey(key, str);
		int keyLen = ix->getKeySize(attr, key);
		nextPageNum = i+1;
		SlotNum slotToInsert;
		rc = ix->binarySearchEntry(nonLeafPage, attr, key, slotToInsert);
		rc = ix->insertEntryAtPos(nonLeafPage, slotToInsert, attr,
				key, keyLen,
				rid, true, prevPageNum, nextPageNum);
		assert(rc == success);
	}

	cout << "now split the nonleaf page into two" << endl;
	ix->setPageEmpty(pageLeft);
	ix->setPageEmpty(pageRight);
	ix->setPageLeaf(pageLeft, CONST_NOT_LEAF);
	ix->setPageLeaf(pageRight, CONST_NOT_LEAF);
	char keyMovedUp[PAGE_SIZE];
	prepareKey(key, "helen");
	nextPageNum = 15;
	rc = ix->splitPageNonLeaf(nonLeafPage, pageLeft, pageRight,
			attr, key, nextPageNum, keyMovedUp);
	assert(rc == success);
	cout << "key moved up is ";
	ix->printKey(attr, keyMovedUp);
	cout << endl;
	ix->printPage(pageLeft, attr);
	ix->printPage(pageRight, attr);

	cout << "******************end split test string" << endl;
}

void basic_test_insert_int() {
	cout << "******************begin insert test int" << endl;
	SpaceManager *sm = SpaceManager::instance();
	IndexManager *ix = IndexManager::instance();
	string indexFileName = "test";
	remove(indexFileName.c_str());
	FileHandle fileHandle;
	Attribute attr;
	attr.length = 4;
	attr.name = "age";
	attr.type = TypeInt;
	// prepare data
	int key = 20;

	RC rc;

	rc = ix->createFile(indexFileName);
	assert(rc == success);
	rc = sm->initIndexFile(indexFileName);
	assert(rc == success);
	rc = ix->openFile(indexFileName, fileHandle);
	assert(rc == success);

	int numTuple = 100;

	RID rid;
	for (int i = 0; i < numTuple; ++i) {
		key = i;
		rid.pageNum = key;
		rid.slotNum = key;
		rc = ix->insertEntry(fileHandle, attr, &key,rid);
		assert(rc == success);
	}

	char page[PAGE_SIZE];
	PageNum totalPageNum = fileHandle.getNumberOfPages();
	totalPageNum = fileHandle.getNumberOfPages();
	for (PageNum pn = 0; pn < totalPageNum; ++pn) {
		cout << "%%%%%%%%%%%%%%%%Page number " << pn << "%%%%%%%%%%%%%%%%" << endl;
		rc = fileHandle.readPage(pn, page);
		assert(rc == success);
		ix->printPage(page, attr);
	}

	rc = ix->closeFile(fileHandle);
	assert(rc == success);

	cout << "******************end insert test int" << endl;
}

void basic_test_insert_float() {
	cout << "******************begin insert test float" << endl;

	SpaceManager *sm = SpaceManager::instance();
	IndexManager *ix = IndexManager::instance();
	string indexFileName = "test";
	remove(indexFileName.c_str());
	FileHandle fileHandle;
	Attribute attr;
	attr.length = 4;
	attr.name = "height";
	attr.type = TypeReal;
	// prepare data
	float key = 20;

	RC rc;

	rc = ix->createFile(indexFileName);
	assert(rc == success);
	rc = sm->initIndexFile(indexFileName);
	assert(rc == success);
	rc = ix->openFile(indexFileName, fileHandle);
	assert(rc == success);

	int numTuple = 100;

	RID rid;
	for (int i = 0; i < numTuple; ++i) {
		key = 170 + (double)i/numTuple;
		rid.pageNum = i;
		rid.slotNum = i;
		rc = ix->insertEntry(fileHandle, attr, &key,rid);
		assert(rc == success);
	}

	char page[PAGE_SIZE];
	PageNum totalPageNum = fileHandle.getNumberOfPages();
	totalPageNum = fileHandle.getNumberOfPages();
	for (PageNum pn = 0; pn < totalPageNum; ++pn) {
		cout << "%%%%%%%%%%%%%%%%Page number " << pn << "%%%%%%%%%%%%%%%%" << endl;
		rc = fileHandle.readPage(pn, page);
		assert(rc == success);
		ix->printPage(page, attr);
	}

	rc = ix->closeFile(fileHandle);
	assert(rc == success);
	cout << "******************end insert test float" << endl;
}

void basic_test_insert_string() {
	cout << "******************begin insert test string" << endl;
	SpaceManager *sm = SpaceManager::instance();
	IndexManager *ix = IndexManager::instance();
	string indexFileName = "test";
	remove(indexFileName.c_str());
	FileHandle fileHandle;
	Attribute attr;
	attr.length = 50;
	attr.name = "name";
	attr.type = TypeVarChar;
	// prepare data
	char key[PAGE_SIZE];

	RC rc;

	rc = ix->createFile(indexFileName);
	assert(rc == success);
	rc = sm->initIndexFile(indexFileName);
	assert(rc == success);
	rc = ix->openFile(indexFileName, fileHandle);
	assert(rc == success);

	int numTuple = 100;

	RID rid;
	for (int i = 0; i < numTuple; ++i) {
		prepareKey(key, generateRandomString(5,5));
		rid.pageNum = i;
		rid.slotNum = i;
		rc = ix->insertEntry(fileHandle, attr, key,rid);
		assert(rc == success);
	}

	char page[PAGE_SIZE];
	PageNum totalPageNum = fileHandle.getNumberOfPages();
	totalPageNum = fileHandle.getNumberOfPages();
	for (PageNum pn = 0; pn < totalPageNum; ++pn) {
		cout << "%%%%%%%%%%%%%%%%Page number " << pn << "%%%%%%%%%%%%%%%%" << endl;
		rc = fileHandle.readPage(pn, page);
		assert(rc == success);
		ix->printPage(page, attr);
	}

	rc = ix->closeFile(fileHandle);
	assert(rc == success);

	cout << "******************end insert test string" << endl;
}



//TODO
int main()
{
	cout << "Begin tests" << endl;
	/*
	basic_test();
	basic_test_search_int();
	basic_test_search_float();
	basic_test_search_string();
	basic_test_space_manager();
	basic_test_space_manager_2();

	basic_test_split_int();
	basic_test_split_float();
	basic_test_split_string();

	basic_test_insert_int();
	basic_test_insert_float();
	basic_test_insert_string();
	*/

	cout << "Finish all tests" << endl;
	return 0;
}

