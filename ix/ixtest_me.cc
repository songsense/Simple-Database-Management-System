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
void basic_test() {
	RC rc;
	cout << "begin basic test" << endl;
	Attribute attr;
	attr.length = 35;
	attr.name = "EmpName";
	attr.type = TypeVarChar;
	char page[PAGE_SIZE];
	ix->setPageEmpty(page);
	ix->setPageLeaf(page, false);
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

	cout << "end basic test" << endl;
}

void basic_test_search_int() {
	cout << "begin search test" << endl;
	RC rc;
	cout << "insert entry" << endl;
	Attribute attr;
	attr.length = sizeof(int);
	attr.name = "age";
	attr.type = TypeInt;
	char page[PAGE_SIZE];
	ix->setPageEmpty(page);
	ix->setPageLeaf(page, true);
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
		rc = ix->binarySearchEntry(page, attr, key, slotNum);

		rc = ix->insertEntryAtPos(page, slotNum,
				attr,key, sizeof(int),
				rid, false,
				0, 0);
	}
	ix->printPage(page, attr);

	cout << "end search test" << endl;
}
void basic_test_search_float() {
	cout << "begin search test" << endl;
	RC rc;
	cout << "insert entry" << endl;
	Attribute attr;
	attr.length = sizeof(float);
	attr.name = "salary";
	attr.type = TypeReal;
	char page[PAGE_SIZE];
	ix->setPageEmpty(page);
	ix->setPageLeaf(page, false);
	// prepare data
	char key[sizeof(float)];
	SlotNum slotNum(1);
	RID	rid;
	rid.pageNum = 0, rid.slotNum = 1;
	int indexNum(100);
	float salaryVal(20.0);
	for (int i = 0; i < indexNum; ++i) {
		salaryVal = 5000.0 + (float(rand()))/RAND_MAX*1000.0;
		memcpy(key, &salaryVal, sizeof(float));
		rc = ix->binarySearchEntry(page, attr, key, slotNum);

		rc = ix->insertEntryAtPos(page, slotNum,
				attr,key, sizeof(float),
				rid, false,
				0, 0);
	}
	ix->printPage(page, attr);

	cout << "end search test" << endl;
}
void basic_test_search_string() {
	cout << "begin search test" << endl;
	RC rc;
	cout << "insert entry" << endl;
	Attribute attr;
	attr.length = 40;
	attr.name = "empName";
	attr.type = TypeVarChar;
	char page[PAGE_SIZE];
	ix->setPageEmpty(page);
	ix->setPageLeaf(page, false);
	// prepare data
	char key[sizeof(float)];
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
		rc = ix->binarySearchEntry(page, attr, key, slotNum);

		rc = ix->insertEntryAtPos(page, slotNum,
				attr,key, sizeof(int)+len,
				rid, false,
				0, 0);
	}
	ix->printPage(page, attr);

	cout << "end search test" << endl;
}
//TODO
int main()
{
	cout << "Begin tests" << endl;
	basic_test();
	basic_test_search_int();
	basic_test_search_float();
	basic_test_search_string();

	cout << "Finish all tests" << endl;
	return 0;
}

