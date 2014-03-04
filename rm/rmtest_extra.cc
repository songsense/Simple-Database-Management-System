#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include <fstream>
#include <iostream>
#include <cassert>

#include "rm.h"

using namespace std;

const int success = 0;

RelationManager *rm = RelationManager::instance();


// Function to prepare the data in the correct form to be inserted/read/updated.
void prepareTuple(const int nameLength, const string &name, const int age, const float height, const int salary, void *buffer, int *tupleSize)
{
    int offset = 0;
 
    memcpy((char *)buffer + offset, &(nameLength), sizeof(int));
    offset += sizeof(int);    
    memcpy((char *)buffer + offset, name.c_str(), nameLength);
    offset += nameLength;
    
    memcpy((char *)buffer + offset, &age, sizeof(int));
    offset += sizeof(int);
    
    memcpy((char *)buffer + offset, &height, sizeof(float));
    offset += sizeof(float);
    
    memcpy((char *)buffer + offset, &salary, sizeof(int));
    offset += sizeof(int);
    
    *tupleSize = offset;
}


// Function to get the data in the correct form to be inserted/read after adding
// the attribute ssn
void prepareTupleAfterAdd(const int nameLength, const string &name, const int age, const float height, const int salary, const int ssn, void *buffer, int *tupleSize)
{
    int offset=0;
    
    memcpy((char*)buffer + offset, &(nameLength), sizeof(int));
    offset += sizeof(int);    
    memcpy((char*)buffer + offset, name.c_str(), nameLength);
    offset += nameLength;
    
    memcpy((char*)buffer + offset, &age, sizeof(int));
    offset += sizeof(int);
        
    memcpy((char*)buffer + offset, &height, sizeof(float));
    offset += sizeof(float);
        
    memcpy((char*)buffer + offset, &salary, sizeof(int));
    offset += sizeof(int);
    
    memcpy((char*)buffer + offset, &ssn, sizeof(int));
    offset += sizeof(int);

    *tupleSize = offset;
}


// Function to parse the data in buffer
void printTuple(const void *buffer, const int tupleSize)
{
    int offset = 0;
    cout << "****Printing Buffer: Start****" << endl;
   
    int nameLength = 0;     
    memcpy(&nameLength, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "nameLength: " << nameLength << endl;
   
    char *name = (char *)malloc(100);
    memcpy(name, (char *)buffer+offset, nameLength);
    name[nameLength] = '\0';
    offset += nameLength;
    cout << "name: " << name << endl;
    
    int age = 0; 
    memcpy(&age, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "age: " << age << endl;
   
    float height = 0.0; 
    memcpy(&height, (char *)buffer+offset, sizeof(float));
    offset += sizeof(int);
    cout << "height: " << height << endl;
       
    int salary = 0; 
    memcpy(&salary, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "salary: " << salary << endl;

    cout << "****Printing Buffer: End****" << endl << endl;    
}


void printTupleAfterDrop( const void *buffer, const int tupleSize)
{
    int offset = 0;
    cout << "****Printing Buffer: Start****" << endl;
   
    int nameLength = 0;     
    memcpy(&nameLength, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "nameLength: " << nameLength << endl;
   
    char *name = (char *)malloc(100);
    memcpy(name, (char *)buffer+offset, nameLength);
    name[nameLength] = '\0';
    offset += nameLength;
    cout << "name: " << name << endl;
    
    int age = 0; 
    memcpy(&age, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "age: " << age << endl;
   
    float height = 0.0; 
    memcpy(&height, (char *)buffer+offset, sizeof(float));
    offset += sizeof(float);
    cout << "height: " << height << endl;
       
    cout << "****Printing Buffer: End****" << endl << endl;    
}   


void printTupleAfterAdd(const void *buffer, const int tupleSize)
{
    int offset = 0;
    cout << "****Printing Buffer: Start****" << endl;
   
    int nameLength = 0;     
    memcpy(&nameLength, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "nameLength: " << nameLength << endl;
   
    char *name = (char *)malloc(100);
    memcpy(name, (char *)buffer+offset, nameLength);
    name[nameLength] = '\0';
    offset += nameLength;
    cout << "name: " << name << endl;
    
    int age = 0; 
    memcpy(&age, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "age: " << age << endl;
   
    float height = 0; 
    memcpy(&height, (char *)buffer+offset, sizeof(float));
    offset += sizeof(float);
    cout << "height: " << height << endl;
    
    int ssn = 0;   
    memcpy(&ssn, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "SSN: " << ssn << endl;

    cout << "****Printing Buffer: End****" << endl << endl;    
}   


// Create an employee table
void createTable(const string &tableName)
{
    cout << "****Create Table " << tableName << " ****" << endl;
    
    // 1. Create Table ** -- made separate now.
    vector<Attribute> attrs;

    Attribute attr;
    attr.name = "EmpName";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)30;
    attrs.push_back(attr);

    attr.name = "Age";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    attrs.push_back(attr);

    attr.name = "Height";
    attr.type = TypeReal;
    attr.length = (AttrLength)4;
    attrs.push_back(attr);

    attr.name = "Salary";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    attrs.push_back(attr);

    int rc = rm->createTable(tableName, attrs);
    assert(rc == success);
    cout << "****Table Created: " << tableName << " ****" << endl << endl;
}


// Advanced Features:
void secB_1(const string &tableName, const int nameLength, const string &name, const int age, const int height, const int salary)
{
    // Functions Tested
    // 1. Insert tuple
    // 2. Read Attributes
    // 3. Drop Attributes **
    cout << "****In Extra Credit Test Case 1****" << endl;

    RID rid;    
    int tupleSize = 0;
    void *tuple = malloc(100);
    void *returnedData = malloc(100);
   
    // Insert Tuple 
    prepareTuple(nameLength, name, age, height, salary, tuple, &tupleSize);
    int rc = rm->insertTuple(tableName, tuple, rid);
    cout << "rc is " << rc << endl;
    assert(rc == success);

    // Read Attribute
    rc = rm->readAttribute(tableName, rid, "Salary", returnedData);
    assert(rc == success);
    cout << "Salary: " << *(int *)returnedData << endl;
 
    if(memcmp((char *)returnedData, (char *)tuple+18, 4) != 0)
    {
        cout << "Read attribute failed!" << endl; 
    }
    else
    {
        //cout << "Read attribute passed!" << endl; 

        // Drop the attribute
        rc = rm->dropAttribute(tableName, "Salary");
        assert(rc == success);

        // Read Tuple and print the tuple
        rc = rm->readTuple(tableName, rid, returnedData);
        assert(rc == success);
        printTupleAfterDrop(returnedData, tupleSize);
    }
    
    free(tuple);
    free(returnedData);

    cout<<"****Extra Credit Test Case 1 passed****"<<endl;
    return;
}


void secB_2(const string &tableName, const int nameLength, const string &name, const int age, const int height, const int salary, const int ssn)
{
    // Functions Tested
    // 1. Add Attribute **
    // 2. Insert Tuple
    cout << "****In Extra Credit Test Case 2****" << endl;
   
    RID rid; 
    int tupleSize=0;
    void *tuple = malloc(100);
    void *returnedData = malloc(100);
   
    // Test Add Attribute
    Attribute attr;
    attr.name = "SSN";
    attr.type = TypeInt;
    attr.length = 4;
    int rc = rm->addAttribute(tableName, attr);
    assert(rc == success);

    // Test Insert Tuple
    prepareTupleAfterAdd(nameLength, name, age, height, salary, ssn, tuple, &tupleSize);
    rc = rm->insertTuple(tableName, tuple, rid);
    assert(rc == success);

    // Test Read Tuple
    rc = rm->readTuple(tableName, rid, returnedData);
    assert(rc == success);

    cout << "Insert Data:" << endl;
    printTupleAfterAdd(tuple, tupleSize);

    cout << "Returned Data:" << endl;
    printTupleAfterAdd(returnedData, tupleSize);

    if (memcmp(returnedData, tuple, tupleSize) != 0)
    {
        cout << "****Extra Credit Test Case 2 failed****" << endl << endl; 
    }
    else
    {
        cout << "****Extra Credit Test Case 2 passed****" << endl << endl; 
    }

    free(tuple);
    free(returnedData);
    return;   
}


void secB_3(const string &tableName)
{
    // Functions Tested
    // 1. Reorganize Table **
    cout << "****In Extra Credit Test Case 3****" << endl;
    
    int tupleSize = 0;
    void *tuple = malloc(100);
    void *returnedData = malloc(100);
   
    RID rid; 
    int numTuples = 200;
    RID rids[numTuples];
   
    int rc = 0; 
    for(int i=0; i < numTuples; i++)
    {
        // Insert Tuple
        prepareTuple(6, "Tester", 100+i, i, 123, tuple, &tupleSize);
        rc = rm->insertTuple(tableName, tuple, rid);
        assert(rc == success);

        rids[i] = rid;
    }
	
	// Delete the first 100 tuples
    for(int i = 0; i < 100; i++)
    {
        rc = rm->deleteTuple(tableName, rids[i]);
        assert(rc == success);

        rc = rm->readTuple(tableName, rids[i], returnedData);
        assert(rc != success);
    }
    cout << "After deletion!" << endl;

    // Reorganize Table
    rc = rm->reorganizeTable(tableName);
    assert(rc == success);

    for(int i = 0; i < 100; i++)
    {
        // Read Tuple
        rc = rm->readTuple(tableName, rids[i], returnedData);
        assert(rc == success);

        // Print the tuple
        printTuple(returnedData, tupleSize);
    }

    // Delete the table
    rc = rm->deleteTable(tableName);
    assert(rc == success);

    free(tuple);
    free(returnedData);

    cout<<"****Extra Credit Test Case 3  passed*****"<<endl;
    return;
}   
    
int main()
{
    string name1 = "Peters";
    string name2 = "Victors";
    
    // Extra Credits
    cout << "Test Extra Credits...." << endl;
    remove("tbl_employee100");
    // Drop Attribute
    createTable("tbl_employee100");
    secB_1("tbl_employee100", 6, name1, 24, 170, 5000);

    // Add Attributes
    createTable("tbl_employee200");
    secB_2("tbl_employee200", 6, name2, 22, 180, 6000, 999);

    // Reorganize Table
    createTable("tbl_employee300");  
    secB_3("tbl_employee300");

    return 0;
}

