#include <fstream>
#include <iostream>
#include <cassert>
#include <vector>

#include <cstdlib>
#include <cstdio>
#include <cstring>

#include <algorithm>
#include <sys/resource.h>
#include "../rm/rm.h"
#include "ix.h"

using namespace std;


const int success = 0;
IndexManager *ixManager;


//Global Initializations
//IX_Manager *ixManager = IX_Manager::Instance();
//const int success = 0;

void memProfile()
{
    int who = RUSAGE_SELF;
    struct rusage usage;
    int ret;
    ret=getrusage(who,&usage);
    cout<<usage.ru_maxrss<<"KB"<<endl;
}


void testCase_1(const string tablename, const string attrname)
{    
    // Functions tested
    // 1. Create Index **
    // 2. OpenIndex **
    // 3. CreateIndex -- when index is already created on that key **
    // 4. CloseIndex **
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 1****" << endl;

    RC rc;
    // Test CreateIndex
    rc = ixManager->createFile(tablename);
    if(rc == success) 
    {
        cout << "Index Created!" << endl;
    }
    else 
    {
        cout << "Failed Creating Index..." << endl;
    }
    
    // Test OpenIndex
    FileHandle ixHandle;
    rc = ixManager->openFile(tablename, ixHandle);
    if(rc == success) 
    {
        cout << "Index on " << attrname << " Opened!" << endl;
    }
    else 
    {
        cout << "Failed Opening Index..." << endl;
    }  
    
    // Test create duplicate index
    rc = ixManager->createFile(tablename);
    if(rc != success)
    {
        cout << "Duplicate Index not Created -- correct!" << endl;
    }
    else
    {
        cout << "Duplicate Index Created -- failure..." << endl;
    }
    
    // Test CloseIndex
    rc = ixManager->closeFile(ixHandle);
    if(rc == success)
    {
        cout << "Index Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Index..." << endl;
    }  
    return;
}


void testCase_2(const string tablename, const string attrname)
{    
    // Functions tested
    // 1. OpenIndex 
    // 2. Insert entry **
    // 3. Delete entry **
    // 4. Delete entry -- when the value is not there **
    // 5. Close Index
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 2****" << endl;
    
    RID rid;
    RC rc;

    // Test Open Index
    FileHandle ixHandle;
    rc = ixManager->openFile(tablename, ixHandle);
    if(rc == success) 
    {
        cout << "Index on " << attrname << " Opened!" << endl;
    }
    else 
    {
        cout << "Failed Opening Index..." << endl;
    }
    
    unsigned numOfTuples = 1;
    unsigned key = 100;
    rid.pageNum = key;
    rid.slotNum = key+1;

    void *payload = malloc(sizeof(int));
    int age = 18;
	memcpy(payload, &age, sizeof(int));
    
	Attribute attr;
	attr.type = TypeInt;

    // Test Insert Entry
    for(unsigned i = 0; i < numOfTuples; i++) 
    {
        rc = ixManager->insertEntry(ixHandle, attr, &age, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Entry..." << endl;
        }     
    }
    
    // Test Delete Entry
    rc = ixManager->deleteEntry(ixHandle, attr, payload, rid);
    if(rc != success)
    {
        cout << "Failed Deleting Entry..." << endl;
    }      
    
    // Test Delete Entry again
    rc = ixManager->deleteEntry(ixHandle, attr, payload, rid);
    if(rc == success) //This time it should NOT give success because entry is not there.
    {
        cout << "Entry deleted again...failure" << endl;
    }
    
    // Test Close Index
    rc = ixManager->closeFile(ixHandle);
    if(rc == success)
    {
        cout << "Index Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Index..." << endl;
    }
    return;
}


void testCase_3(const string tablename, const string attrname)
{    
    // Functions tested
    // 1. Destroy Index **
    // 2. OpenIndex -- should fail
    // 3. Scan  -- should fail
    cout << endl << "****In Test Case 3****" << endl;
    
    RC rc;
    // Test Destroy Index
    rc = ixManager->destroyFile(tablename);
    if(rc != success)
    {
        cout << "Failed Destroying Index..." << endl;
    }     
    
    Attribute attr;
    attr.type = TypeInt;

    // Test Open the destroyed index
    FileHandle ixHandle;
    rc = ixManager->openFile(tablename, ixHandle);
    if(rc == success) //should not work now
    {
        cout << "Index opened again...failure" << endl;
    }  
    
    IX_ScanIterator scanner;
    // Test Open Scan
    rc = ixManager->scan(ixHandle, attr, NULL, NULL, true, true, scanner);
    if(rc == success) 
    {
        cout << "Scan opened again...failure" << endl;
    }  
    return;
}


void testCase_4(const string tablename, const string attrname)
{    
    // Functions tested
    // 1. Create Index
    // 2. OpenIndex 
    // 3. Insert entry
    // 4. Scan entries NO_OP -- open**
    // 5. Scan close **
    // 6. CloseIndex
    // 7. DestroyIndex
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 4****" << endl;
 
    RID rid;
    RC rc;
    // Test CreateIndex
    FileHandle fileHandle;
    rc = ixManager->createFile(tablename);
    if(rc == success) 
    {
        cout << "Index Created!" << endl;
    }
    else 
    {
        cout << "Failed Creating Index..." << endl;
    }
    
    Attribute attr;
    attr.type = TypeInt;

    // Test OpenIndex
    rc = ixManager->openFile(tablename, fileHandle);
    if(rc == success) 
    {
        cout << "Index on " << attrname << " Opened!" << endl;
    }
    else 
    {
        cout << "Failed Opening Index..." << endl;
    }
    
    // Test InsertEntry
    unsigned numOfTuples = 1000;
    for(unsigned i = 0; i <= numOfTuples; i++) 
    {
        unsigned key = i+1;//just in case somebody starts pageNum and recordId from 1
        RID rid;
        rid.pageNum = key;
        rid.slotNum = key+1;
        
        rc = ixManager->insertEntry(fileHandle, attr, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Entry..." << endl;
        }     
    }
    
    // Scan
    IX_ScanIterator scanner;
    rc = ixManager->scan(fileHandle, attr, NULL, NULL, true, true, scanner);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Opening Scan!" << endl;
    }  

    int key;
    while (scanner.getNextEntry(rid, &key) != IX_EOF)
    {
        cout << rid.pageNum << " " << rid.slotNum << endl;
    }
    
    // Close Scan
    rc = scanner.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Scan..." << endl;
    }  
    
    // Close Index
    rc = ixManager->closeFile(fileHandle);
    if(rc == success)
    {
        cout << "Index Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Index..." << endl;
    }

    // Destroy Index
    rc = ixManager->destroyFile(tablename);
    if(rc == success)
    {
        cout << "Index Destroyed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Destroying Index..." << endl;
    }
    return;  
}



void testCase_5(const string tablename, const string attrname)
{    
    // Functions tested
    // 1. Create Index
    // 2. OpenIndex 
    // 3. Insert entry 
    // 4. Scan entries using GE_OP operator and checking if the values returned are correct. **
    // 5. Scan close 
    // 6. CloseIndex 
    // 7. DestroyIndex
    // NOTE: "**" signifies the new functions being tested in this test case.   
    cout << endl << "****In Test Case 5****" << endl;
        
    RID rid;
    RC rc;
    Attribute attr;
    attr.type = TypeInt;
    FileHandle fileHandle;

    // Test Create Index
    rc = ixManager->createFile(tablename);
    if(rc == success) 
    {
        cout << "Index Created!" << endl;
    }
    else 
    {
        cout << "Failed Creating Index..." << endl;
    }
    
    // Test Open Index
    rc = ixManager->openFile(tablename, fileHandle);
    if(rc == success) 
    {
        cout << "Index Opened!" << endl;
    }
    else 
    {
        cout << "Failed Opening Index..." << endl;
    }  
    
    // Test Insert Entry
    unsigned numOfTuples = 100;
    for(unsigned i = 1; i <= numOfTuples; i++) 
    {
        unsigned key = i;
        RID rid;
        rid.pageNum = key;
        rid.slotNum = key+1;
        
        rc = ixManager->insertEntry(fileHandle, attr, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        }     
    }
    
    numOfTuples = 100;
    for(unsigned i = 501; i < numOfTuples+500; i++) 
    {
        unsigned key = i;
        RID rid;
        rid.pageNum = key;
        rid.slotNum = key+1;
        
        rc = ixManager->insertEntry(fileHandle, attr, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        }     
    }
    
    // Test Open Scan
    IX_ScanIterator scanner;
    int value = 501;

    rc = ixManager->scan(fileHandle, attr, &value, NULL, true, true, scanner);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Opening Scan..." << endl;
    }  
    
    // Test IndexScan iterator
    int key;
    while(scanner.getNextEntry(rid, &key) != IX_EOF) {
        cout << rid.pageNum << " " << rid.slotNum << endl;
        if (rid.pageNum < 501 || rid.slotNum < 501)
        {
            cout << "Wrong entries output...failure" << endl;
        }
    }
    
    // Test Closing Scan
    rc = scanner.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Scan..." << endl;
    }  
    
    // Test Closing Index
    rc = ixManager->closeFile(fileHandle);
    if(rc == success)
    {
        cout << "Index Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Index..." << endl;
    }  
    
    // Test Destroying Index
    rc = ixManager->destroyFile(tablename);
    if(rc == success)
    {
        cout << "Index Destroyed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Destroying Index..." << endl;
    }
    return;
}


void testCase_6(const string tablename, const string attrname)
{
    // Functions tested
    // 1. Create Index
    // 2. OpenIndex 
    // 3. Insert entry 
    // 4. Scan entries using LT_OP operator and checking if the values returned are correct. Returned values are part of two separate insertions **
    // 5. Scan close 
    // 6. CloseIndex 
    // 7. DestroyIndex
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 6****" << endl;

    // Test CreateIndex
    RC rc;
    RID rid;
    Attribute attr;
    attr.type = TypeReal;

    rc = ixManager->createFile(tablename);
    if(rc == success) 
    {
        cout << "Index Created!" << endl;
    }
    else 
    {
        cout << "Failed Creating Index..." << endl;
    }

    // Test OpenIndex    
    FileHandle ixHandle;
    rc = ixManager->openFile(tablename, ixHandle);
    if(rc == success) 
    {
        cout << "Index Opened!" << endl;
    }
    else 
    {
        cout << "Failed Opening Index..." << endl;
    }  

    // Test InsertEntry    
    unsigned numOfTuples = 2000;
    for(unsigned i = 1; i <= numOfTuples; i++) 
    {
        float key = (float)i + 76.5;
        rid.pageNum = i;
        rid.slotNum = i;

        rc = ixManager->insertEntry(ixHandle, attr, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        }     
    }

    numOfTuples = 2000;
    for(unsigned i = 6000; i <= numOfTuples+6000; i++) 
    {
        float key = (float)i + 76.5;
        rid.pageNum = i;
        rid.slotNum = i-(unsigned)500;
        
        rc = ixManager->insertEntry(ixHandle, attr, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        }     
    }

    // Test Scan    
    IX_ScanIterator scanner;
    float compVal = 6500;

    rc = ixManager->scan(ixHandle, attr, NULL, &compVal, true, true, scanner);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Opening Scan..." << endl;
    }  

    // Test IndexScan Iterator
    float key;
    while (scanner.getNextEntry(rid, &key) != IX_EOF)
    {
        if(rid.pageNum % 500 == 0)
            cout << rid.pageNum << " " << rid.slotNum << endl;
        if ((rid.pageNum > 2000 && rid.pageNum < 6000) || rid.pageNum >= 6500)
        {
            cout << "Wrong entries output...failure" << endl;
        }
    }

    // Test CloseScan
    rc = scanner.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Scan..." << endl;
    }  

    // Test CloseIndex    
    rc = ixManager->closeFile(ixHandle);
    if(rc == success)
    {
        cout << "Index Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Index..." << endl;
    }  

    // Test DestroyIndex    
    rc = ixManager->destroyFile(tablename);
    if(rc == success)
    {
        cout << "Index Destroyed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Destroying Index..." << endl;
    }
    return;
}


void testCase_7(const string tablename, const string attrname)
{
    // Functions tested
    // 1. Create Index
    // 2. OpenIndex 
    // 3. Insert entry 
    // 4. Scan entries, and delete entries
    // 5. Scan close 
    // 6. CloseIndex 
    // 7. DestroyIndex
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 7****" << endl;
    
    RC rc;
    RID rid;
    Attribute attr;
    attr.type = TypeReal;

    // Test CreateIndex
    rc = ixManager->createFile(tablename);
    if(rc == success) 
    {
        cout << "Index Created!" << endl;
    }
    else 
    {
        cout << "Failed Creating Index..." << endl;
    }

    // Test OpenIndex
    FileHandle ixHandle;
    rc = ixManager->openFile(tablename, ixHandle);
    if(rc == success) 
    {
        cout << "Index Opened!" << endl;
    }
    else 
    {
        cout << "Failed Opening Index..." << endl;
    }  
    
    // Test InsertEntry
    unsigned numOfTuples = 100;
    for(unsigned i = 1; i <= numOfTuples; i++) 
    {
        float key = (float)i;
        rid.pageNum = i;
        rid.slotNum = i;

        rc = ixManager->insertEntry(ixHandle, attr, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        }     
    }

    // Test Scan
    IX_ScanIterator scanner;
    float compVal = 100.0;

    rc = ixManager->scan(ixHandle,
    		attr, NULL, &compVal, true, true, scanner);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Opening Scan..." << endl;
    }

    // Test DeleteEntry in IndexScan Iterator
    float key;
    while (scanner.getNextEntry(rid, &key) != IX_EOF)
    {
        cout << rid.pageNum << " " << rid.slotNum << endl;

        float key = (float)rid.pageNum;
        rc = ixManager->deleteEntry(ixHandle, attr, &key, rid);
        if(rc != success)
        {
            cout << "Failed deleting entry in Scan..." << endl;
        }
    }
    cout << endl;

    // Test CloseScan
    rc = scanner.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Scan..." << endl;
    }

    // Open Scan again
    rc = ixManager->scan(ixHandle, attr, NULL, &compVal, true, true, scanner);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Opening Scan..." << endl;
    }

    // Test IndexScan Iterator
    while (scanner.getNextEntry(rid, &key) != IX_EOF)
    {
        cout << "Entry returned: " << rid.pageNum << " " << rid.slotNum << "--- failure" << endl;

        if(rid.pageNum > 100)
        {
            cout << "Wrong entries output...failure" << endl;
        }
    }

    // Test CloseScan
    rc = scanner.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Scan..." << endl;
    }  

    // Test CloseIndex    
    rc = ixManager->closeFile(ixHandle);
    if(rc == success)
    {
        cout << "Index Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Index..." << endl;
    }  

    // Test DestroyIndex
    rc = ixManager->destroyFile(tablename);
    if(rc == success)
    {
        cout << "Index Destroyed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Destroying Index..." << endl;
    }
        
    return;
}


void testCase_8(const string tablename, const string attrname)
{
    // Functions tested
    // 1. Create Index
    // 2. OpenIndex 
    // 3. Insert entry 
    // 4. Scan entries, and delete entries
    // 5. Scan close
    // 6. Insert entries again
    // 7. Scan entries
    // 8. CloseIndex 
    // 9. DestroyIndex
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 8****" << endl;
    
    RC rc;
    RID rid;
    Attribute attr;
    attr.type = TypeReal;
    // Test CreateIndex
    rc = ixManager->createFile(tablename);
    if(rc == success) 
    {
        cout << "Index Created!" << endl;
    }
    else 
    {
        cout << "Failed Creating Index..." << endl;
    }

    // Test OpenIndex
    FileHandle ixHandle;
    rc = ixManager->openFile(tablename, ixHandle);
    if(rc == success) 
    {
        cout << "Index Opened!" << endl;
    }
    else 
    {
        cout << "Failed Opening Index..." << endl;
    }  
    
    // Test InsertEntry
    unsigned numOfTuples = 200;
    for(unsigned i = 1; i <= numOfTuples; i++) 
    {
        float key = (float)i;
        rid.pageNum = i;
        rid.slotNum = i;
        
        rc = ixManager->insertEntry(ixHandle, attr, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        }     
    }

    // Test Scan
    IX_ScanIterator scanner;
    float compVal = 200.0;

    rc = ixManager->scan(ixHandle, attr, NULL, &compVal, true, true, scanner);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Opening Scan..." << endl;
    }

    // Test DeleteEntry in IndexScan Iterator
    float key;
    while (scanner.getNextEntry(rid, &key) != IX_EOF)
    {
        cout << rid.pageNum << " " << rid.slotNum << endl;
        
        float key = (float)rid.pageNum;
        rc = ixManager->deleteEntry(ixHandle, attr, &key, rid);
        if(rc != success)
        {
            cout << "Failed deleting entry in Scan..." << endl;
        }
    }
    cout << endl;

    // Test CloseScan
    rc = scanner.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Scan..." << endl;
    }

    // Test InsertEntry Again
    numOfTuples = 500;
    for(unsigned i = 1; i <= numOfTuples; i++) 
    {
        float key = (float)i;
        rid.pageNum = i;
        rid.slotNum = i;
        
        rc = ixManager->insertEntry(ixHandle, attr, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        }     
    }

    // Test Scan
    compVal = 450.0;
    rc = ixManager->scan(ixHandle, attr, &compVal, NULL, false, true, scanner);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Opening Scan..." << endl;
    }

    while (scanner.getNextEntry(rid, &key) != IX_EOF)
    {
        cout << rid.pageNum << " " << rid.slotNum << endl;

        if(rid.pageNum <= 450 || rid.slotNum <= 450)
        {
            cout << "Wrong entries output...failure" << endl;
        }
    }
    cout << endl;

    // Test CloseScan
    rc = scanner.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Scan..." << endl;
    }  

    // Test CloseIndex
    rc = ixManager->closeFile(ixHandle);
    if(rc == success)
    {
        cout << "Index Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Index..." << endl;
    }  

    // Test DestroyIndex
    rc = ixManager->destroyFile(tablename);
    if(rc == success)
    {
        cout << "Index Destroyed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Destroying Index..." << endl;
    }
        
    return;
}


void testCase_9(string tablename, string attrname)
{
    // Functions tested
    // 1. Create Index
    // 2. OpenIndex 
    // 3. Insert entry 
    // 4. Scan entries, and delete entries
    // 5. Scan close
    // 6. Insert entries again
    // 7. Scan entries
    // 8. CloseIndex 
    // 9. DestroyIndex
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 9****" << endl;
    
    RC rc;
    RID rid;
    Attribute attr;
    attr.type = TypeInt;
    // Test CreateIndex
    rc = ixManager->createFile(tablename);
    if(rc == success) 
    {
        cout << "Index Created!" << endl;
    }
    else 
    {
        cout << "Failed Creating Index..." << endl;
    }

    // Test OpenIndex
    FileHandle ixHandle;
    rc = ixManager->openFile(tablename, ixHandle);
    if(rc == success) 
    {
        cout << "Index Opened!" << endl;
    }
    else 
    {
        cout << "Failed Opening Index..." << endl;
    }  
    
    // Test InsertEntry
    int numOfTuples = 30000;
    int A[30000];
    for(int i = 0; i < numOfTuples; i++)
    {
        A[i] = i;
    }
    random_shuffle(A, A+numOfTuples);

    for(unsigned i = 0; i < numOfTuples; i++) 
    {
        int key = A[i];
        rid.pageNum = i+1;
        rid.slotNum = i+1;
        
        rc = ixManager->insertEntry(ixHandle, attr, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        }     
    }

    // Test Scan
    IX_ScanIterator scanner;
    int compVal = 20000;

    rc = ixManager->scan(ixHandle, attr, NULL, &compVal, true, true, scanner);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Opening Scan..." << endl;
    }

    // Test DeleteEntry in IndexScan Iterator
    int count = 0;

    int key;
    while (scanner.getNextEntry(rid, &key) != IX_EOF)
    {
        if(count % 1000 == 0)
            cout << rid.pageNum << " " << rid.slotNum << endl;
        
        int key = A[rid.pageNum-1];
        rc = ixManager->deleteEntry(ixHandle, attr, &key, rid);
        if(rc != success)
        {
            cout << "Failed deleting entry in Scan..." << endl;
        }
        count++;
    }
    cout << "Number of deleted entries: " << count << endl;

    // Test CloseScan
    rc = scanner.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Scan..." << endl;
    }

    // Test InsertEntry Again
    numOfTuples = 20000;
    int B[20000];
    for(int i = 0; i < numOfTuples; i++)
    {
        B[i] = 30000+i;
    }
    random_shuffle(B, B+numOfTuples);

    for(int i = 0; i < numOfTuples; i++) 
    {
        int key = B[i];
        rid.pageNum = i+30001;
        rid.slotNum = i+30001;
        
        rc = ixManager->insertEntry(ixHandle, attr, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        }     
    }

    // Test Scan
    compVal = 35000;
    rc = ixManager->scan(ixHandle, attr, NULL, &compVal, true, true, scanner);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Opening Scan..." << endl;
    }

    count = 0;
    while (scanner.getNextEntry(rid, &key) != IX_EOF)
    {
        if (count % 1000 == 0)
            cout << rid.pageNum << " " << rid.slotNum << endl;

        if(rid.pageNum > 30000 && B[rid.pageNum-30001] > 35000)
        {
            cout << "Wrong entries output...failure" << endl;
        }
        count ++;
    }
    cout << "Number of scanned entries: " << count << endl;

    // Test CloseScan
    rc = scanner.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Scan..." << endl;
    }  

    // Test CloseIndex
    rc = ixManager->closeFile(ixHandle);
    if(rc == success)
    {
        cout << "Index Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Index..." << endl;
    }  

    // Test DestroyIndex    
    rc = ixManager->destroyFile(tablename);
    if(rc == success)
    {
        cout << "Index Destroyed Successfully!" << endl;  
    }
    else 
    {
        cout << "Failed Destroying Index..." << endl;
    }
    return;
}


void testCase_10(string tablename, string attrname)
{
    // Functions tested
    // 1. Create Index
    // 2. OpenIndex 
    // 3. Insert entry 
    // 4. Scan entries, and delete entries
    // 5. Scan close
    // 6. Insert entries again
    // 7. Scan entries
    // 8. CloseIndex 
    // 9. DestroyIndex
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 10****" << endl;
    
    RC rc;
    RID rid;
    Attribute attr;
    attr.type = TypeReal;
    // Test CreateIndex
    rc = ixManager->createFile(tablename);
    if(rc == success) 
    {
        cout << "Index Created!" << endl;
    }
    else 
    {
        cout << "Failed Creating Index..." << endl;
    }

    // Test OpenIndex
    FileHandle ixHandle;
    rc = ixManager->openFile(tablename, ixHandle);
    if(rc == success) 
    {
        cout << "Index Opened!" << endl;
    }
    else 
    {
        cout << "Failed Opening Index..." << endl;
    }  
    
    // Test InsertEntry
    int numOfTuples = 40000;
    float A[40000];
    for(int i = 0; i < numOfTuples; i++)
    {
        A[i] = (float)i;
    }
    random_shuffle(A, A+numOfTuples);

    for(int i = 0; i < numOfTuples; i++) 
    {
        float key = A[i];
        rid.pageNum = i+1;
        rid.slotNum = i+1;
        
        rc = ixManager->insertEntry(ixHandle, attr, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        }     
    }

    // Test Scan
    IX_ScanIterator scanner;
    float compVal = 30000.0;

    rc = ixManager->scan(ixHandle, attr, NULL, &compVal, true, true, scanner);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Opening Scan..." << endl;
    }

    // Test DeleteEntry in IndexScan Iterator
    int count = 0;
    float key;
    while (scanner.getNextEntry(rid, &key) != IX_EOF)
    {
        if(count % 1000 == 0)
            cout << rid.pageNum << " " << rid.slotNum << endl;
        
        float key = A[rid.pageNum-1];
        rc = ixManager->deleteEntry(ixHandle, attr, &key, rid);
        if(rc != success)
        {
            cout << "Failed deleting entry in Scan..." << endl;
        }
        count++;
    }
    cout << "Number of deleted entries: " << count << endl;

    // Test CloseScan
    rc = scanner.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Scan..." << endl;
    }

    // Test InsertEntry Again
    numOfTuples = 30000;
    float B[30000];
    for(int i = 0; i < numOfTuples; i++)
    {
        B[i] = (float)(40000+i);
    }
    random_shuffle(B, B+numOfTuples);

    for(unsigned i = 0; i < numOfTuples; i++) 
    {
        float key = B[i];
        rid.pageNum = i+40001;
        rid.slotNum = i+40001;
        
        rc = ixManager->insertEntry(ixHandle, attr, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        }     
    }

    // Test Scan
    compVal = 45000.0;
    rc = ixManager->scan(ixHandle, attr, NULL, &compVal, true, true, scanner);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Opening Scan..." << endl;
    }

    count = 0;
    while (scanner.getNextEntry(rid, &key) != IX_EOF)
    {
        if (count % 1000 == 0)
            cout << rid.pageNum << " " << rid.slotNum << endl;

        if(rid.pageNum > 40000 && B[rid.pageNum-40001] > 45000)
        {
            cout << "Wrong entries output...failure" << endl;
        }
        count ++;
    }
    cout << "Number of scanned entries: " << count << endl;

    // Test CloseScan
    rc = scanner.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Scan..." << endl;
    }  

    // Test CloseIndex    
    rc = ixManager->closeFile(ixHandle);
    if(rc == success)
    {
        cout << "Index Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Index..." << endl;
    }  

    // Test DestroyIndex    
    rc = ixManager->destroyFile(tablename);
    if(rc == success)
    {
        cout << "Index Destroyed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Destroying Index..." << endl;
    }
        
    return;
}


void testCase_extra_1(const string tablename, const string attrname)
{    
    // Functions tested
    // 1. Create Index
    // 2. OpenIndex 
    // 3. Insert entry 
    // 4. Scan entries.
    // 5. Scan close 
    // 6. CloseIndex 
    // 7. DestroyIndex
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Extra Test Case 1****" << endl;
    
    RC rc;
    RID rid;
    Attribute attr;
    attr.type = TypeInt;
    // Test CreateIndex
    rc = ixManager->createFile(tablename);
    if(rc == success) 
    {
        cout << "Index Created!" << endl;
    }
    else 
    {
        cout << "Failed Creating Index..." << endl;
    }

    // Test OpenIndex
    FileHandle ixHandle;
    rc = ixManager->openFile(tablename, ixHandle);
    if(rc == success) 
    {
        cout << "Index Opened!" << endl;
    }
    else 
    {
        cout << "Failed Opening Index..." << endl;
    }  
    
    // Test InsertEntry
    unsigned numOfTuples = 2000;
    for(unsigned i = 1; i <= numOfTuples; i++) 
    {
        unsigned key = 1234;
        rid.pageNum = i;
        rid.slotNum = i;

        rc = ixManager->insertEntry(ixHandle, attr, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        }     
    }

    numOfTuples = 2000;
    for(unsigned i = 500; i < numOfTuples+500; i++) 
    {
        unsigned key = 4321;
        rid.pageNum = i;
        rid.slotNum = i-5;
        
        rc = ixManager->insertEntry(ixHandle, attr, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        } 
    }
    
    // Test Scan
    IX_ScanIterator scanner;
    int compVal = 1234;

    rc = ixManager->scan(ixHandle, attr, &compVal, &compVal, true, true, scanner);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Opening Scan..." << endl;
    }

    // Test IndexScan Iterator
    int count = 0;
    int key;
    while (scanner.getNextEntry(rid, &key) != IX_EOF)
    {
        if(count % 1000 == 0)
            cout << rid.pageNum << " " << rid.slotNum << endl;
        count++;
    }
    cout << "Number of scanned entries: " << count << endl;

    // Test CloseScan
    rc = scanner.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Scan..." << endl;
    }  

    // Test CloseIndex    
    rc = ixManager->closeFile(ixHandle);
    if(rc == success)
    {
        cout << "Index Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Index..." << endl;
    }  

    // Test DestroyIndex    
    rc = ixManager->destroyFile(tablename);
    if(rc == success)
    {
        cout << "Index Destroyed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Destroying Index..." << endl;
    }
    return;
}


void testCase_extra_2(const string tablename, const string attrname)
{    
    // Functions tested
    // 1. Create Index
    // 2. OpenIndex 
    // 3. Insert entry 
    // 4. Scan entries.
    // 5. Scan close 
    // 6. CloseIndex 
    // 7. DestroyIndex
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Extra Test Case 2****" << endl;
    
    RC rc;
    RID rid;
    Attribute attr;
    attr.type = TypeInt;
    // Test CreateIndex
    rc = ixManager->createFile(tablename);
    if(rc == success) 
    {
        cout << "Index Created!" << endl;
    }
    else 
    {
        cout << "Failed Creating Index..." << endl;
    }

    // Test OpenIndex
    FileHandle ixHandle;
    rc = ixManager->openFile(tablename, ixHandle);
    if(rc == success) 
    {
        cout << "Index Opened!" << endl;
    }
    else 
    {
        cout << "Failed Opening Index..." << endl;
    }  
    
    // Test InsertEntry
    unsigned numOfTuples = 2000;
    for(unsigned i = 1; i <= numOfTuples; i++) 
    {
        unsigned key = 1234;
        rid.pageNum = i;
        rid.slotNum = i;
        
        rc = ixManager->insertEntry(ixHandle, attr, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        }     
    }

    numOfTuples = 2000;
    for(unsigned i = 500; i < numOfTuples+500; i++) 
    {
        unsigned key = 4321;
        rid.pageNum = i;
        rid.slotNum = i-5;
        
        rc = ixManager->insertEntry(ixHandle, attr, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        } 
    }
    
    // Test Scan
    IX_ScanIterator scanner;
    int compVal = 4321;

    rc = ixManager->scan(ixHandle, attr, &compVal, &compVal, true, true, scanner);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Opening Scan..." << endl;
    }

    // Test IndexScan Iterator
    int count = 0;
    int key;
    while(scanner.getNextEntry(rid, &key) != IX_EOF)
    {
        if(count % 1000 == 0)
            cout << rid.pageNum << " " << rid.slotNum << endl;
        count++;
    }
    cout << "Number of scanned entries: " << count << endl;

    // Test CloseScan
    rc = scanner.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Scan..." << endl;
    }  

    // Test CloseIndex
    rc = ixManager->closeFile(ixHandle);
    if(rc == success)
    {
        cout << "Index Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Index..." << endl;
    }  

    // Test DestroyIndex
    rc = ixManager->destroyFile(tablename);
    if(rc == success)
    {
        cout << "Index Destroyed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Destroying Index..." << endl;
    }
        
    return;
}


void testCase_extra_3(const string tablename, const string attrname)
{
    // Functions Tested:
    // 1. Create Index
    // 2. Open Index
    // 3. Insert Entry
    // 4. Scan
    // 5. Close Scan
    // 6. Close Index
    // 7. Destroy Index
    cout << endl << "****In Extra Test Case 3****" << endl;
    
    RC rc;
    RID rid;
    Attribute attr;
    attr.type = TypeVarChar;

    // Test CreateIndex
    rc = ixManager->createFile(tablename);
    if(rc == success) 
    {
        cout << "Index Created!" << endl;
    }
    else 
    {
        cout << "Failed Creating Index..." << endl;
    }

    // Test OpenIndex
    FileHandle ixHandle;
    rc = ixManager->openFile(tablename, ixHandle);
    if(rc == success) 
    {
        cout << "Index Opened!" << endl;
    }
    else 
    {
        cout << "Failed Opening Index..." << endl;
    }  
    
    // Test InsertEntry
    unsigned numOfTuples = 5000;
    for(unsigned i = 1; i <= numOfTuples; i++) 
    {
        void *key = malloc(100);
        unsigned count = ((i-1) % 26) + 1;
        *(int *)key = count;
   
        for(unsigned j = 0; j < count; j++)
        {
            *((char *)key+4+j) = 96+count;
        }

        rid.pageNum = i;
        rid.slotNum = i;
        
        rc = ixManager->insertEntry(ixHandle, attr, key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        }     
    }

    // Test Scan
    IX_ScanIterator scanner;
    unsigned offset = 20;
    void *key = malloc(100);
    *(int *)key = offset;
    for(unsigned j = 0; j < offset; j++)
    {
        *((char *)key+4+j) = 96+offset;
    }

    rc = ixManager->scan(ixHandle,attr, key, key, true, true, scanner);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Opening Scan..." << endl;
    }

    // Test IndexScan Iterator
    while(scanner.getNextEntry(rid, key) != IX_EOF)
    {
        cout << rid.pageNum << " " << rid.slotNum << endl;
    }
    cout << endl;

    // Test CloseScan
    rc = scanner.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Scan..." << endl;
    }  

    // Test CloseIndex    
    rc = ixManager->closeFile(ixHandle);
    if(rc == success)
    {
        cout << "Index Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Index..." << endl;
    }  

    // Test DestroyIndex
    rc = ixManager->destroyFile(tablename);
    if(rc == success)
    {
        cout << "Index Destroyed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Destroying Index..." << endl;
    }

    return;
}

void test()
{
    testCase_2("tbl_employee", "Age");
    testCase_3("tbl_employee", "Age");
    testCase_4("tbl_employee", "Age");
    testCase_5("tbl_employee", "Age");
    memProfile();

    testCase_6("tbl_employee", "Height");
    testCase_7("tbl_employee", "Height");
    testCase_8("tbl_employee", "Height");
    testCase_9("tbl_employee", "Age");
    testCase_10("tbl_employee", "Height");
    memProfile();

    // Extra Credit Work
    // Duplicat Entries
    testCase_extra_1("tbl_employee", "Age");
    testCase_extra_2("tbl_employee", "Age");
    // TypeVarChar
    testCase_extra_3("tbl_employee", "EmpName");
    memProfile();
    return; 
}


int main()
{

    //Global Initializations
    cout << "****Starting Test Cases****" << endl;
    ixManager = IndexManager::instance();
    testCase_1("tbl_employee", "Age");
    test();
    return 0;
}

