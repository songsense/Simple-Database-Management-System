
#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <sstream>
#include <vector>

#include "../rbf/pfm.h"

using namespace std;

// define a bunch of types
typedef unsigned long FieldAddress; 		// for address
typedef unsigned short FieldOffset;
typedef unsigned short SlotNum;

// define return code for record manager
#define RECORD_FILE_HANDLE_NOT_FOUND 1000
#define RECORD_OVERFLOW 1001
#define RECORD_NOT_ENOUGH_SPACE_FOR_MORE_SLOTS 1002


// Record ID
typedef struct
{
  unsigned pageNum;
  unsigned slotNum;
} RID;


// slot directory: saved in pages
typedef struct {
	FieldAddress recordOffset;
	unsigned recordLength;
	char version;
} SlotDir;

// define delete/update mode
// NOTE here, we use the C++ grammar for we need operate memory
// must be extra careful
const unsigned RECORD_DEL = -1;
const unsigned RECORD_FORWARD = -2;
// define return code when data is deleted/updated
#define RC_RECORD_DELETED 40
#define RC_RECORD_FORWARDED 41


// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar } AttrType;

typedef unsigned AttrLength;

struct Attribute {
    string   name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum { EQ_OP = 0,  // =
           LT_OP,      // <
           GT_OP,      // >
           LE_OP,      // <=
           GE_OP,      // >=
           NE_OP,      // !=
           NO_OP       // no condition
} CompOp;



/****************************************************************************
The scan iterator is NOT required to be implemented for part 1 of the project 
*****************************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iteratr to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();


class RBFM_ScanIterator {
public:
  RBFM_ScanIterator() {};
  ~RBFM_ScanIterator() {};

  // "data" follows the same format as RecordBasedFileManager::insertRecord()
  RC getNextRecord(RID &rid, void *data) { return RBFM_EOF; };
  RC close() { return -1; };
};


class RecordBasedFileManager
{
public:
  static RecordBasedFileManager* instance();

  RC createFile(const string &fileName);
  
  RC destroyFile(const string &fileName);
  
  RC openFile(const string &fileName, FileHandle &fileHandle);
  
  RC closeFile(FileHandle &fileHandle);

  //  Format of the data passed into the function is the following:
  //  1) data is a concatenation of values of the attributes
  //  2) For int and real: use 4 bytes to store the value;
  //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
  //  !!!The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute()
  RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);
  RC insertRecordWithVersion(FileHandle &fileHandle,
		  const vector<Attribute> &recordDescriptor,
		  const void *data, RID &rid, const char &ver);

  RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);
  RC readRecordWithVersion(FileHandle &fileHandle,
		  const vector<Attribute> &recordDescriptor,
		  const RID &rid, void *data, char &ver);
  
  // This method will be mainly used for debugging/testing
  RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);
  // Translate record to printable version
  void translateRecord2Printable(const void *rawData, void *formattedData, const vector<Attribute> &recordDescriptor);
  // Translate printable version to raw version
  // Note: record size is the size of the record
  // in the page exclude the rid size
  unsigned translateRecord2Raw(void *rawData, const void *formattedData, const vector<Attribute> &recordDescriptor);

  // create an empty page
  void createEmptyPage(void *page);
  // get the size of a record & it's directory
  unsigned getRecordDirectorySize(const vector<Attribute> &recordDescriptor,
		  unsigned &recordSize);
  // get/set the free space start point
  void * getFreeSpaceStartPoint(void *page);
  void setFreeSpaceStartPoint(void *page, void * startPoint);
  // calculate the size of free space
  unsigned getFreeSpaceSize(void *page);
  unsigned getEmptySpaceSize();
  // get/set the number of slots
  SlotNum getNumSlots(void *page);
  RC setNumSlots(void *page, SlotNum num);
  // get directory of nth slot
  // Note that the id of slot starts from 0
  RC getSlotDir(void *page, SlotDir &slotDir, const SlotNum &nth);
  // set directory of nth slot
  RC setSlotDir(void *page, const SlotDir &slotDir, const SlotNum &nth);
  // get the forwarded page number and slot id
  void getForwardRID(RID &rid, FieldAddress offset);
  char pageContent[PAGE_SIZE];
  char recordConent[PAGE_SIZE];
/**************************************************************************************************************************************************************
***************************************************************************************************************************************************************
IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for part 1 of the project
***************************************************************************************************************************************************************
***************************************************************************************************************************************************************/
public:
  RC deleteRecords(FileHandle &fileHandle);

  RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);

  // Assume the rid does not change after update
  RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);

  RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string attributeName, void *data);

  RC reorganizePage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const unsigned pageNumber);

  // scan returns an iterator to allow the caller to go through the results one by one. 
  RC scan(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator);


// Extra credit for part 2 of the project, please ignore for part 1 of the project
public:

  RC reorganizeFile(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor);


protected:
  RecordBasedFileManager();
  ~RecordBasedFileManager();

private:
  static RecordBasedFileManager *_rbf_manager;
};

#endif
