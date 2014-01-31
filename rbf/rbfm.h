
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
  RC getNextRecord(RID &rid, void *data);
  RC close();
};

/*
 * 		Middleware Version Manager
 */
#define MAX_ATTR_LEN 64
// version information
struct VersionInfoFrame {
	char name[MAX_ATTR_LEN];
	unsigned attrLength;
	AttrType attrType;
	char verChangeAction;
	char AttrColumn;
};
// define attribute change actions
const int ADD_ATTRIBUTE = 1;
const int DROP_ATTRIBUTE = 2;
const int NO_ACTION_ATTRIBUTE = 0;

// define maximum version
#define MAX_VER 50
// define error code
#define VERSION_TABLE_NOT_FOUND 50
#define VERSION_OVERFLOW 51
#define ATTR_OVERFLOW 52
#define ATTR_NOT_FOUND 53

// define char as version
typedef unsigned char VersionNumber;
// define record descriptor
typedef vector<Attribute> RecordDescriptor;
// define member types
typedef unordered_map<string, vector<RecordDescriptor> > AttrMap;
typedef unordered_map<string, VersionNumber> VersionMap;

// define the class
class VersionManager {
private:
	AttrMap attrMap;
	VersionMap versionMap;
	static VersionManager *_ver_manager;
public:
	VersionManager();
	// open up a Table
	RC initTableVersionInfo(const string &tableName, FileHandle &fileHandle);

	// get the attributes of a version
	RC getAttributes(const string &tableName, vector<Attribute> &attrs,
			const VersionNumber ver);
	// add an attribute
	RC addAttribute(const string &tableName, const Attribute &attr, FileHandle &fileHandle);
	// drop an attribute
	RC dropAttribute(const string &tableName, const string &atttributeName, FileHandle &fileHandle);

	void eraseTableVersionInfo(const string &tableName);
	void eraseAllInfo();

	RC formatFirst2Page(const string &tableName,
			const vector<Attribute> &attrs,
			FileHandle &fileHandle);

	void printAttributes(const string &tableName);
	static VersionManager* instance();
private:
	// tools
	// reset the attribute pages
	RC resetAttributePages(const vector<Attribute> &attrs, FileHandle &fileHandle);
	// get i'th version information
	void get_ithVersionInfo(void *page, VersionNumber ver,
			VersionInfoFrame &versionInfoFrame);
	// set i'th version Information
	void set_ithVersionInfo(void *page, VersionNumber ver,
			const VersionInfoFrame &versionInfoFrame);
	// get/set number of attributes
	unsigned getNumberAttributes(void *page);
	void setNumberAttributes(void *page, const unsigned numAttrs);
	// get/set the version of the number
	RC setVersionNumber(const string &tableName, void *page, const VersionNumber &ver);
	void getVersionNumber(const string &tableName, void *page, VersionNumber &ver);
	// create attribute descriptor for attribute
	vector<Attribute> recordAttributeDescriptor;
private:
	char page[PAGE_SIZE];
	void createAttrRecordDescriptor(vector<Attribute> &recordDescriptor);
};

/*
 * 			Record based file manager
 */

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


  RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);
  
  // This method will be mainly used for debugging/testing
  RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);

  // Note: record size is the size of the record
  // in the page exclude the rid size
  unsigned getRecordSize(const void *formattedData, const vector<Attribute> &recordDescriptor);

  // create an empty page
  void setPageEmpty(void *page);
  // get the size of a record & it's directory
  unsigned getRecordDirectorySize(const vector<Attribute> &recordDescriptor,
		  unsigned &recordSize);
  // get/set the free space start point
  void * getFreeSpaceStartPoint(void *page);
  void setFreeSpaceStartPoint(void *page, void * startPoint);
  // calculate the size of free space
  int getFreeSpaceSize(void *page);
  unsigned getEmptySpaceSize();
  // get/set the number of slots
  SlotNum getNumSlots(void *page);
  // get next available num slots
  SlotNum getNextAvailableSlot(void *page);
  RC setNumSlots(void *page, SlotNum num);
  // get directory of nth slot
  // Note that the id of slot starts from 0
  RC getSlotDir(void *page, SlotDir &slotDir, const SlotNum &nth);
  // set directory of nth slot
  RC setSlotDir(void *page, const SlotDir &slotDir, const SlotNum &nth);
  // get the forwarded page number and slot id
  void getForwardRID(RID &rid, const FieldAddress &offset);
  // set the forwarded page number and slot id
  void setForwardRID(const RID &rid, FieldAddress &offset);

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
  char pageContent[PAGE_SIZE];
};

#endif
