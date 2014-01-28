
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "../rbf/rbfm.h"

using namespace std;

// define attribute change actions
#define ADD_ATTRIBUTE 0
#define DROP_ATTRIBUTE 1
// define maximum version
#define MAX_VER 100
// define errors
#define VERSION_OVERFLOW 30
#define ATTR_OVERFLOW 31

// version information
struct VersionInfoFrame {
	unsigned attrLength;
	unsigned short attrColumn;
	char attrType;
	char verChangeAction;
};


typedef unsigned char VersionNumber;
typedef unsigned AttrNumber;

# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iteratr to go through tuples
// The way to use it is like the following:
//  RM_ScanIterator rmScanIterator;
//  rm.open(..., rmScanIterator);
//  while (rmScanIterator(rid, data) != RM_EOF) {
//    process the data;
//  }
//  rmScanIterator.close();

class RM_ScanIterator {
public:
  RM_ScanIterator() {};
  ~RM_ScanIterator() {};

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data) { return RM_EOF; };
  RC close() { return -1; };
};


// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuples(const string &tableName);

  RC deleteTuple(const string &tableName, const RID &rid);

  // Assume the rid does not change after update
  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);
  // never reorganize the first two pages
  RC reorganizePage(const string &tableName, const unsigned pageNumber);

  // scan returns an iterator to allow the caller to go through the results one by one. 
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);


// Extra credit
public:
  RC dropAttribute(const string &tableName, const string &attributeName);

  RC addAttribute(const string &tableName, const Attribute &attr);
  // never reorganize the first two pages
  RC reorganizeTable(const string &tableName);



protected:
  RelationManager();
  ~RelationManager();

public: // tools

private:
  static RelationManager *_rm;

  RC formatFirst2Page(const string &tableName,
		  const vector<Attribute> &attrs,
		  FileHandle &fileHandle);
  // tools
  // get the version of the number
  VersionNumber getVersionNumber(void *page);
  // set the version of the number
  RC setVersionNumber(void *page, VersionNumber ver);
  // get i'th version information
  RC get_ithVersionInfo(void *page, VersionNumber ver, VersionInfoFrame &versionInfoFrame);
  // set i'th version Information
  RC set_ithVersionInfo(void *page, VersionNumber ver, const VersionInfoFrame &versionInfoFrame);
  // translate an attribute into a record
  unsigned translateAttribte2Record(const Attribute & attr, void *record);
  // translate a record into an attribute
  void translateRecord2Attribte(Attribute & attr, const void *record);
  // create attribute descriptor for attribute
  void createRecordDescriptor(vector<Attribute> &recordDescriptor);

  char page[PAGE_SIZE];
  char record[PAGE_SIZE];
  vector<Attribute> recordAttributeDescriptor;
  vector<Attribute> currentRecordDescriptor;
};

#endif
