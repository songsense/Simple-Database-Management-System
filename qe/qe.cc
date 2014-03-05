
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) {
	RelationManager *rm = RelationManager::instance();
	RC rc;

	// get the table and condition attribute name from condition's lhsAttr
	string tableAttributeName(condition.lhsAttr);
	rc = getTableAttributeName(tableAttributeName, tableName, conditionAttribute);
	if (rc != SUCC) {
		cerr << "Filter: Fail to init filter because of illegal condition's lhsAttr " << rc << endl;
		initStatus = false;
		return;
	}

	// get the attributes
	rc = rm->getAttributes(tableName, attrs);
	if (rc != SUCC) {
		cerr << "Filter: Fail to get attributes " << rc << endl;
		initStatus = false;
		return;
	}

	// get the projected attribute names (all of them)
	vector<string> attributeNames;
	for (unsigned i = 1; i < attrs.size(); ++i) {
		attributeNames.push_back(attrs[i].name);
	}

	// set he compOp
	compOp = condition.op;

	// check if bRhsIsAttr is false, otherwise the parameter is passed by mistake
	if (condition.bRhsIsAttr == true) {
		// The condition is not for Filter but for NLjoin
		cerr << "The condition is not for Filter but for NLjoin " << endl;
		initStatus = false;
		return;
	}

	// initialize scan
	rc = rm->scan(tableName, conditionAttribute,
			compOp, condition.rhsValue.data, attributeNames, iter);
	if (rc != SUCC) {
		cerr << "Filter: Fail to initialize scan " << rc << endl;
		initStatus = false;
		return;
	}

	initStatus = true;
}
Filter::~Filter(){
	iter.close();
}
// ... the rest of your implementations go here
RC Filter::getNextTuple(void *data) {
	if (initStatus == false) {
		cerr << "getNextTuple: initialization failure, quit " << endl;
		return QE_EOF;
	}
	RID rid;
	if(iter.getNextTuple(rid, data) == RM_EOF) {
		return QE_EOF;
	} else {
		return SUCC;
	}
}
// For attribute in vector<Attribute>, name it as rel.attr
void Filter::getAttributes(vector<Attribute> &attrs) const {
    attrs.clear();
    attrs = this->attrs;
    unsigned i;

    // For attribute in vector<Attribute>, name it as rel.attr
    for(i = 0; i < attrs.size(); ++i)
    {
        string tmp = tableName;
        tmp += ".";
        tmp += attrs[i].name;
        attrs[i].name = tmp;
    }
}
// get the table and condition attribute name from condition's lhsAttr
RC Filter::getTableAttributeName(const string &tableAttribute,
		string &table, string &attribute) {
	RC rc = QE_FAIL_TO_SPLIT_TABLE_ATTRIBUTE;
	table.clear();
	attribute.clear();
	bool hit = false;
	for (char c : tableAttribute) {
		if (c != '.') {
			if (!hit) {
				table.push_back(c);
			} else {
				attribute.push_back(c);
			}
		} else {
			hit = true;
			rc = SUCC;
		}
	}
	return rc;
}
