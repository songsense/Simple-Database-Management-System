
#include "qe.h"
// get the table and condition attribute name from table.attribute
RC getTableAttributeName(const string &tableAttribute,
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
/*
 *
 * 				Filter
 *
 */
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
/*
 *
 * 				Project
 *
 */
// Iterator of input R
// vector containing attribute names
Project::Project(Iterator *input,
		const vector<string> &attrNames) {
	RelationManager *rm = RelationManager::instance();
	RC rc;
	if (attrNames.empty()) {
		initStatus = false;
		cerr << "Empty attribute names" << endl;
		return;
	}

	string tableName;
	string attributeName;
	vector<string> attributeNames;

	rc = getTableAttributeName(attrNames[0], tableName, attributeName);
	if (rc != SUCC) {
		initStatus = false;
		cerr << "Initialize Project failure: getTableAttributeName " << rc << endl;
		return;
	}
	attributeNames.push_back(attributeName);
	// get the attributes
	rc = rm->getAttributes(tableName, attrs);
	if (rc != SUCC) {
		cerr << "Filter: Fail to get attributes " << rc << endl;
		initStatus = false;
		return;
	}

	// get the projected attributes
	for (unsigned i = 1; i < attrNames.size(); ++i) {
		rc = getTableAttributeName(attrNames[i], tableName, attributeName);
		if (rc != SUCC) {
			initStatus = false;
			cerr << "Initialize Project failure: getTableAttributeName " << rc << endl;
			return;
		}
		attributeNames.push_back(attributeName);
	}

	// initialize the scanner
	rc = rm->scan(tableName, "",
			NO_OP, NULL, attributeNames, iter);
	if (rc != SUCC) {
		cerr << "Project: Fail to initialize scan " << rc << endl;
		initStatus = false;
		return;
	}

	initStatus = true;

}
Project::~Project(){
	iter.close();
}
RC Project::getNextTuple(void *data) {
	if (initStatus == false) {
		cerr << "Project::getNextTuple: initialization failure, quit " << endl;
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
void Project::getAttributes(vector<Attribute> &attrs) const{
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
};
