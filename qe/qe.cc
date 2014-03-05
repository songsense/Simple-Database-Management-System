
#include "qe.h"
/*
 *
 * 					General API
 *
 */
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
// copy the data according to the type
void copyData(void *dest, const void *src, const AttrType &type) {
	int len;
	switch(type) {
	case TypeInt:
		memcpy(dest, src, sizeof(int));
		break;
	case TypeReal:
		memcpy(dest, src, sizeof(float));
		break;
	case TypeVarChar:
		len = *((int *)src);
		memcpy(dest, src, sizeof(int) + len);
		break;
	}
}
// move the pointer according to the type
void movePointer(char *&data, const AttrType &type) {
	int len = 0;
	switch(type) {
	case TypeInt:
		data += sizeof(int);
		break;
	case TypeReal:
		data += sizeof(float);
		break;
	case TypeVarChar:
		len = *((int *)data);
		data += (sizeof(int) + len);
		break;
	}
}
bool compareValues(const char *lhs, const char *rhs,
		const CompOp &compOp, const AttrType &type) {
	int lhs_int, rhs_int;
	float lhs_float, rhs_float;
	string lhs_str, rhs_str;
	int len;
	char tempData[PAGE_SIZE];
	switch(type) {
	case TypeInt:
		lhs_int = *((int *)lhs);
		rhs_int = *((int *)rhs);
		return compareValueTemplate(lhs_int, rhs_int, compOp);
		break;
	case TypeReal:
		lhs_float = *((float *)lhs);
		rhs_float = *((float *)rhs);
		return compareValueTemplate(lhs_float, rhs_float, compOp);
		break;
	case TypeVarChar:
		// left string
		len = *((int *)lhs);
		memcpy(tempData, lhs + sizeof(int), len);
		tempData[len] = '\0';
		lhs_str.assign(tempData);
		// right string
		len = *((int *)rhs);
		memcpy(tempData, rhs + sizeof(int), len);
		tempData[len] = '\0';
		rhs_str.assign(tempData);
		// compare two string
		return compareValueTemplate(lhs_str, rhs_str, compOp);
		break;
	}
	return true;
}
// get a record size according to the attributes
int getRecordSize(char *data, const vector<Attribute> &attrs) {
	int len = 0;
	char *temp = data;
	int strLen = 0;
	for (Attribute attr : attrs) {
		switch(attr.type) {
		case TypeInt:
			len += sizeof(int);
			temp += sizeof(int);
			break;
		case TypeReal:
			len += sizeof(float);
			temp += sizeof(float);
			break;
		case TypeVarChar:
			strLen = *((int *)temp) + sizeof(int);
			len += strLen;
			temp += strLen;
			break;
		}
	}

	return len;
}
/*
 *
 * 				Filter
 *
 */
Filter::Filter(Iterator* input, const Condition &condition) {
	// set the iterator
	iter = input;

	// set the left hand side string
	lhsAttr = condition.lhsAttr;

	// set the compOp
	compOp = condition.op;

	// check if bRhsIsAttr is false, otherwise the parameter is passed by mistake
	if (condition.bRhsIsAttr == true) {
		// The condition is not for Filter but for NLjoin
		cerr << "The condition is not for Filter but for NLjoin " << endl;
		initStatus = false;
		return;
	}

	// set type
	type = condition.rhsValue.type;

	// set value
	copyValue(condition.rhsValue);

	// set attribute names
	iter->getAttributes(attributeNames);

	initStatus = true;
}
Filter::~Filter(){
}
// ... the rest of your implementations go here
RC Filter::getNextTuple(void *data) {
	if (initStatus == false) {
		cerr << "getNextTuple: initialization failure, quit " << endl;
		return QE_EOF;
	}
	RC rc;

	do {
		rc = iter->getNextTuple(data);
		if (rc != SUCC) {
			return QE_EOF;
		}
	} while (!compareValue(data));

	return SUCC;
}
// For attribute in vector<Attribute>, name it as rel.attr
void Filter::getAttributes(vector<Attribute> &attrs) const {
    iter->getAttributes(attrs);
}
void Filter::copyValue(const Value &input) {
	int len = 0;
	switch(input.type) {
	case TypeInt:
		memcpy(value, input.data, sizeof(int));
		break;
	case TypeReal:
		memcpy(value, input.data, sizeof(float));
		break;
	case TypeVarChar:
		len = *((int *)input.data);
		memcpy(value, input.data, sizeof(int) + len);
		break;
	}
}

bool Filter::compareValue(void *input) {
	char *lhs_value = (char *)input;
	int len;

	for (Attribute attr : attributeNames) {
		if (attr.name == lhsAttr) {
			break;
		}
		// move pointer
		movePointer(lhs_value, attr.type);
	}


	int lhs_int, rhs_int;
	float lhs_float, rhs_float;
	string lhs_str, rhs_str;
	switch(type) {
	case TypeInt:
		lhs_int = *((int *)lhs_value);
		rhs_int = *((int *)value);
		return compareValueTemplate(lhs_int, rhs_int, compOp);
		break;
	case TypeReal:
		lhs_float = *((float *)lhs_value);
		rhs_float = *((float *)value);
		return compareValueTemplate(lhs_float, rhs_float, compOp);
		break;
	case TypeVarChar:
		// left string
		len = *((int *)lhs_value);
		memcpy(tempData, lhs_value + sizeof(int), len);
		tempData[len] = '\0';
		lhs_str.assign(tempData);
		// right string
		len = *((int *)value);
		memcpy(tempData, value + sizeof(int), len);
		tempData[len] = '\0';
		rhs_str.assign(tempData);
		// compare two string
		return compareValueTemplate(lhs_str, rhs_str, compOp);
		break;
	}
	return true;
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
	if (attrNames.empty()) {
		initStatus = false;
		cerr << "Empty attribute names" << endl;
		return;
	}

	// set the iterator
	iter = input;

	for (string attrName : attrNames) {
		checkExistAttrNames.emplace(attrName);
	}

	// For attribute in vector<Attribute>, name it as rel.attr
	input->getAttributes(originAttrs);
	for (Attribute attr : originAttrs) {
		// original attributes saves the information retrieved from
		// iterator input
		if (checkExistAttrNames.count(attr.name) > 0) {
			attrs.push_back(attr);
		}
	}

	initStatus = true;

}
Project::~Project(){
}
RC Project::getNextTuple(void *data) {
	if (initStatus == false) {
		cerr << "Project::getNextTuple: initialization failure, quit " << endl;
		return QE_EOF;
	}
	RC rc = iter->getNextTuple(tempData);
	if (rc != SUCC) {
		return rc;
	}

	char *returnData = (char*)data;
	char *obtainData = (char*)tempData;
	// now check if the attribute exists
	for (Attribute attr : originAttrs) {
		if (checkExistAttrNames.count(attr.name) > 0) {
			// contains the attribute. copy it to the data
			copyData(returnData, obtainData, attr.type);
			// move pointer
			movePointer(returnData, attr.type);
		}
		// move pointer
		movePointer(obtainData, attr.type);
	}
	return SUCC;
}

// For attribute in vector<Attribute>, name it as rel.attr
void Project::getAttributes(vector<Attribute> &attrs) const{
	attrs.clear();
	attrs = this->attrs;
};
/*
 *
 * 				NLJoin
 *
 */
/*
struct Condition {
    string lhsAttr;         // left-hand side attribute
    CompOp  op;             // comparison operator
    bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise
    string rhsAttr;         // right-hand side attribute if bRhsIsAttr = TRUE
    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};
 */
NLJoin::NLJoin(Iterator *leftIn,                             // Iterator of input R
               TableScan *rightIn,                           // TableScan Iterator of input S
               const Condition &condition,                   // Join condition
               const unsigned numPages                       // Number of pages can be used to do join (decided by the optimizer)
        ){
	if (condition.bRhsIsAttr == false) {
		initStatus = false;
		return;
	}
	leftIter = leftIn;
	leftIter->getAttributes(leftAttrs);
	// set the compAttrType
	// this is used to faster compare left and right given this type value
	for (Attribute attr : leftAttrs) {
		if (attr.name == condition.lhsAttr) {
			compAttrType = attr.type;
			break;
		}
	}

	rightIter = rightIn;
	rightIter->getAttributes(rightAttrs);

	// set the attrs
	attrs = leftAttrs;
	for (Attribute attr : rightAttrs) {
		attrs.push_back(attr);
	}

	this->condition = condition;
	this->numPages = numPages;

	needLoadNextLeftValue = true;
	initStatus = true;
}
NLJoin::~NLJoin(){

}
RC NLJoin::getNextTuple(void *data){
	if (initStatus == false) {
		return QE_EOF;
	}

	RC rc;
	do {
		rc = rightIter->getNextTuple(curRightValue);
		if (rc != SUCC) {
			// need to load more outer data
			needLoadNextLeftValue = true;
			// reset the iterator, rewind to the begining
			rightIter->setIterator();
			// retry load data
			rc = rightIter->getNextTuple(curRightValue);
			if (rc != SUCC) {
				rc = QE_FAIL_TO_FIND_CONDITION_ATTRIBUTE;
				cerr << "Fail to load the inner data " << rc << endl;
				return rc;
			}
		}
		// successfully load the current inner data
		// get current right condition value
		rc = getAttributeValue(curRightValue,
				curRightConditionValue, rightAttrs, condition.rhsAttr);
		if (rc != SUCC) {
			cerr << "Fail to find the right condition value by the attribute " << rc << endl;
			return rc;
		}

		if (needLoadNextLeftValue == true) {
			rc = leftIter->getNextTuple(curLeftValue);
			if (rc != SUCC) {
				return QE_EOF;
			}
			// get current left condition value
			rc = getAttributeValue(curLeftValue,
					curLeftConditionValue, leftAttrs, condition.lhsAttr);
			if (rc != SUCC) {
				cerr << "Fail to find the left condition value by the attribute " << rc << endl;
				return rc;
			}
			// set the needLoadNextLeftValue to be false
			// next outer data ready
			needLoadNextLeftValue = false;
		}
		// comare values to see if need to return the value
	} while (!compareValues(curLeftConditionValue, curRightConditionValue, condition.op, compAttrType));

	// get the record size
    int leftRecordSize = getRecordSize(curLeftValue, leftAttrs);
    int rightRecordSize = getRecordSize(curRightValue, rightAttrs);
    // copy data
    char *returnData = (char *)data;
    memcpy(returnData, curLeftValue, leftRecordSize);
    memcpy(returnData + leftRecordSize, curRightValue, rightRecordSize);

	return SUCC;
}
RC NLJoin::getAttributeValue(char *data,
		char *attrData, const vector<Attribute> &attrs,
		const string &conditionAttr) {
	char *val = data;
	for (Attribute attr : attrs) {
		if (attr.name == conditionAttr) {
			copyData(attrData, val, attr.type);
			return SUCC;
		}
		movePointer(val, attr.type);
	}
	return QE_FAIL_TO_FIND_CONDITION_ATTRIBUTE;
}

// For attribute in vector<Attribute>, name it as rel.attr
void NLJoin::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->attrs;
};
/*
 *
 * 				NLJoin
 *
 */
/*
struct Condition {
    string lhsAttr;         // left-hand side attribute
    CompOp  op;             // comparison operator
    bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise
    string rhsAttr;         // right-hand side attribute if bRhsIsAttr = TRUE
    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};
 */
INLJoin::INLJoin(Iterator *leftIn,                             // Iterator of input R
		IndexScan *rightIn,                           // IndexScan Iterator of input S
        const Condition &condition,                   // Join condition
        const unsigned numPages                       // Number of pages can be used to do join (decided by the optimizer)
        ){
	if (condition.bRhsIsAttr == false) {
		initStatus = false;
		return;
	}
	leftIter = leftIn;
	leftIter->getAttributes(leftAttrs);
	// set the compAttrType
	// this is used to faster compare left and right given this type value
	for (Attribute attr : leftAttrs) {
		if (attr.name == condition.lhsAttr) {
			compAttrType = attr.type;
			break;
		}
	}

	rightIter = rightIn;
	rightIter->getAttributes(rightAttrs);

	// set the attrs
	attrs = leftAttrs;
	for (Attribute attr : rightAttrs) {
		attrs.push_back(attr);
	}

	this->condition = condition;
	this->numPages = numPages;

	needLoadNextLeftValue = true;
	initStatus = true;
}
INLJoin::~INLJoin(){

}
RC INLJoin::getNextTuple(void *data){
	if (initStatus == false) {
		return QE_EOF;
	}

	RC rc;
	do {
		rc = rightIter->getNextTuple(curRightValue);
		if (rc != SUCC) {
			// need to load more outer data
			needLoadNextLeftValue = true;
		}
		if (needLoadNextLeftValue == true) {
			do {
				rc = leftIter->getNextTuple(curLeftValue);
				if (rc != SUCC) {
					return QE_EOF;
				}
				// get current left condition value
				rc = getAttributeValue(curLeftValue,
						curLeftConditionValue, leftAttrs, condition.lhsAttr);
				if (rc != SUCC) {
					cerr << "Fail to find the left condition value by the attribute " << rc << endl;
					return rc;
				}
				// set the needLoadNextLeftValue to be false
				// next outer data ready
				needLoadNextLeftValue = false;

				// now reset the right iterator
				setRightIterator(curLeftConditionValue);
				// get the first right data
			} while(rightIter->getNextTuple(curRightValue) == QE_EOF);
		}

		// successfully load the current inner data
		// get current right condition value
		rc = getAttributeValue(curRightValue,
				curRightConditionValue, rightAttrs, condition.rhsAttr);
		if (rc != SUCC) {
			cerr << "Fail to find the right condition value by the attribute " << rc << endl;
			return rc;
		}
		// compare values to see if need to return the value
	} while (!compareValues(curLeftConditionValue, curRightConditionValue, condition.op, compAttrType));

	// get the record size
    int leftRecordSize = getRecordSize(curLeftValue, leftAttrs);
    int rightRecordSize = getRecordSize(curRightValue, rightAttrs);
    // copy data
    char *returnData = (char *)data;
    memcpy(returnData, curLeftValue, leftRecordSize);
    memcpy(returnData + leftRecordSize, curRightValue, rightRecordSize);

	return SUCC;
}
void INLJoin::setRightIterator(char *leftValue) {
	switch(condition.op) {
	case EQ_OP:
		rightIter->setIterator(leftValue, leftValue, true, true);
		break;
	case LT_OP:
		rightIter->setIterator(leftValue, NULL, false, true);
		break;
	case GT_OP:
		rightIter->setIterator(NULL, leftValue, true, false);
		break;
	case LE_OP:
		rightIter->setIterator(leftValue, NULL, true, true);
		break;
	case GE_OP:
		rightIter->setIterator(NULL, leftValue, true, true);
		break;
	case NE_OP:
		rightIter->setIterator(NULL, NULL, true, true);
		break;
	default:
		rightIter->setIterator(NULL, NULL, true, true);
	}
}
RC INLJoin::getAttributeValue(char *data,
		char *attrData, const vector<Attribute> &attrs,
		const string &conditionAttr) {
	char *val = data;
	for (Attribute attr : attrs) {
		if (attr.name == conditionAttr) {
			copyData(attrData, val, attr.type);
			return SUCC;
		}
		movePointer(val, attr.type);
	}
	return QE_FAIL_TO_FIND_CONDITION_ATTRIBUTE;
}

// For attribute in vector<Attribute>, name it as rel.attr
void INLJoin::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->attrs;
};
