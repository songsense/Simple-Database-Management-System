
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
        ) : blockBuffer(numPages, leftIn),
        		blockBufferRight(numPages, rightIn) {
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
//		rc = rightIter->getNextTuple(curRightValue);
		rc = blockBufferRight.getNextTuple(curRightValue);
		if (rc != SUCC) {
			// need to load more outer data
			needLoadNextLeftValue = true;
			// reset the iterator, rewind to the begining
			rightIter->setIterator();
			// retry load data
//			rc = rightIter->getNextTuple(curRightValue);
			rc = blockBufferRight.getNextTuple(curRightValue);
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
//			rc = leftIter->getNextTuple(curLeftValue);
			rc = blockBuffer.getNextTuple(curLeftValue);
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
 * 				INLJoin
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
        ) : blockBuffer(numPages, leftIn) {
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
//				rc = leftIter->getNextTuple(curLeftValue);
				rc = blockBuffer.getNextTuple(curLeftValue);
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
/*
 *
 * 				Aggregate
 *
 */
Aggregate::Aggregate(Iterator *input,                              // Iterator of input R
          Attribute aggAttr,                            // The attribute over which we are computing an aggregate
          AggregateOp op                                // Aggregate operation
) {
	if (aggAttr.type == TypeVarChar) {
		initStatus = false;
		cerr << "Try to aggregate a char that we don't support" << endl;
		return;
	}
	initStatus = true;
	// set iterator
	iter = input;
	// set the attributes of iterator
	iter->getAttributes(attrs);
	// set aggAttr;
	this->aggAttr = aggAttr;
	// set aggMode
	aggMode = AGG_SINGLE_MODE;
	// set AggregateOp
	this->op = op;
}
Aggregate::Aggregate(Iterator *input,                              // Iterator of input R
          Attribute aggAttr,                            // The attribute over which we are computing an aggregate
          Attribute gAttr,                              // The attribute over which we are grouping the tuples
          AggregateOp op                                // Aggregate operation
){
	if (aggAttr.type == TypeVarChar) {
		initStatus = false;
		cerr << "Try to aggregate a char that we don't support" << endl;
		return;
	}
	initStatus = true;
	// set iterator
	iter = input;
	// set the attributes of iterator
	iter->getAttributes(attrs);
	// set aggAttr;
	this->aggAttr = aggAttr;
	// set gAttr
	this->gAttr = gAttr;
	// set aggMode
	aggMode = AGG_GROUP_MODE;
	// set AggregateOp
	this->op = op;
	switch(op) {
	case MIN:
		prepareGroupMin();
		break;
	case MAX:
		prepareGroupMax();
		break;
	case SUM:
		prepareGroupSum();
		break;
	case AVG:
		prepareGroupAvg();
		break;
	case COUNT:
		prepareGroupCount();
	}
}

RC Aggregate::getNextTuple(void *data){
	if (initStatus == false) {
		return QE_EOF;
	}

	if (aggMode == AGG_SINGLE_MODE) {
		getNextTuple_single(data);
		initStatus = false;
		return SUCC;
	} else if (aggMode == AGG_GROUP_MODE) {
		RC rc;
		switch(op) {
		case MIN:
		case MAX:
		case SUM:
			rc = getNextTuple_groupMaxMinSum(data);
			break;
		case AVG:
			rc = getNextTuple_groupAvg(data);
			break;
		case COUNT:
			rc = getNextTuple_groupCount(data);
		}
		return rc;
	}

	return SUCC;
}
void Aggregate::getAttributes(vector<Attribute> &attrs) const{
	Attribute attr = aggAttr;
	switch(op) {
	case MIN:
		attr.name = "MIN(";
		break;
	case MAX:
		attr.name = "MAX(";
		break;
	case SUM:
		attr.name = "SUM(";
		break;
	case AVG:
		attr.name = "AVG(";
		break;
	case COUNT:
		attr.name = "COUNT(";
		break;
	}
	attr.name += (aggAttr.name + ")");

	attrs.clear();
	attrs.push_back(attr);
}
void Aggregate::getNextTuple_single(void *data) {
	switch(op) {
	case MIN:
		singleMin(data);
		break;
	case MAX:
		singleMax(data);
		break;
	case SUM:
		singleSum(data);
		break;
	case AVG:
		singleAvg(data);
		break;
	case COUNT:
		singleCount(data);
	}
}
RC Aggregate::getNextTuple_groupMaxMinSum(void *data) {
	char *returnData = (char *)data;
	if (aggAttr.type == TypeInt) {
		if (gAttr.type == TypeInt) {
			if (group_int_int.empty()) {
				return QE_EOF;
			} else {
				auto itr = group_int_int.begin();
				int id = itr->first;
				int val = itr->second;
				copyData(returnData, &id, gAttr.type);
				copyData(returnData+sizeof(int), &val, aggAttr.type);
				group_int_int.erase(itr);
			}
		} else if (gAttr.type == TypeReal) {
			if (group_float_int.empty()) {
				return QE_EOF;
			} else {
				auto itr = group_float_int.begin();
				float id = itr->first;
				int val = itr->second;
				copyData(returnData, &id, gAttr.type);
				copyData(returnData+sizeof(float), &val, aggAttr.type);
				group_float_int.erase(itr);
			}
		} else {
			if (group_string_int.empty()) {
				return QE_EOF;
			} else {
				auto itr = group_string_int.begin();
				string id = itr->first;
				int id_len = id.size();
				int val = itr->second;
				memcpy(returnData, &id_len, sizeof(int));
				memcpy(returnData+sizeof(int), id.c_str(), id_len);
				copyData(returnData+sizeof(int)+id_len, &val, aggAttr.type);
				group_string_int.erase(itr);
			}
		}
	} else if (aggAttr.type == TypeReal) {
		if (gAttr.type == TypeInt) {
			if (group_int_float.empty()) {
				return QE_EOF;
			} else {
				auto itr = group_int_float.begin();
				int id = itr->first;
				float val = itr->second;
				copyData(returnData, &id, gAttr.type);
				copyData(returnData+sizeof(int), &val, aggAttr.type);
				group_int_float.erase(itr);
			}
		} else if (gAttr.type == TypeReal) {
			if (group_float_float.empty()) {
				return QE_EOF;
			} else {
				auto itr = group_float_float.begin();
				float id = itr->first;
				float val = itr->second;
				copyData(returnData, &id, gAttr.type);
				copyData(returnData+sizeof(float), &val, aggAttr.type);
				group_float_float.erase(itr);
			}
		} else {
			if (group_string_float.empty()) {
				return QE_EOF;
			} else {
				auto itr = group_string_float.begin();
				string id = itr->first;
				int id_len = id.size();
				float val = itr->second;
				memcpy(returnData, &id_len, sizeof(int));
				memcpy(returnData+sizeof(int), id.c_str(), id_len);
				copyData(returnData+sizeof(int)+id_len, &val, aggAttr.type);
				group_string_float.erase(itr);
			}
		}
	} else {
		cerr << "Try to aggregate a char that we don't support" << endl;
	}
	return SUCC;
}
RC Aggregate::getNextTuple_groupAvg(void *data) {
	char *returnData = (char *)data;
	if (gAttr.type == TypeInt) {
		if (group_int_int.empty()) {
			return QE_EOF;
		} else {
			auto itr_1 = group_int_int.begin();
			int id = itr_1->first;
			int cnt = itr_1->second;
			auto itr_2 = group_int_float.begin();
			float sum =  itr_2->second;
			float avg = sum / (float)cnt;
			copyData(returnData, &id, gAttr.type);
			copyData(returnData+sizeof(int), &avg, aggAttr.type);
			group_int_int.erase(itr_1);
			group_int_float.erase(itr_2);
		}
	} else if (gAttr.type == TypeReal) {
		if (group_float_int.empty()) {
			return QE_EOF;
		} else {
			auto itr_1 = group_float_int.begin();
			int id = itr_1->first;
			int cnt = itr_1->second;
			auto itr_2 = group_float_float.begin();
			float sum =  itr_2->second;
			float avg = sum / (float)cnt;
			copyData(returnData, &id, gAttr.type);
			copyData(returnData+sizeof(int), &avg, aggAttr.type);
			group_float_int.erase(itr_1);
			group_float_float.erase(itr_2);
		}
	} else {
		if (group_string_int.empty()) {
			return QE_EOF;
		} else {
			auto itr_1 = group_string_int.begin();
			string id = itr_1->first;
			int id_len = id.size();
			int cnt = itr_1->second;
			auto itr_2 = group_string_float.begin();
			float sum =  itr_2->second;
			float avg = sum / (float)cnt;
			memcpy(returnData, &id_len, sizeof(int));
			memcpy(returnData+sizeof(int), id.c_str(), id_len);
			copyData(returnData+sizeof(int)+id_len, &avg, aggAttr.type);
			group_string_int.erase(itr_1);
			group_string_float.erase(itr_2);
		}
	}
	return SUCC;
}
RC Aggregate::getNextTuple_groupCount(void *data) {
	char *returnData = (char *)data;
	if (gAttr.type == TypeInt) {
		if (group_int_int.empty()) {
			return QE_EOF;
		} else {
			auto itr = group_int_int.begin();
			int id = itr->first;
			int val = itr->second;
			copyData(returnData, &id, gAttr.type);
			copyData(returnData+sizeof(int), &val, aggAttr.type);
			group_int_int.erase(itr);
		}
	} else if (gAttr.type == TypeReal) {
		if (group_float_int.empty()) {
			return QE_EOF;
		} else {
			auto itr = group_float_int.begin();
			float id = itr->first;
			int val = itr->second;
			copyData(returnData, &id, gAttr.type);
			copyData(returnData+sizeof(float), &val, aggAttr.type);
			group_float_int.erase(itr);
		}
	} else {
		if (group_string_int.empty()) {
			return QE_EOF;
		} else {
			auto itr = group_string_int.begin();
			string id = itr->first;
			int id_len = id.size();
			int val = itr->second;
			memcpy(returnData, &id_len, sizeof(int));
			memcpy(returnData+sizeof(int), id.c_str(), id_len);
			copyData(returnData+sizeof(int)+id_len, &val, aggAttr.type);
			group_string_int.erase(itr);
		}
	}
	return SUCC;
}
void Aggregate::prepareGroupMax() {
	while (iter->getNextTuple(readValue) != QE_EOF) {
		char *aggValue, *gValue;
		char *value = readValue;
		int hit = 0;
		// find pointer to aggValue and gValue
		for (Attribute attr : attrs) {
			if (hit >= 2) {
				// find all. no need to continue
				break;
			}
			if (attr.name == aggAttr.name) {
				aggValue = value;
				++hit;
			}
			if (attr.name == gAttr.name) {
				gValue = value;
				++hit;
			}
			// move pointer
			movePointer(value, attr.type);
		}

		if (aggAttr.type == TypeInt) {
			int aggVal = *((int *)aggValue);
			if (gAttr.type == TypeInt) {
				int gVal = *((int *)gValue);
				groupMax(group_int_int, gVal, aggVal);
			} else if (gAttr.type == TypeReal) {
				float gVal = *((float *)gValue);
				groupMax(group_float_int, gVal, aggVal);
			} else {
				int len = *((int *)gValue);
				memcpy(str, gValue + sizeof(int), len);
				str[len] = '\0';
				string gVal(str);
				groupMax(group_string_int, gVal, aggVal);
			}
		} else if (aggAttr.type == TypeReal) {
			float aggVal = *((float *)aggValue);
			if (gAttr.type == TypeInt) {
				int gVal = *((int *)gValue);
				groupMax(group_int_float, gVal, aggVal);
			} else if (gAttr.type == TypeReal) {
				float gVal = *((float *)gValue);
				groupMax(group_float_float, gVal, aggVal);
			} else {
				int len = *((int *)gValue);
				memcpy(str, gValue + sizeof(int), len);
				str[len] = '\0';
				string gVal(str);
				groupMax(group_string_float, gVal, aggVal);
			}
		} else {
			cerr << "Try to aggregate a char that we don't support" << endl;
		}
	}
}
void Aggregate::prepareGroupMin() {
	while (iter->getNextTuple(readValue) != QE_EOF) {
		char *aggValue, *gValue;
		char *value = readValue;
		int hit = 0;
		// find pointer to aggValue and gValue
		for (Attribute attr : attrs) {
			if (hit >= 2) {
				// find all. no need to continue
				break;
			}
			if (attr.name == aggAttr.name) {
				aggValue = value;
				++hit;
			}
			if (attr.name == gAttr.name) {
				gValue = value;
				++hit;
			}
			// move pointer
			movePointer(value, attr.type);
		}

		if (aggAttr.type == TypeInt) {
			int aggVal = *((int *)aggValue);
			if (gAttr.type == TypeInt) {
				int gVal = *((int *)gValue);
				groupMin(group_int_int, gVal, aggVal);
			} else if (gAttr.type == TypeReal) {
				float gVal = *((float *)gValue);
				groupMin(group_float_int, gVal, aggVal);
			} else {
				int len = *((int *)gValue);
				memcpy(str, gValue + sizeof(int), len);
				str[len] = '\0';
				string gVal(str);
				groupMin(group_string_int, gVal, aggVal);
			}
		} else if (aggAttr.type == TypeReal) {
			float aggVal = *((float *)aggValue);
			if (gAttr.type == TypeInt) {
				int gVal = *((int *)gValue);
				groupMin(group_int_float, gVal, aggVal);
			} else if (gAttr.type == TypeReal) {
				float gVal = *((float *)gValue);
				groupMin(group_float_float, gVal, aggVal);
			} else {
				int len = *((int *)gValue);
				memcpy(str, gValue + sizeof(int), len);
				str[len] = '\0';
				string gVal(str);
				groupMin(group_string_float, gVal, aggVal);
			}
		} else {
			cerr << "Try to aggregate a char that we don't support" << endl;
		}
	}
}
void Aggregate::prepareGroupSum() {
	while (iter->getNextTuple(readValue) != QE_EOF) {
		char *aggValue, *gValue;
		char *value = readValue;
		int hit = 0;
		// find pointer to aggValue and gValue
		for (Attribute attr : attrs) {
			if (hit >= 2) {
				// find all. no need to continue
				break;
			}
			if (attr.name == aggAttr.name) {
				aggValue = value;
				++hit;
			}
			if (attr.name == gAttr.name) {
				gValue = value;
				++hit;
			}
			// move pointer
			movePointer(value, attr.type);
		}

		if (aggAttr.type == TypeInt) {
			int aggVal = *((int *)aggValue);
			if (gAttr.type == TypeInt) {
				int gVal = *((int *)gValue);
				groupSum(group_int_int, gVal, aggVal);
			} else if (gAttr.type == TypeReal) {
				float gVal = *((float *)gValue);
				groupSum(group_float_int, gVal, aggVal);
			} else {
				int len = *((int *)gValue);
				memcpy(str, gValue + sizeof(int), len);
				str[len] = '\0';
				string gVal(str);
				groupSum(group_string_int, gVal, aggVal);
			}
		} else if (aggAttr.type == TypeReal) {
			float aggVal = *((float *)aggValue);
			if (gAttr.type == TypeInt) {
				int gVal = *((int *)gValue);
				groupSum(group_int_float, gVal, aggVal);
			} else if (gAttr.type == TypeReal) {
				float gVal = *((float *)gValue);
				groupSum(group_float_float, gVal, aggVal);
			} else {
				int len = *((int *)gValue);
				memcpy(str, gValue + sizeof(int), len);
				str[len] = '\0';
				string gVal(str);
				groupSum(group_string_float, gVal, aggVal);
			}
		} else {
			cerr << "Try to aggregate a char that we don't support" << endl;
		}
	}
}
void Aggregate::prepareGroupAvg() {
	while (iter->getNextTuple(readValue) != QE_EOF) {
		char *aggValue, *gValue;
		char *value = readValue;
		int hit = 0;
		// find pointer to aggValue and gValue
		for (Attribute attr : attrs) {
			if (hit >= 2) {
				// find all. no need to continue
				break;
			}
			if (attr.name == aggAttr.name) {
				aggValue = value;
				++hit;
			}
			if (attr.name == gAttr.name) {
				gValue = value;
				++hit;
			}
			// move pointer
			movePointer(value, attr.type);
		}

		if (aggAttr.type == TypeInt) {
			float aggVal = (float)*((int *)aggValue);
			if (gAttr.type == TypeInt) {
				int gVal = *((int *)gValue);
				groupAvg(group_int_float, group_int_int, gVal, aggVal);
			} else if (gAttr.type == TypeReal) {
				float gVal = *((float *)gValue);
				groupAvg(group_float_float, group_float_int, gVal, aggVal);
			} else {
				int len = *((int *)gValue);
				memcpy(str, gValue + sizeof(int), len);
				str[len] = '\0';
				string gVal(str);
				groupAvg(group_string_float, group_string_int, gVal, aggVal);
			}
		} else if (aggAttr.type == TypeReal) {
			float aggVal = *((float *)aggValue);
			if (gAttr.type == TypeInt) {
				int gVal = *((int *)gValue);
				groupAvg(group_int_float, group_int_int, gVal, aggVal);
			} else if (gAttr.type == TypeReal) {
				float gVal = *((float *)gValue);
				groupAvg(group_float_float, group_float_int, gVal, aggVal);
			} else {
				int len = *((int *)gValue);
				memcpy(str, gValue + sizeof(int), len);
				str[len] = '\0';
				string gVal(str);
				groupAvg(group_string_float, group_string_int, gVal, aggVal);
			}
		} else {
			cerr << "Try to aggregate a char that we don't support" << endl;
		}
	}
}
void Aggregate::prepareGroupCount() {
	while (iter->getNextTuple(readValue) != QE_EOF) {
		char *aggValue, *gValue;
		char *value = readValue;
		int hit = 0;
		// find pointer to aggValue and gValue
		for (Attribute attr : attrs) {
			if (hit >= 2) {
				// find all. no need to continue
				break;
			}
			if (attr.name == aggAttr.name) {
				aggValue = value;
				++hit;
			}
			if (attr.name == gAttr.name) {
				gValue = value;
				++hit;
			}
			// move pointer
			movePointer(value, attr.type);
		}

		if (aggAttr.type == TypeInt) {
			int aggVal = *((int *)aggValue);
			if (gAttr.type == TypeInt) {
				int gVal = *((int *)gValue);
				groupCount(group_int_int, gVal);
			} else if (gAttr.type == TypeReal) {
				float gVal = *((float *)gValue);
				groupCount(group_float_int, gVal);
			} else {
				int len = *((int *)gValue);
				memcpy(str, gValue + sizeof(int), len);
				str[len] = '\0';
				string gVal(str);
				groupCount(group_string_int, gVal);
			}
		} else if (aggAttr.type == TypeReal) {
			float aggVal = *((float *)aggValue);
			if (gAttr.type == TypeInt) {
				int gVal = *((int *)gValue);
				groupCount(group_int_int, gVal);
			} else if (gAttr.type == TypeReal) {
				float gVal = *((float *)gValue);
				groupCount(group_float_int, gVal);
			} else {
				int len = *((int *)gValue);
				memcpy(str, gValue + sizeof(int), len);
				str[len] = '\0';
				string gVal(str);
				groupCount(group_string_int, gVal);
			}
		} else {
			cerr << "Try to aggregate a char that we don't support" << endl;
		}
	}
}
void Aggregate::singleMax(void *data) {
	float val_float = -FLT_MAX;
	int val_int = INT_MIN;

	while (iter->getNextTuple(readValue) != QE_EOF) {
		char *value = readValue;
		for (Attribute attr : attrs) {
			if (attr.name == aggAttr.name) {
				break;
			}
			// move pointer
			movePointer(value, attr.type);
		}

		switch(aggAttr.type) {
		case TypeInt:
			if (val_int < *((int *)value)) {
				val_int = *((int *)value);
			}
			break;
		case TypeReal:
			if (val_float < *((float *)value)) {
				val_float = *((float *)value);
			}
			break;
		default:
			cerr << "Try to aggregate a char that we don't support" << endl;
		}
	}
	switch(aggAttr.type) {
	case TypeInt:
		memcpy(data, &val_int, sizeof(int));
		break;
	case TypeReal:
		memcpy(data, &val_float, sizeof(float));
		break;
	default:
		cerr << "Try to aggregate a char that we don't support" << endl;
	}
}
void Aggregate::singleMin(void *data) {
	float val_float = FLT_MAX;
	int val_int = INT_MAX;

	while (iter->getNextTuple(readValue) != QE_EOF) {
		char *value = readValue;
		for (Attribute attr : attrs) {
			if (attr.name == aggAttr.name) {
				break;
			}
			// move pointer
			movePointer(value, attr.type);
		}

		switch(aggAttr.type) {
		case TypeInt:
			if (val_int > *((int *)value)) {
				val_int = *((int *)value);
			}
			break;
		case TypeReal:
			if (val_float > *((float *)value)) {
				val_float = *((float *)value);
			}
			break;
		default:
			cerr << "Try to aggregate a char that we don't support" << endl;
		}
	}
	switch(aggAttr.type) {
	case TypeInt:
		memcpy(data, &val_int, sizeof(int));
		break;
	case TypeReal:
		memcpy(data, &val_float, sizeof(float));
		break;
	default:
		cerr << "Try to aggregate a char that we don't support" << endl;
	}
}
void Aggregate::singleSum(void *data) {
	float val_float = 0.;
	int val_int = 0;

	while (iter->getNextTuple(readValue) != QE_EOF) {
		char *value = readValue;
		for (Attribute attr : attrs) {
			if (attr.name == aggAttr.name) {
				break;
			}
			// move pointer
			movePointer(value, attr.type);
		}

		switch(aggAttr.type) {
		case TypeInt:
			val_int += *((int *)value);
			break;
		case TypeReal:
			val_float += *((float *)value);
			break;
		default:
			cerr << "Try to aggregate a char that we don't support" << endl;
		}
	}
	switch(aggAttr.type) {
	case TypeInt:
		memcpy(data, &val_int, sizeof(int));
		break;
	case TypeReal:
		memcpy(data, &val_float, sizeof(float));
		break;
	default:
		cerr << "Try to aggregate a char that we don't support" << endl;
	}
}
void Aggregate::singleAvg(void *data) {
	float val_float = 0.;
	int val_int = 0;
	float avg = 0;
	float count = 0.;

	while (iter->getNextTuple(readValue) != QE_EOF) {
		char *value = readValue;
		for (Attribute attr : attrs) {
			if (attr.name == aggAttr.name) {
				break;
			}
			// move pointer
			movePointer(value, attr.type);
		}

		switch(aggAttr.type) {
		case TypeInt:
			val_int += *((int *)value);
			break;
		case TypeReal:
			val_float += *((float *)value);
			break;
		default:
			cerr << "Try to aggregate a char that we don't support" << endl;
		}
		count += 1.0;
	}
	switch(aggAttr.type) {
	case TypeInt:
		avg = (float)val_int / count;
		break;
	case TypeReal:
		avg = val_float / count;
		break;
	default:
		cerr << "Try to aggregate a char that we don't support" << endl;
	}
	memcpy(data, &avg, sizeof(float));
}
void Aggregate::singleCount(void *data) {
	unordered_set<float> val_float;
	unordered_set<int> val_int;
	float countUniqueVal;

	while (iter->getNextTuple(readValue) != QE_EOF) {
		char *value = readValue;
		for (Attribute attr : attrs) {
			if (attr.name == aggAttr.name) {
				break;
			}
			// move pointer
			movePointer(value, attr.type);
		}

		switch(aggAttr.type) {
		case TypeInt:
			val_int.insert(*((int *)value));
			break;
		case TypeReal:
			val_float.insert(*((float *)value));
			break;
		default:
			cerr << "Try to aggregate a char that we don't support" << endl;
		}
	}
	switch(aggAttr.type) {
	case TypeInt:
		countUniqueVal = val_int.size();
		break;
	case TypeReal:
		countUniqueVal = val_float.size();
		break;
	default:
		cerr << "Try to aggregate a char that we don't support" << endl;
	}
	memcpy(data, &countUniqueVal, sizeof(float));
}

/*
 *
 * 				BlockBuffer
 *
 */
BlockBuffer::BlockBuffer(unsigned numPages, Iterator *iter) :
	size(numPages * PAGE_SIZE){
	this->numPages = numPages;

	// set buffer
	// add one more page size in case overflow
	buffer = new char[size+PAGE_SIZE];
	curBufferPointer = buffer;
	curIndex = 0;
	bufferUsage = 0;

	// set the iterator;
	this->iter = iter;

	// set attributes
	iter->getAttributes(attrs);
}
BlockBuffer::~BlockBuffer() {
	delete []buffer;
}
void BlockBuffer::loadNextBlock() {
	// set the buffer index to be the beginning of the buffer
	bufferUsage = 0;
	curIndex = 0;
	curBufferPointer = buffer;
	// clear index of buffer
	dataPositions.clear();
	dataSizes.clear();

	RC rc;

	while (true) {
		rc = iter->getNextTuple(inputBuffer);
		// if there is no more tuple in the disk
		// mark as end of tuples
		if (rc == QE_EOF) {
			break;
		}

		int recordSize = getRecordSize(inputBuffer, attrs);
		// copy to the buffer
		memcpy(curBufferPointer, inputBuffer, recordSize);
		curBufferPointer += recordSize;
		// add the data position to the index of buffer
		dataPositions.push_back(bufferUsage);
		// add the buffer size to he index of buffer
		dataSizes.push_back(recordSize);
		// update buffer size
		bufferUsage += recordSize;

		// if the buffer is full
		// mark as more tuples waiting
		if (bufferUsage >= size) {
			break;
		}
	}
}

RC BlockBuffer::getNextTuple(void *data) {
	if (curIndex == dataPositions.size()) {
		// no buffer cached, load more
		loadNextBlock();
//		cerr << "bufferUsage " << bufferUsage << endl;
//		cerr << "number of data " << dataSizes.size() << endl;
		// retry to see if there is any more
		if (curIndex == dataPositions.size()) {
			// no buffer in disk either, quit
			return QE_EOF;
		}
	}

	// good! we have data
	// get the position of data in the buffer
	unsigned pos = dataPositions[curIndex];
	// get the data size
	unsigned recordSize = dataSizes[curIndex];
	// copy the data
	memcpy(data, buffer + pos, recordSize);

	// index increment
	++curIndex;

	return SUCC;
}


