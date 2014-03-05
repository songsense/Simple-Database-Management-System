
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
		if (rc == RM_EOF || rc == IX_EOF || rc == QE_EOF) {
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
		switch(attr.type) {
		case TypeInt:
			lhs_value += sizeof(int);
			break;
		case TypeReal:
			lhs_value += sizeof(float);
			break;
		case TypeVarChar:
			len = *((int *)lhs_value);
			lhs_value += (len + sizeof(int));
		}
	}


	int lhs_int, rhs_int;
	float lhs_float, rhs_float;
	string lhs_str, rhs_str;
	switch(type) {
	case TypeInt:
		lhs_int = *((int *)lhs_value);
		rhs_int = *((int *)value);
		return compareValueTemplate(lhs_int, rhs_int);
		break;
	case TypeReal:
		lhs_float = *((float *)lhs_value);
		rhs_float = *((float *)value);
		return compareValueTemplate(lhs_float, rhs_float);
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
		return compareValueTemplate(lhs_str, rhs_str);
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
	if (rc == RM_EOF || rc == IX_EOF || rc == QE_EOF) {
		return rc;
	}

	char *returnData = (char*)data;
	char *obtainData = (char*)tempData;
	int len = 0;
	// now check if the attribute exists
	for (Attribute attr : originAttrs) {
		if (checkExistAttrNames.count(attr.name) > 0) {
			// contains the attribute. copy it to the data
			copyData(returnData, obtainData, attr.type);
			// move pointer
			switch(attr.type) {
			case TypeInt:
				returnData += sizeof(int);
				break;
			case TypeReal:
				returnData += sizeof(float);
				break;
			case TypeVarChar:
				len = *((int *)returnData);
				returnData += (sizeof(int) + len);
				break;
			}
		}
		// move pointer
		switch(attr.type) {
		case TypeInt:
			obtainData += sizeof(int);
			break;
		case TypeReal:
			obtainData += sizeof(float);
			break;
		case TypeVarChar:
			len = *((int *)obtainData);
			obtainData += (sizeof(int) + len);
			break;
		}
	}
	return SUCC;
}
// For attribute in vector<Attribute>, name it as rel.attr
void Project::getAttributes(vector<Attribute> &attrs) const{
	attrs.clear();
	attrs = this->attrs;
};

void Project::copyData(void *dest, const void *src, const AttrType &type) {
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
