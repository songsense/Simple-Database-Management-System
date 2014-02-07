
#include <fstream>
#include <iostream>
#include <cassert>

#include "../rbf/pfm.h"
#include "../rbf/rbfm.h"
#include "rm.h"
#include "test_util.h"

using namespace std;


int main() 
{
  cout << "test..." << endl;
  std::string str1 ("mike");
  std::string str2 ("rubby");

  if (str1.compare(str2) != 0)
    std::cout << str1 << " is not " << str2 << '\n';
  if (str1.compare(str2) < 0)
	  cout << str1 << " is smaller than " << str2 << endl;
  if (str1.compare(str2) <= 0)
  	  cout << str1 << " is less or equal than " << str2 << endl;
  if (str1.compare(str2) > 0)
  	  cout << str1 << " is greater than " << str2 << endl;
  if (str1.compare(str2) >= 0)
  	  cout << str1 << " is greater than " << str2 << endl;
  return 0;
  cout << "O.K." << endl;
}

