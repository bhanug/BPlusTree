/*
* Rimon Barr    Fall 98  CS432 Cornell University
*/

#include <stdlib.h>
#include <iostream>

#include "btreetest.h"

int MINIBASE_RESTART_FLAG = 0;

int main(int argc, char *argv[]) {
	std::cout << "btree tester" <<std::endl;
	std::cout << "Execute 'btree ?' for info"<<std::endl<<std::endl;

	BTreeTest btt;
	if (argc==1) {
		btt.RunTests(std::cin);
	}
	else if (argc==2 && argv[1][0]!='?') {
		std::ifstream is=std::ifstream(argv[1], std::ios::in);
		if(!is.is_open()) {
			std::cout << "Error: Failed to open "<<argv[1]<<std::endl;
			return 1;
		}
		btt.RunTests(is);
	}
	else {
		std::cout << "Syntax: btree [command_file]"<<std::endl;
		std::cout << "If no file, commands read from stdin"<<std::endl<<std::endl;

		std::cout << "Commands should be of the form:"<<std::endl;
		std::cout << "insert <low> <high>"<<std::endl;
		std::cout << "scan <low> <high>"<<std::endl;
		std::cout << "delete <low> <high>"<<std::endl;
		std::cout << "print"<<std::endl;
		std::cout << "stats"<<std::endl;
		std::cout << "quit (not required)"<<std::endl;
		std::cout << "Note that (<low>==-1)=>min and (<high>==-1)=>max"<<std::endl;

		return 1;
	}
	return 0;
}

