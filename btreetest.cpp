#include <math.h> 
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include "bufmgr.h"
#include "db.h"
#include "btfile.h"

#include "btreetest.h"

#define MAX_COMMAND_SIZE 1000

Status BTreeTest::RunTests(std::istream &in) {

	char *dbname="btdb";
	char *logname="btlog";
	char *btfname="BTreeIndex";

	remove(dbname);
	remove(logname);

	Status status;
	minibase_globals = new SystemDefs(status, dbname, logname, 1000,500,200);
	if (status != OK) {
		minibase_errors.show_errors();
		exit(1);
	}
	
	BTreeFile* btf=createIndex(btfname);
	if (btf==NULL) {
		std::cout << "Error: Unable to create index file"<< std::endl;
	}

	char command[MAX_COMMAND_SIZE];
	in >> command;
	while(in) {
		if(!strcmp(command, "insert")) {
			int high, low;
			in >> low >> high;
			insertHighLow(btf,low,high);
		} 
		else if(!strcmp(command, "scan")) {
			int high, low;
			in >> low >> high;
			scanHighLow(btf,low,high);
		}
		else if(!strcmp(command, "delete")) {
			int high, low;
			in >> low >> high;
			deleteHighLow(btf,low,high);
		}
		else if(!strcmp(command, "deletescan")) {
			int high, low;
			in >> low >> high;
			deleteScanHighLow(btf,low,high);
		}
		else if(!strcmp(command, "print")) {
			btf->Print();
		}
		else if(!strcmp(command, "stats")) {
			btf->DumpStatistics();
		}
		else if(!strcmp(command, "quit")) {
			break;
		}
		else {
			std::cout << "Error: Unrecognized command: "<<command<< std::endl;
		}
		in >> command;
	}

	destroyIndex(btf, btfname);

	delete minibase_globals;
	remove(dbname);
	remove(logname);

	return OK;
}


BTreeFile *BTreeTest::createIndex(char *name) {
    std::cout << "Create B+tree." << std::endl;
    std::cout << "  Page size="<<MINIBASE_PAGESIZE<< " Max space="<<MAX_SPACE<<std::endl;
	
    Status status;
    BTreeFile *btf = new BTreeFile(status, name);
    if (status != OK) {
        minibase_errors.show_errors();
        std::cout << "  Error: can not open index file."<<std::endl;
		if (btf!=NULL)
			delete btf;
		return NULL;
    }
    std::cout<<"  Success."<<std::endl;
	return btf;
}


void BTreeTest::destroyIndex(BTreeFile *btf, char *name) {
	std::cout << "Destroy B+tree."<<std::endl;
	Status status = btf->DestroyFile();
	if (status != OK)
		minibase_errors.show_errors();
    delete btf;
}


void BTreeTest::insertHighLow(BTreeFile *btf, int low, int high) {
	int numkey=high-low+1;
	std::cout << "Inserting: ("<<low<<" to "<<high<<")"<<std::endl;
    for (int i=0; i<numkey; i++) {
		RecordID rid;
        rid.pageNo=i; rid.slotNo=i+1;
		int key = low + i; // rand() % numkey;
		std::cout << "  Insert: "<<key<<" @[pg,slot]=["<<rid.pageNo<<","<<rid.slotNo<<"]"<<std::endl;
        if (btf->Insert(key, rid) != OK) {
			std::cout << "  Insertion failed."<< std::endl;
            minibase_errors.show_errors();
			return;
        }
    }
	std::cout << "  Success."<< std::endl;
}


void BTreeTest::scanHighLow(BTreeFile *btf, int low, int high) {
	std::cout << "Scanning ("<<low<<" to "<<high<<"):"<< std::endl;

	int *plow=&low, *phigh=&high;
	if(low==-1) plow=NULL;
	if(high==-1) phigh=NULL;

	IndexFileScan *scan = btf->OpenScan(plow, phigh);
	if(scan == NULL) {
		std::cout << "  Error: cannot open a scan." << std::endl;
		minibase_errors.show_errors();
		return;
	}
	
	RecordID rid;
	int ikey, count=0;
	Status status = scan->GetNext(rid, ikey);
	while (status == OK) {
		count++;
		std::cout<<"  Scanned @[pg,slot]=["<<rid.pageNo<<","<<rid.slotNo<<"]";
		std::cout<<" key="<<ikey<< std::endl;
		status = scan->GetNext(rid, ikey);
	}
	delete scan;
	std::cout << "  "<< count << " records found."<< std::endl;

	if (status!=DONE) {
		minibase_errors.show_errors();
		return;
	}
	std::cout << "  Success."<< std::endl;
}


void BTreeTest::deleteHighLow(BTreeFile *btf, int low, int high) {
	std::cout << "Deleting ("<<low<<"-"<<high<<"):"<< std::endl;

	int *plow=&low, *phigh=&high;
	if(low==-1) plow=NULL;
	if(high==-1) phigh=NULL;

	IndexFileScan *scan = btf->OpenScan(plow, phigh);
	if(scan == NULL) {
		std::cout << "Error: cannot open a scan." << std::endl;
		minibase_errors.show_errors();
		return;
	}
	RecordID rid;
	int ikey, count=0;
	Status status = scan->GetNext(rid, ikey);
    while (status == OK) {
		std::cout << "  Delete [pg,slot]=["<<rid.pageNo<<","<<rid.slotNo<<"]";
		std::cout << " key="<<ikey<< std::endl;
		delete scan;
		scan=NULL;
		if ((status = btf->Delete(ikey,rid)) != OK) {
			std::cout << "  Failure to delete record...\n";
			minibase_errors.show_errors();
			break;
		}
		count++;

		scan = btf->OpenScan(plow, phigh);
		if(scan == NULL) {
			std::cout << "Error: cannot open a scan." << std::endl;
			minibase_errors.show_errors();
			break;
		}
		status = scan->GetNext(rid, ikey);
    }
	delete scan;
	std::cout << "  " << count << " records deleted."<< std::endl;

    if (status != DONE) {
		std::cout << "  Error: During delete";
		minibase_errors.show_errors();
		return;
    }
	std::cout << "  Success."<< std::endl;
}



void BTreeTest::deleteScanHighLow(BTreeFile *btf, int low, int high) {
	std::cout << "Scan/Deleting ("<<low<<"-"<<high<<"):"<< std::endl;

	int *plow=&low, *phigh=&high;
	if(low==-1) plow=NULL;
	if(high==-1) phigh=NULL;

	IndexFileScan *scan = btf->OpenScan(plow, phigh);
	if(scan == NULL) {
		std::cout << "Error: cannot open a scan." << std::endl;
		minibase_errors.show_errors();
		return;
	}

	RecordID rid;
	int ikey, count=0;
	Status status = scan->GetNext(rid, ikey);
    while (status == OK) {
		std::cout << "  DeleteCurrent [pg,slot]=["<<rid.pageNo<<","<<rid.slotNo<<"]";
		std::cout << " key="<<ikey<< std::endl;
		if ((status = scan->DeleteCurrent()) != OK) {
			std::cout << "  Failure to delete record...\n";
			minibase_errors.show_errors();
			break;
		}
		count++;
		status = scan->GetNext(rid, ikey);
    }
	delete scan;
	std::cout << "  " << count << " records scan/deleted."<< std::endl;

    if (status != DONE) {
		std::cout << "  Error: During scan/delete";
		minibase_errors.show_errors();
		return;
    }
	std::cout << "  Success."<< std::endl;
}
