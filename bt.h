/*
* bt.h - global declarations for the B+ Tree.
*
* Gideon Glass & Johannes Gehrke  951012  CS564  UW-Madison
* Wei Tsang Ooi Spring 97 Fall 98 CS432 Cornell University
* Rimon Barr    Fall 98  CS432 Cornell University
*/ 

#ifndef BT_H
#define BT_H

#include "minirel.h"


typedef enum 
{
	INDEX_NODE,
	LEAF_NODE		
} NodeType;

struct LeafEntry {
    	int key;
	RecordID rid;
};

struct IndexEntry {
    	int key;
	PageID pid;
};

// There macros might be useful to you.

#define INSERT(page, key, data, rid) {\
	if ((page)->Insert(key, data, rid) != OK) {\
		cerr << "Unable to insert in " << __FILE__ << ":" << __LINE__;return FAIL;}}
#define DELETE(page, key, rid) {\
	if ((page)->Delete(key, rid) != OK) {\
		cerr << "Unable to delete in " << __FILE__ << ":" << __LINE__;return FAIL;}}

#endif
