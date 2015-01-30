
#ifndef _BUF_H
#define _BUF_H

#include "db.h"
#include "page.h"
#include "frame.h"
#include "replacer.h"
#include "hash.h"

class BufMgr 
{
	private:

		HashTable *hashTable;
		ClockFrame **frames;
		Replacer *replacer;
		int   numOfBuf;

		int FindFrame( PageID pid );
		int totalCall;
		int totalHit;
		int pages;

	public:

		BufMgr( int bufsize );
		~BufMgr();      
		Status PinPage( PageID pid, Page*& page, Bool emptyPage=FALSE );
		Status UnpinPage( PageID pid, Bool dirty=FALSE );
		Status NewPage( PageID& pid, Page*& firstpage,int howmany=1 ); 
		Status FreePage( PageID pid ); 
		Status FlushPage( PageID pid );
		Status FlushAllPages();
		int  GetStat() { return pages; }
		void   ResetStat() { pages = 0; totalHit = 0; totalCall = 0; }

		unsigned int GetNumOfBuffers();
		unsigned int GetNumOfUnpinnedBuffers();
};


#endif // _BUF_H
