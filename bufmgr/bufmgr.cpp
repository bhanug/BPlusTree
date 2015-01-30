
#include "bufmgr.h"
#include "frame.h"
#include "hash.h"

static int breakpoint()
{
	return 2;
}

//--------------------------------------------------------------------
// Constructor for BufMgr
//
// Input   : bufSize  - number of pages in the this buffer manager
//           replacer - a pointer to a Replacer object, to be used by
//                      this buffer manager to pick a victim to be
//                      replaced.
// Output  : None
//--------------------------------------------------------------------

BufMgr::BufMgr( int bufSize )
{
	frames = new ClockFrame*[bufSize];//(ClockFrame **)malloc(sizeof(ClockFrame *)*bufSize);
	for (int i = 0; i < bufSize; i++)
		frames[i] = new ClockFrame();
	hashTable = new HashTable();
	replacer = new Clock( bufSize, frames, hashTable );
	numOfBuf = bufSize;
	totalHit = 0;
	totalCall = 0;
	pages = 0;
}


//--------------------------------------------------------------------
// Destructor for BufMgr
//
// Input   : None
// Output  : None
//--------------------------------------------------------------------

BufMgr::~BufMgr()
{   
	for (int i = 0; i < numOfBuf; i++)
		delete frames[i];
	delete [] frames;
	delete replacer;
	delete hashTable;
}


//--------------------------------------------------------------------
// BufMgr::FlushAllPages
//
// Input    : None
// Output   : None
// Purpose  : Flush all pages in this buffer pool to disk.
// PreCond  : All pages in the buffer pool must not be pinned.
// PostCond : All pages in the buffer pool are written to disk.
// Return   : OK if operation is successful.  FAIL otherwise.
//--------------------------------------------------------------------

Status BufMgr::FlushAllPages()
{
	int i;
	Status s;
	int goingToFail;

	goingToFail = FALSE;
	s = OK;
	for (i = 0; s == OK && i < numOfBuf; i++)
	{
		if (frames[i]->IsValid())
		{
			if (!frames[i]->NotPinned())
			{
				goingToFail = TRUE;
			}
			s = frames[i]->Write();
		}
	}

	hashTable->EmptyIt();

	if (goingToFail)
		return FAIL;
	else
		return s;
}


//--------------------------------------------------------------------
// BufMgr::FlushPages
//
// Input    : pid  - page id of a particular page 
// Output   : None
// Purpose  : Flush the page with the given pid to disk.
// PreCond  : The page with page id = pid must not be pinned.
// PostCond : The page with page id = pid is written to disk. 
// Return   : OK if operation is successful.  FAIL otherwise.
//--------------------------------------------------------------------

Status BufMgr::FlushPage(PageID pid)
{
	int frameNo;

	frameNo = FindFrame(pid);
	if (frameNo == INVALID_PAGE)
	{
		std::cerr << "Error : Unable to find the page with page id " << pid << std::endl;
		return FAIL;
	}

	if (frames[frameNo]->NotPinned())
	{
		frames[frameNo]->Write();
		return hashTable->Delete(pid);
	}
	else
		return FAIL;
} 


//--------------------------------------------------------------------
// BufMgr::PinPage
//
// Input    : pid     - page id of a particular page 
//            isEmpty - (optional, default to FALSE) if true indicate
//                      that the page to be pinned is an empty page.
// Output   : page - a pointer to a page in the buffer pool.
// Purpose  : Pin the page with page id = pid to the buffer.  
//            Read the page from disk unless isEmpty is TRUE or unless
//            the page is already in the buffer.
// PreCond  : Either the page is already in the buffer, or there is at
//            least one frame available in the buffer pool for the 
//            page.
// PostCond : The page with page id = pid resides in the buffer and 
//            is pinned. The number of pin on the page increase by
//            one.
// Return   : OK if operation is successful.  FAIL otherwise.
//--------------------------------------------------------------------

Status BufMgr::PinPage(PageID pid, Page*& page, bool isEmpty)
{
	int frameNo;

	if (pid == 63)
		breakpoint();


	totalCall++;
	frameNo = FindFrame(pid);

	if (frameNo == INVALID_FRAME)
	{
		// Page not in buffer.
		frameNo = replacer->PickVictim();
		if (frameNo == INVALID_FRAME)
		{
			// No more place to put this page.
			std::cerr << "   Buffer is full.\n";
			return FAIL;
		}

		if (!isEmpty)
		{
			// Not empty. Read it in from Disk.
			// cerr << "pin " << pid << " miss\n";
			Status  s = frames[frameNo]->Read(pid);
			if (s != OK)
			{
				std::cerr << "  Cannot read page " << pid << std::endl;
				return FAIL;
			}
		}
		else
		{
			frames[frameNo]->SetPageID(pid);
		}
		hashTable->Insert(pid, frameNo);
	}
	else
	{
		// cerr << "pin " << pid << " hit\n";
		totalHit++;
	}

	frames[frameNo]->Pin();
	page = frames[frameNo]->GetPage();     

	return OK;
} 


//--------------------------------------------------------------------
// BufMgr::UnpinPage
//
// Input    : pid     - page id of a particular page 
// Output   : None
// Purpose  : Unpin the page with page id = pid in the buffer.  
// PreCond  : The page is already in the buffer and is pinned.
// PostCond : The page is unpinned and the number of pin on the
//            page decrease by one.
// Return   : OK if operation is successful.  FAIL otherwise.
//--------------------------------------------------------------------

Status BufMgr::UnpinPage(PageID pid, Bool dirty)
{
	int frameNo;

	if (pid == 0) {
	    breakpoint();
	}
	frameNo = FindFrame(pid);

	if (frameNo == INVALID_FRAME)
	{
		std::cerr << "   Page " << pid << " is not in the buffer\n";
		return FAIL;
	}

	if (frames[frameNo]->NotPinned())
	{
		std::cerr << "   Trying to unpin page " << pid << ", which is not pinned.\n";
		return FAIL;
	}

	if (dirty)
		frames[frameNo]->DirtyIt();

	// cerr << "unpin " << pid << "\n";
	frames[frameNo]->Unpin();
    return OK;
}


//--------------------------------------------------------------------
// BufMgr::FreePage
//
// Input    : pid     - page id of a particular page 
// Output   : None
// Purpose  : Free the memory allocated for the page with 
//            page id = pid  
// PreCond  : Either the page is already in the buffer and is pinned
//            no more than once, or the page is not in the buffer.
// PostCond : The page is unpinned, and the frame where it resides in
//            the buffer pool is freed.  Also the page is deallocated
//            from the database. 
// Return   : OK if operation is successful.  FAIL otherwise.
// Note     : You can called MINIBASE_DB->DeallocatePage(pid) to
//            deallocate a page.
//--------------------------------------------------------------------

Status BufMgr::FreePage(PageID pid)
{
	int frameNo;
	Status s;

	pages--;
	frameNo = FindFrame(pid);
	if (frameNo == INVALID_FRAME)
	{
		return MINIBASE_DB->DeallocatePage(pid);
	}
	else
	{
		s = frames[frameNo]->Free();
		if (s == OK)
			hashTable->Delete(pid);
		return s;
	}
}


//--------------------------------------------------------------------
// BufMgr::NewPage
//
// Input    : howMany - (optional, default to 1) how many pages to 
//                      allocate.
// Output   : pid  - the page id of the first page (as output by
//                   DB::AllocatePage) allocated.
//            page - a pointer to the page in memory.
// Purpose  : Allocate howMany number of pages, and pin the first page
//            into the buffer. 
// PreCond  : howMany > 0 and there is at least one free buffer space
//            to hold a page.
// PostCond : The page with page id = pid is pinned into the buffer.
// Return   : OK if operation is successful.  FAIL otherwise.
// Note     : You can call DB::AllocatePage() to allocate a page.  
//            You should call DB:DeallocatePage() to deallocate the
//            pages you allocate if you failed to pin the page in the
//            buffer.
//--------------------------------------------------------------------

Status BufMgr::NewPage (int& pid, Page*& page, int howMany)
{
	Status s;

 	s = MINIBASE_DB->AllocatePage(pid, howMany);
	if (s != OK)
	{
		std::cerr << "  BufMgr :: Unable to allocate " << howMany << " pages\n";
		return FAIL;
	}
	pages++;
	return PinPage(pid, page, TRUE) ;
}


//--------------------------------------------------------------------
// BufMgr::GetNumOfUnpinnedBuffers
//
// Input    : None
// Output   : None
// Purpose  : Find out how many unpinned locations are in the buffer
//            pool.
// PreCond  : None
// PostCond : None
// Return   : The number of unpinned buffers in the buffer pool.
//--------------------------------------------------------------------

unsigned int BufMgr::GetNumOfUnpinnedBuffers()
{
	int i;
	int count;

	count = 0;

	for (i = 0; i < numOfBuf; i++)
	{
		if (frames[i]->NotPinned())
			count ++;
	}

	return count;
}


//--------------------------------------------------------------------
// BufMgr::GetNumOfBuffers
//
// Input    : None
// Output   : None
// Purpose  : Find out how many buffers are there in the buffer pool.
// PreCond  : None
// PostCond : None
// Return   : The number of buffers in the buffer pool.
//--------------------------------------------------------------------

unsigned int BufMgr::GetNumOfBuffers()
{
    return numOfBuf;
}

int BufMgr::FindFrame( PageID pid )
{
	return hashTable->LookUp(pid);	
}

