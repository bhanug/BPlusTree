#include "clockframe.h"
#include "replacer.h"


Replacer::Replacer()
{

}

Replacer::~Replacer()
{

}

Clock::Clock(int bufSize, ClockFrame **bufFrames, HashTable *table)
{
	current = 0;
	numOfBuf = bufSize;
	frames = bufFrames;
	hashTable = table;
}


Clock::~Clock()
{

}

int Clock::PickVictim()
{
	int start;
	int numOfTest;

	numOfTest = 0;
	start = current;
	
	while (numOfTest != 2*numOfBuf)
	{
		if (frames[current]->IsVictim())
		{
			if (frames[current]->IsValid())
			{
				hashTable->Delete(frames[current]->GetPageID());
				frames[current]->Write();
			}
			//cerr << "  Replacing " << current << endl;
			return current;
		}
		else if (frames[current]->IsReferenced())
			frames[current]->UnsetReferenced();

		current++;
		if (current == numOfBuf)
			current = 0;
		numOfTest++;

	}

	// if reach here then no free buffer;

	return INVALID_FRAME;
}