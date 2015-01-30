
#include "clockframe.h"

ClockFrame::ClockFrame() : Frame()
{
	referenced = FALSE;
}

ClockFrame::~ClockFrame()
{

}

void ClockFrame::Unpin()
{
	Frame::Unpin();
	if (NotPinned())
		referenced = TRUE;
}

Bool ClockFrame::IsVictim()
{
	return (referenced == FALSE && NotPinned());
}

Bool ClockFrame::IsReferenced()
{
	return (referenced == (Bool)TRUE);
}

Status ClockFrame::Free()
{
	if (Frame::Free() != OK)
		return FAIL;

	referenced = FALSE;
	return OK;
}

void ClockFrame::UnsetReferenced()
{
	referenced = FALSE;
}