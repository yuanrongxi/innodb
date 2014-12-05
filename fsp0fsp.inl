
UNIV_INLINE ibool fsp_descr_page(ulint page_no)
{
	if(page_no % XDES_DESCRIBED_PER_PAGE == FSP_XDES_OFFSET)
		return TRUE;
	
	return FALSE;
}