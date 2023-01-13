package simpledb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {
	private HeapFile hf;
	private int currentPage;
	private HeapPage currentHeapPage;
	private HeapPageId currentHPId;
	private Iterator<Tuple> currentPageIt;
	private TransactionId tid;
	
	
	public HeapFileIterator(TransactionId tid, HeapFile hf) {
		currentPage = -1;
		this.hf = hf;
		this.tid = tid;
	}
	
	public void open() throws DbException {
		currentPage = 0;
		this.currentHPId = new HeapPageId(this.hf.getId(), currentPage);
		
		try {
    		File f = hf.getFile();
    		
    		FileInputStream fis = new FileInputStream(f);
    		byte[] fileInBytes = new byte[(int) f.length()];
    		fis.read(fileInBytes);
    		fis.close();
    		this.currentHeapPage = new HeapPage(this.currentHPId, fileInBytes);
    		this.currentPageIt = this.currentHeapPage.iterator();
    		this.currentPage = 0;
		}
		catch(FileNotFoundException e) {
			throw new DbException(e.toString());
		}
		catch(IOException e) {
			throw new DbException(e.toString());
		}
		

	}
	
	public boolean hasNext() throws DbException{
		if (this.currentPage == -1)
			return false;

		while(this.currentPage < this.hf.numPages()) {
			if(currentPageIt.hasNext() == false) {
				this.currentPage++;
				
				this.currentHPId = new HeapPageId(this.hf.getId(), this.currentPage);
				
	    		try {
	        		File f = hf.getFile();
	        		
	        		FileInputStream fis = new FileInputStream(f);
	        		byte[] fileInBytes = new byte[(int) f.length()];
	        		fis.read(fileInBytes);
	        		fis.close();
	        		this.currentHeapPage = new HeapPage(this.currentHPId, fileInBytes);
	        		this.currentPageIt = this.currentHeapPage.iterator();		
	    		}
	    		catch(FileNotFoundException e){
	    			throw new DbException(e.toString());
	    		}
	    		catch(IOException e){
	    			throw new DbException(e.toString());
	    		}
			}
			else {
				return true;
			}
		}
		
		
		return false;
		
	}

	public Tuple next() throws DbException, NoSuchElementException{
		
		if(!this.hasNext()) {
			throw new NoSuchElementException("NO SUCH ELEMENT");
		}
		
		if(this.currentPage == -1) {
			throw new DbException("NOT OPENED");
		}
		
		return (Tuple) this.currentPageIt.next();
	}
	
	public void rewind() {
		//this.currentPage = 0;
		this.close();
		try {
			this.open();
		} catch (DbException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void close() {
		this.currentPage = -1;
		this.currentPageIt = null;
	}
	
}