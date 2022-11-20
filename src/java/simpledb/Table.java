package simpledb;

import java.util.ArrayList;

public class Table {
	
	private String name;
	private TupleDesc td;
	private DbFile file;
	private String pkey;
	private int id;
	
	public Table(DbFile file, String name, String pkey, int id) {
		this.file = file;
		this.name = name;
		this.pkey = pkey;
		this.id = id;
		this.td = file.getTupleDesc();
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public TupleDesc getTd() {
		return td;
	}
	public void setTd(TupleDesc td) {
		this.td = td;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public DbFile getFile() {
		return file;
	}
	public void setFile(DbFile file) {
		this.file = file;
	}
	public String getPkey() {
		return pkey;
	}
	public void setPkey(String pkey) {
		this.pkey = pkey;
	}
	
	public String toString() {
		return "(" + id + ")" + name;
	}
}
