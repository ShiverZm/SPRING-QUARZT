package org.dreams.fly.dao.domain;

import java.io.Serializable;

/**
 * @author book
 */
public class Example implements Serializable {

	private static final long serialVersionUID = 1L;
	
    private int id;
    
    private String name;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		return "Example [id=" + id + ", name=" + name + "]";
	}

	public Example(int id, String name) {
		super();
		this.id = id;
		this.name = name;
	}

	public Example() {
		super();
	}
    
    
    
	
}
