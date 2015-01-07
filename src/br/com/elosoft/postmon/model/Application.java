package br.com.elosoft.postmon.model;

public class Application {

	private Integer id;
	private String name;
	private String systemId;
	private Integer type;

	public Integer getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public String getSystemId() {
		return systemId;
	}

	public Integer getType() {
		return type;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setSystemId(String systemId) {
		this.systemId = systemId;
	}

	public void setType(Integer type) {
		this.type = type;
	}

}
