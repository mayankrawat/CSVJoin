package com.mkjoin.metadeta.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Entity implements Serializable{

	private static final long serialVersionUID = 720236087191704193L;
	
	private String filePath;
	private String entityName;
	private List<String> primaryKeys;
	private List<Entity> childEntities;
	private List<String> fkColumns;
	private Entity parentEntity;
	private String joinType;
	

	public void addToChildEntities(Entity e) {
		if (childEntities == null) {
			childEntities = new ArrayList<Entity>();
		}
		childEntities.add(e);
	}

	public String getEntityName() {
		return entityName;
	}

	public void setEntityName(String entityName) {
		this.entityName = entityName;
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public List<Entity> getChildEntities() {
		return childEntities;
	}

	public void setChildEntities(List<Entity> childEntities) {
		this.childEntities = childEntities;
	}

	public List<String> getFkColumns() {
		return fkColumns;
	}

	public void setFkColumns(List<String> fkColumns) {
		this.fkColumns = fkColumns;
	}

	public Entity getParentEntity() {
		return parentEntity;
	}

	public void setParentEntity(Entity parentEntity) {
		this.parentEntity = parentEntity;
	}

	public List<String> getPrimaryKeys() {
		return primaryKeys;
	}

	public void setPrimaryKeys(List<String> primaryKeys) {
		this.primaryKeys = primaryKeys;
	}

	@Override
	public String toString() {
		return this.entityName + " " + this.childEntities;
	}

	public String getJoinType() {
		return joinType;
	}

	public void setJoinType(String joinType) {
		this.joinType = joinType;
	}
}