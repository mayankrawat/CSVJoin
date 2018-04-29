package com.mkjoin.metadeta;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.mkjoin.metadeta.model.Entity;

/**
 * Class load the metadata from datasource.json and kafka.properties
 * @author Mayank Rawat
 *
 */
public class MetaDataLoader {

	private static LinkedList<JsonObject> jsonObjects = new LinkedList<JsonObject>();

	private static Entity parentEntity;
	
	private static Properties properties;

	static {
		try {
			loadDataSource();
			processEntity(parentEntity);
			producerProperties();
		} catch (JsonIOException e) {
			e.printStackTrace();
		} catch (JsonSyntaxException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void producerProperties() throws FileNotFoundException, IOException {
        properties = new Properties();
        properties.load(new FileReader(ClassLoader.getSystemResource("kafka.properties").getFile()));
	}
	
	public static Entity getParentEntity() {
		return parentEntity;
	}
	
	public static Properties getKafkaProperties() {
		return properties;
	}
	
	private static void loadDataSource() throws JsonIOException, JsonSyntaxException, FileNotFoundException {
		JsonParser jsonParser = new JsonParser();
		JsonArray jsonArray = jsonParser.parse(new FileReader(ClassLoader.getSystemResource("datasource.json").getFile())).getAsJsonArray();

		Iterator<JsonElement> iterator = jsonArray.iterator();
		while (iterator.hasNext()) {
			JsonObject jsonObject = iterator.next().getAsJsonObject();
			if (jsonObject.get("parentTable") == null) {
				parentEntity = createEntity(jsonObject);
				continue;
			}
			jsonObjects.addFirst(jsonObject);
		}
	}

	private static void processEntity(Entity entity) {
		Iterator<JsonObject> iterator = jsonObjects.iterator();
		while (iterator.hasNext()) {
			JsonObject jsonObject = iterator.next();
			JsonElement entityName = jsonObject.get("parentTable");
			if (entity.getEntityName().equals(entityName.getAsString())) {
				Entity entity2 = createEntity(jsonObject);
				entity.addToChildEntities(entity2);
				entity2.setParentEntity(entity);
				entity2.setJoinType(jsonObject.get("jointype").getAsString());
				iterator.remove();
				processEntity(entity2);
			}
		}
	}

	private static Entity createEntity(JsonObject jsonObject) {
		Entity entity = new Entity();
		entity.setEntityName(jsonObject.get("EntityName").getAsString());
		JsonArray primaryKeyJsonArray = jsonObject.get("Pk").getAsJsonArray();
		Iterator<JsonElement> primaryKeyElementItr = primaryKeyJsonArray.iterator();
		List<String> primaryKeys = new ArrayList<String>();
		
		while (primaryKeyElementItr.hasNext()) {
			primaryKeys.add(primaryKeyElementItr.next().getAsString());
		}
		
		entity.setPrimaryKeys(primaryKeys);
		JsonElement fKeyJsonElement = jsonObject.get("FK");
		if (fKeyJsonElement != null) {
			JsonArray fKeyJsonArray = fKeyJsonElement.getAsJsonArray();
			Iterator<JsonElement> fKeyElementItr = fKeyJsonArray.iterator();
			List<String> fKeys = new ArrayList<String>();
			while (fKeyElementItr.hasNext()) {
				fKeys.add(fKeyElementItr.next().getAsString());
			}
			entity.setFkColumns(fKeys);
		}
		entity.setFilePath(jsonObject.get("fileName").getAsString());
		return entity;
	}
}
