package com.mkjoin.main;

import java.io.FileNotFoundException;

import org.apache.spark.sql.AnalysisException;
import org.junit.Test;

import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

public class MainTest {

	@Test
	public void testMainWithoutKafka() throws JsonIOException, JsonSyntaxException, FileNotFoundException, AnalysisException {
		String[] args = {"false"};
		MainJoin.main(args);
	}
	
	@Test
	public void testMainWithKafka() throws JsonIOException, JsonSyntaxException, FileNotFoundException, AnalysisException {
		String[] args = {"true","joinedDataOuputTopic"};
		MainJoin.main(args);
	}
}