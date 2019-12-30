package com.shopNow.Lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class Shop_now_category implements RequestHandler<JSONObject, JSONObject> {
	
	private String USERNAME;
	private String PASSWORD;
	private String DB_URL;


	@SuppressWarnings({ "unchecked" })
	public JSONObject handleRequest(JSONObject input, Context context) {
		LambdaLogger logger = context.getLogger();
		logger.log("Invoked JDBCSample.getCurrentTime");

		JSONArray category_array = new JSONArray();
		JSONObject jsonObject_category_result = new JSONObject();

		
		Properties prop = new Properties();

		try {
			prop.load(getClass().getResourceAsStream("/application.properties"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.log("Exception "+ e);
			JSONObject jo_catch = new JSONObject();
			jo_catch.put("Exception",e.getMessage());
			return jo_catch;

		}
		try{
		String id_get = input.get("id").toString();
		logger.log("Reached Here");
		
		if (input.get("id") != null && input.get("id") != "") {

			// Get time from DB server
			try {
				DB_URL = prop.getProperty("url");
				USERNAME = prop.getProperty("username");
				PASSWORD = prop.getProperty("password");
				//DB=prop.getProperty("database");
				Connection conn = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);

				Statement stmt = conn.createStatement();
				ResultSet resultSet = stmt.executeQuery(
						"SELECT * FROM categories where parent_id =" + input.get("id"));

				while (resultSet.next()) {
					
					JSONObject jsonObject_category = new JSONObject();
					jsonObject_category.put("id", resultSet.getString(1));
					jsonObject_category.put("name", resultSet.getString(2));
					jsonObject_category.put("image", resultSet.getString(4));

					category_array.add(jsonObject_category);
					jsonObject_category_result.put("categories", category_array);

				}
				if(category_array.isEmpty()) {
					jsonObject_category_result.put("message", "Category not found");
					jsonObject_category_result.put("status", "0");
					return jsonObject_category_result;

					
				}

			} catch (Exception e) {
				e.printStackTrace();
				logger.log("Caught exception: " + e.getMessage());
				logger.log("Exception "+ e);
				JSONObject jo_catch = new JSONObject();
				jo_catch.put("Exception",e.getMessage());
				return jo_catch;

			}
			return jsonObject_category_result;
		} else {
			jsonObject_category_result.put("message", "Category id cannot be null");
			jsonObject_category_result.put("status", "0");
			return jsonObject_category_result;

		}
		}
		catch (Exception e1) {
		jsonObject_category_result.put("errorType", "BadRequest");
		jsonObject_category_result.put("httpStatus", 400);
		jsonObject_category_result.put("requestId", context.getAwsRequestId());
		jsonObject_category_result.put("message", "JSON Input key 'id' is missing");
		}
		throw new RuntimeException(jsonObject_category_result.toJSONString());
		
		
	}
}
