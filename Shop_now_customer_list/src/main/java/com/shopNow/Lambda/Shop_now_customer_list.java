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

public class Shop_now_customer_list implements RequestHandler<JSONObject, JSONObject> {

	
	
	private String USERNAME;
	private String PASSWORD;
	private String DB_URL;
		


	
	
	@SuppressWarnings("unchecked")
	public JSONObject handleRequest(JSONObject input, Context context) {
		LambdaLogger logger = context.getLogger();
		logger.log("Invoked JDBCSample.getCurrentTime");

		JSONObject errorPayload = new JSONObject();
	
		if(!input.containsKey("id")){			
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'id' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}

		String currentTime2 = "unavailable";
		String msg=null;

		JSONArray customer_array = new JSONArray();
		JSONObject jsonObject_customer_result = new JSONObject();
		Object user_id = input.get("id").toString();

		if (user_id != null && user_id != "") {
			// Get time from DB server
			
			Properties prop = new Properties();

			try {
				prop.load(getClass().getResourceAsStream("/application.properties"));
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
				
			
			try {
				DB_URL = prop.getProperty("url");
				USERNAME = prop.getProperty("username");
				PASSWORD = prop.getProperty("password");
				Connection conn = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);

				
				
				
				
				
				
				
				Statement stmt = conn.createStatement();
				ResultSet resultSet = stmt.executeQuery(
						"SELECT id, first_name,last_name,email,phone_number,image,status FROM customers where id='"
								+ user_id+"'");

				logger.log("SELECT id, first_name,last_name,email,phone_number,image,status FROM customers where id='"
								+ user_id+"'");
				if (resultSet.next()) {

					if(!resultSet.getString("status").equalsIgnoreCase("1"))
					{
						msg = "User not confirmed !";
						jsonObject_customer_result.put("status", "0");
						jsonObject_customer_result.put("message", msg);
						return jsonObject_customer_result;
										
					}	
					JSONObject json_obj_customer = new JSONObject();
					json_obj_customer.put("first_name", resultSet.getString("first_name"));
					json_obj_customer.put("last_name", resultSet.getString("last_name"));
					json_obj_customer.put("email", resultSet.getString("email"));
					json_obj_customer.put("phone_number", resultSet.getString("phone_number"));
					json_obj_customer.put("photo", resultSet.getString("image"));

		    		customer_array.add(json_obj_customer);
					jsonObject_customer_result.put("customer", customer_array);
					currentTime2 = jsonObject_customer_result.toString();
				}
				else {
					
					msg="No user found with this id.";
					jsonObject_customer_result.put("status", "0");
					jsonObject_customer_result.put("message", msg);
					return jsonObject_customer_result;
								
				}

				

			} catch (Exception e) {
				e.printStackTrace();
				logger.log("Caught exception: " + e.getMessage());
				msg=e.toString();
				jsonObject_customer_result.put("status", "0");
				jsonObject_customer_result.put("message", msg);	
				return jsonObject_customer_result;

			}

			return jsonObject_customer_result;
		} else {
			
			msg="Customer_id  cannot be null ";
			jsonObject_customer_result.put("status", "0");
			jsonObject_customer_result.put("message", msg);	
			return jsonObject_customer_result;

		}

	}
}
