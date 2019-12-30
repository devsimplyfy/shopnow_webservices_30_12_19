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

public class Shop_now_Cart_Remove implements RequestHandler<JSONObject, JSONObject> {

	
	private String USERNAME;
	private String PASSWORD;
	private String DB_URL;
	


	
	
	
	@SuppressWarnings({ "unchecked", "unused" })
	public JSONObject handleRequest(JSONObject input, Context context) {

		LambdaLogger logger = context.getLogger();
		logger.log("Invoked JDBCSample.getCurrentTime");

		JSONObject errorPayload = new JSONObject();
	
		if(!input.containsKey("userid")){			
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'userid' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}
		if(!input.containsKey("product_id")){
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'product_id' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}
		if(!input.containsKey("device_id")){
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'device_id' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}
		
		
		JSONArray cart_Add_array = new JSONArray();
		JSONObject jsonObject_cartAdd_result = new JSONObject();

		Object userid1 = input.get("userid");
		Object product_id1 = input.get("product_id");
		Object device_id1 = input.get("device_id");
		String device_id = device_id1.toString();
		String vendor_id = null;
		long userid ;
		long product_id ;

		String Str_msg;
		JSONObject jo_cartInsert = new JSONObject();

		if (userid1 == null || userid1 == "") {
			userid = 0;
		} else {
			userid = Integer.parseInt(userid1.toString());
		}

		if (product_id1 == null || product_id1 == "") {

			product_id = 0;
		} else {
			product_id = Integer.parseInt(product_id1.toString());

		}
		
		if ((device_id1 == null || device_id1 == "") && userid == 0) {
			// device_id=null;
			Str_msg = "Please Enter either UserId or Device_id";
			jo_cartInsert.put("status", "0");
			jo_cartInsert.put("message", Str_msg);
			return jo_cartInsert;
		}

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

			
			if (userid == 0) {
                  
				
				String sql1 = "Delete from cart_items where ProductId='" + product_id
						+ "' and device_id='" + device_id + "' and UserId='0'";

				Statement stmt1 = conn.createStatement();
				int i = stmt1.executeUpdate(sql1);

				if (i > 0) {
					Str_msg = "Removed Product from  cart ";
					jo_cartInsert.put("status", "1");
					jo_cartInsert.put("message", Str_msg);
				} else {

					Str_msg = "Not Removed Product from  cart ";
					jo_cartInsert.put("status", "0");
					jo_cartInsert.put("message", Str_msg);

				}
				return jo_cartInsert;

			} else {

				String sql2 = "Delete from cart_items where  UserId='" + userid + "' and ProductId='"
						+ product_id + "'";

				Statement stmt2 = conn.createStatement();
				int i = stmt2.executeUpdate(sql2);

				if (i > 0) {
					Str_msg = "Removed Product from  cart ";
					jo_cartInsert.put("status", "1");
					jo_cartInsert.put("message", Str_msg);
				} else {

					Str_msg = " Not Removed Product from  cart ";
					jo_cartInsert.put("status", "0");
					jo_cartInsert.put("message", Str_msg);

				}
				return jo_cartInsert;
			}

			// Changing-------------------------------------------------------

		} catch (Exception e) {
			logger.log("Exception" + e);
			JSONObject jo_catch = new JSONObject();
			jo_catch.put("Exception", e.getMessage());
			return jo_catch;

		}

		
	}

}
