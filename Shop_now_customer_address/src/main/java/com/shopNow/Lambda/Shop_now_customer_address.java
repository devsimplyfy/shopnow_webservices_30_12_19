
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

public class Shop_now_customer_address implements RequestHandler<JSONObject, JSONObject> {

	private String USERNAME;
	private String PASSWORD;
	private String DB_URL;
		


	
	@SuppressWarnings("unchecked")
	public JSONObject handleRequest(JSONObject input, Context context) {
		
		LambdaLogger logger = context.getLogger();
		JSONObject errorPayload = new JSONObject();
	
		if(!input.containsKey("customer_id")){			
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'customer_id' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}

		
		
		JSONArray jsonArray_customerAddress = new JSONArray();
		JSONObject jsonObject_customerAddress_Result = new JSONObject();
		if (input.get("customer_id") != null && input.get("customer_id") != "") {

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
						"select id,customerId,type_billing_shipping,firstName,lastName,address1,address2,address3,city,state,country,phoneNumber,IsPrimary,createdDatetime,pincode,email_address from address where customerId="
								+ input.get("customer_id"));

				while (resultSet.next()) {

					JSONObject jsonObject_customerAddress = new JSONObject();
					jsonObject_customerAddress.put("id", resultSet.getString("id"));
					jsonObject_customerAddress.put("firstName", resultSet.getString("firstName"));
					jsonObject_customerAddress.put("lastName", resultSet.getString("lastName"));
					jsonObject_customerAddress.put("address1", resultSet.getString("address1"));
					jsonObject_customerAddress.put("address2", resultSet.getString("address2"));
					jsonObject_customerAddress.put("address3", resultSet.getString("address3"));
					jsonObject_customerAddress.put("city", resultSet.getString("city"));
					jsonObject_customerAddress.put("state", resultSet.getString("state"));
					jsonObject_customerAddress.put("country", resultSet.getString("country"));
					jsonObject_customerAddress.put("phoneNumber", resultSet.getString("phoneNumber"));
					jsonObject_customerAddress.put("isDefault", resultSet.getString("isPrimary"));
					jsonObject_customerAddress.put("pincode", resultSet.getInt("pincode"));
					jsonObject_customerAddress.put("email_address", resultSet.getString("email_address"));
					
					
					jsonArray_customerAddress.add(jsonObject_customerAddress);

				}

				jsonObject_customerAddress_Result.put("Address", jsonArray_customerAddress);

			} catch (Exception e) {
				e.printStackTrace();
				logger.log("Caught exception: " + e.getMessage());
				JSONObject jo_catch = new JSONObject();
				jo_catch.put("Exception",e.getMessage());
				return jo_catch;

		
			}

			return jsonObject_customerAddress_Result;
		} else {
			String Str_msg = "enter valid customer ID ";
			jsonObject_customerAddress_Result.put("status", "0");
			jsonObject_customerAddress_Result.put("message", Str_msg);
			return jsonObject_customerAddress_Result;
		}
	}
}
