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




public class Shop_now_customer_profile_update implements RequestHandler<JSONObject, JSONObject> {
	private String USERNAME;
	private String PASSWORD;
	private String DB_URL;
		


	

	@SuppressWarnings("unchecked")
	public JSONObject handleRequest(JSONObject input, Context context) {
		
		
		LambdaLogger logger = context.getLogger();
		
		JSONObject errorPayload = new JSONObject();
	
		if(!input.containsKey("custid")){			
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'custid' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}
		if(!input.containsKey("first_name")){			
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'first_name' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}
		if(!input.containsKey("last_name")){
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'last_name' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}
		if(!input.containsKey("email")){
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'email' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}
		if(!input.containsKey("phone_number")){
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'phone_number' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}	
		if(!input.containsKey("image")){
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'image' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}
		if(!input.containsKey("googleId")){
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'googleId' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}
		

		String Str_msg;
		JSONObject jsonObject_cust_Profile_update = new JSONObject();
		JSONObject jsonObject_cust_Profile_update1 = new JSONObject();
	
		
		String custid=input.get("custid").toString();
		
		String first_name = input.get("first_name").toString();
		String last_name = input.get("last_name").toString();
		String email = input.get("email").toString();	
		String google_id = input.get("googleId").toString();
		String phone_number = input.get("phone_number").toString();
		String image = input.get("image").toString();

	
		long custid1;

		if (custid == null || custid == "") {
			Str_msg = "UserId not valid ";
			jsonObject_cust_Profile_update.put("status", "0");
			jsonObject_cust_Profile_update.put("message", Str_msg);
			return jsonObject_cust_Profile_update;
		
		} else {
		 custid1=Long.parseLong(custid);
		}
		
		
		if (email == null || email == "") {
			Str_msg = "email not valid ";
			jsonObject_cust_Profile_update.put("status", "0");
			jsonObject_cust_Profile_update.put("message", Str_msg);
			return jsonObject_cust_Profile_update;
		
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
					
					
					Statement stmt_ = conn.createStatement();
					ResultSet resultSet_ = stmt_.executeQuery("SELECT id, status FROM customers where id='"+custid1+"'" );
					
					if(resultSet_.next()==false) {

						Str_msg = "No user found!";
						jsonObject_cust_Profile_update.put("status", "0");
						jsonObject_cust_Profile_update.put("message", Str_msg);
						return jsonObject_cust_Profile_update;
					}
					else if(!resultSet_.getString("status").equalsIgnoreCase("1")){
						
						Str_msg = "User not confirmed !";
						jsonObject_cust_Profile_update.put("status", "0");
						jsonObject_cust_Profile_update.put("message", Str_msg);
						return jsonObject_cust_Profile_update;
			}

					
										
				
				Statement stmt = conn.createStatement();
				ResultSet resultSet = stmt.executeQuery(
						"SELECT id,first_name,last_name,email,phone_number,image,google_id FROM customers where id='"+custid1+"' and email='"+email+"'" );

			
				
				if(resultSet.next()==false) {

					Str_msg = "user id or email not valid";
					jsonObject_cust_Profile_update.put("status", "0");
					jsonObject_cust_Profile_update.put("message", Str_msg);
					return jsonObject_cust_Profile_update;
				}
				else {
					
					
					String first_name2 = resultSet.getString("first_name");
					String last_name2 = resultSet.getString("last_name");
					String phone_number2 = resultSet.getString("phone_number");
					String image2 = resultSet.getString("image");
					String email2 =resultSet.getString("email");
					String google_id1=resultSet.getString("google_id");
					
					
				/*	if(first_name==null || first_name=="") {
						first_name=first_name2;
						
					}
				
					if(last_name==null || last_name=="") {
						last_name=last_name2;
						
					}
				
					if(image==null || image=="") {
						image=image2;
						
					}
					if(phone_number==null || phone_number=="") {
						phone_number=phone_number2;
						
					}
					
					if(google_id==null || google_id=="") {
						google_id=google_id1;
						
					}
			*/	
					if(phone_number==null || phone_number=="") {
						phone_number="NULL";
						
					}		
							
					String sql="update customers set first_name='"+first_name+"',last_name='"+last_name+"',phone_number="+phone_number+",image='"+image+"',google_id='"+google_id+"' where id='"+custid1+"' and email='"+email+"'";
					
					logger.log("\n " + sql);
					
					Statement stmt1 = conn.createStatement();
					int i =stmt1.executeUpdate(sql);
					
					if(i>0) {
					Str_msg = "Customer Profile Update successfully ";
					jsonObject_cust_Profile_update.put("status", "1");
					jsonObject_cust_Profile_update.put("message", Str_msg);
					return jsonObject_cust_Profile_update;	
					}else {
						
						Str_msg = "error in Update ";
						jsonObject_cust_Profile_update.put("status", "1");
						jsonObject_cust_Profile_update.put("message", Str_msg);
						return jsonObject_cust_Profile_update;					
						
					}
				}
			} catch (Exception e) {
				
				logger.log("Caught exception: " + e.getMessage());
				jsonObject_cust_Profile_update1.put("message", e.getMessage());
				return jsonObject_cust_Profile_update1;					

		
			}

		
	
	}
}
