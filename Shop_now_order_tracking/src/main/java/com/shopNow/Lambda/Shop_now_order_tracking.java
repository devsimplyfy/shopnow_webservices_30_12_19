
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

public class Shop_now_order_tracking implements RequestHandler<JSONObject, JSONObject> {

	private String USERNAME;
	private String PASSWORD;
	private String DB_URL;


	@SuppressWarnings({ "unchecked" })
	public JSONObject handleRequest(JSONObject input, Context context) {
		LambdaLogger logger = context.getLogger();
		logger.log("Invoked JDBCSample.getCurrentTime");
JSONObject errorPayload = new JSONObject();
	
		if(!input.containsKey("user_id")){			
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'user_id' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}
		if(!input.containsKey("order_id")){
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'order_id' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}
		if(!input.containsKey("tracking_number")){
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'tracking_number' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}
		
		String user_id = input.get("user_id").toString();
		logger.log(input.get("user_id").toString());
		String order_id = input.get("order_id").toString();
		String tracking_number = input.get("tracking_number").toString();	
		
		JSONArray pincode_array = new JSONArray();
		JSONObject jsonObject_tracking_result = new JSONObject();
		String Str_msg = null;
		
		
		if (input.get("user_id")==null ||input.get("user_id")=="") {

			Str_msg = "User Id cannot be empty !! ";
			jsonObject_tracking_result.put("status", "0");
			jsonObject_tracking_result.put("message", Str_msg);
			return jsonObject_tracking_result;

		}
		
		if(input.get("order_id")==null||input.get("order_id")=="") {
			if(tracking_number==null||tracking_number=="")
			{
				
				Str_msg = "Either order_id or tracking_number is required !! ";
				jsonObject_tracking_result.put("status", "0");
				jsonObject_tracking_result.put("message", Str_msg);
				return jsonObject_tracking_result;
				
				
			}			
					
		}
		
		
			

		Properties prop = new Properties();
	
	// Get time from DB server
		try {
			prop.load(getClass().getResourceAsStream("/application.properties"));
			DB_URL = prop.getProperty("url");
			USERNAME = prop.getProperty("username");
			PASSWORD = prop.getProperty("password");
		
			Connection conn = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
		
			Statement stmt_order_tracking = conn.createStatement();
			Statement stmt_order = conn.createStatement();
			
			String sql;
			logger.log("\n "+ order_id);
			
       if(input.get("order_id")==null||input.get("order_id")=="") {
    	   sql="select * from order_tracking where tracking_number='"+tracking_number+"'"; 
				
       }
       else {
    	   sql = "select * from customer_orders where order_id='"+order_id + "' AND customer_id = '"+user_id+"'";
    	    	   
       }
			logger.log("\n sql_tracking  resultSet_order\n "+sql );
			
			ResultSet resultSet_order = stmt_order.executeQuery(sql);	
			if (resultSet_order.next() == false) {

				Str_msg = "Sorry, no records found for entered details";
				jsonObject_tracking_result.put("status", "0");
				jsonObject_tracking_result.put("message", Str_msg);
				return jsonObject_tracking_result;

			}
			 
			
			String sql_tracking ="SELECT couriers.courier_name,table1.*,customer_orders.order_id,customer_orders.date_of_placed_order,customer_orders.date_of_order_paid,customer_orders.order_number FROM (SELECT order_tracking.*,admin.name FROM order_tracking LEFT JOIN admin  ON order_tracking.vendor_id=admin.id)AS table1 LEFT JOIN couriers ON table1.courier_id=couriers.id LEFT JOIN \r\n" + 
					"customer_orders ON table1.order_id=customer_orders.order_id WHERE customer_orders.customer_id='"+user_id+"'  AND ( customer_orders.order_id='"+order_id+"' OR table1.tracking_number='"+tracking_number+"')";
			
			logger.log("\n sql_tracking \n  "+ sql_tracking);
			
			ResultSet resultSet_tracking=stmt_order_tracking.executeQuery(sql_tracking); 
			if (resultSet_tracking.next()) {

				jsonObject_tracking_result.put("tracking_id", resultSet_tracking.getInt("tracking_number"));
				jsonObject_tracking_result.put("order_number", resultSet_tracking.getString("order_number"));
				jsonObject_tracking_result.put("current_status", resultSet_tracking.getString("STATUS"));
				jsonObject_tracking_result.put("courier_name", resultSet_tracking.getString("courier_name"));
				jsonObject_tracking_result.put("courier_address", "courier address");
				jsonObject_tracking_result.put("courier_contact","contact number");	
			
				jsonObject_tracking_result.put("order_placed_date_time", resultSet_tracking.getDate("date_of_placed_order"));
				jsonObject_tracking_result.put("order_paid_date_time", resultSet_tracking.getDate("date_of_order_paid"));
				jsonObject_tracking_result.put("expected_date_of_delivery", resultSet_tracking.getDate("expected_date_of_delivery"));
			
						
			}
			else {
				jsonObject_tracking_result.put("message", "Sorry, no records found for entered details");
				jsonObject_tracking_result.put("status", "0");
				
			}
			
			
		}catch(Exception e) {
				jsonObject_tracking_result.put("message", e.toString());
				jsonObject_tracking_result.put("status", "0");
			
			
		}	
			
		return jsonObject_tracking_result;
	}
}
