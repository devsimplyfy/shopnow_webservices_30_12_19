
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

import org.json.simple.JSONObject;

public class Shop_now_customer_address_delete implements RequestHandler<JSONObject, JSONObject> {

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
		if(!input.containsKey("addressId")){			
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'addressId' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}

		
		
		JSONObject jsonObject_customerAddress_Delete = new JSONObject();
		String Str_msg = null;
		String customer_id=input.get("customer_id").toString();
		String addressId=input.get("addressId").toString();
		
		if (customer_id != null && addressId != null && customer_id != "" && addressId != "") {

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
				Statement select_stmt = conn.createStatement();
				Statement update_stmt = conn.createStatement();
				String update_sql=null;
				final String select_sql = "SELECT * FROM address WHERE customerId ='" + customer_id + "' and id='" + addressId + "'and isPrimary=1";
				
				
				ResultSet rs = select_stmt.executeQuery(select_sql);
				int flag = 0;
				if(rs.next()) {
					flag = 1;
					update_sql="UPDATE address SET isPrimary=1 WHERE customerId ='"+ customer_id + "' AND isPrimary=0 LIMIT 1";
				}
				
				
				final String sql = "DELETE FROM address WHERE customerId ='" + customer_id + "' and id='"+ addressId + "'";

				int i = stmt.executeUpdate(sql);
				if (i > 0) {
					if(flag==1){
					update_stmt.executeUpdate(update_sql);
					}
					Str_msg = "Record deleted successfully";
					jsonObject_customerAddress_Delete.put("status", "1");
					jsonObject_customerAddress_Delete.put("message", Str_msg);
				}

				else {
					Str_msg = "Record not Found";
					jsonObject_customerAddress_Delete.put("status", "0");
					jsonObject_customerAddress_Delete.put("message", Str_msg);
				}

			} catch (Exception e) {
				e.printStackTrace();
				logger.log("Caught exception: " + e.getMessage());
				JSONObject jo_catch = new JSONObject();
				jo_catch.put("Exception",e.getMessage());
				return jo_catch;
			}
		} else {
			Str_msg = "enter valid customer ID or id";
			jsonObject_customerAddress_Delete.put("status", "0");
			jsonObject_customerAddress_Delete.put("message", Str_msg);

		}

		return jsonObject_customerAddress_Delete;
	}
}
