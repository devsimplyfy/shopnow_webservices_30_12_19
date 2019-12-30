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

public class Shop_now_customer_address_insert implements RequestHandler<JSONObject, JSONObject> {

	private String USERNAME;
	private String PASSWORD;
	private String DB_URL;

	@SuppressWarnings("unchecked")
	public JSONObject handleRequest(JSONObject input, Context context) {
		LambdaLogger logger = context.getLogger();
		logger.log("Invoked JDBCSample.getCurrentTime");
		
				JSONObject errorPayload = new JSONObject();
	
		if(!input.containsKey("customerId")){			
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'customerId' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}
		if(!input.containsKey("addressId")){			
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'addressId' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}
		if(!input.containsKey("type_billing_shipping")){
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'type_billing_shipping' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}
		if(!input.containsKey("firstName")){
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'firstName' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}
		if(!input.containsKey("lastName")){
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'lastName' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}	
		if(!input.containsKey("address1")){
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'address1' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}
		if(!input.containsKey("address2")){
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'address2' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}
		if(!input.containsKey("address3")){			
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'address3' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}
		if(!input.containsKey("city")){			
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'city' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}
		if(!input.containsKey("state")){
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'state' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}
		if(!input.containsKey("country")){
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'country' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}
		if(!input.containsKey("phoneNumber")){
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'phoneNumber' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}	
		if(!input.containsKey("isDefault")){
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'isDefault' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}
		if(!input.containsKey("pincode")){
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'pincode' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}
		if(!input.containsKey("email_address")){
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'email_address' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}
		String isDefault = input.get("isDefault").toString();
		boolean isDefault1 = Boolean.parseBoolean(isDefault);

		String addressId1 = input.get("addressId").toString();
		int addressId = 0;
		if (addressId1 == null || addressId1 == "") {
			addressId = 0;

		} else {
			addressId = Integer.parseInt(addressId1);
		}

		JSONObject jsonObject_customerAddress_insert = new JSONObject();
		String Str_msg = null;
		String customerid = input.get("customerId").toString();
		String type_billing_shipping = input.get("type_billing_shipping").toString();
		String lastName = input.get("lastName").toString();
		String firstName = input.get("firstName").toString();
		String address1 = input.get("address1").toString();
		String address2 = input.get("address2").toString();
		String address3 = input.get("address3").toString();
		String city = input.get("city").toString();
		String country = input.get("country").toString();
		String state = input.get("state").toString();
		String phoneNumber = input.get("phoneNumber").toString();
		String email_address = input.get("email_address").toString();
		String pincode = input.get("pincode").toString();

		if (customerid != null && customerid != "" && input.get("type_billing_shipping") != null
				&& input.get("type_billing_shipping") != "" && input.get("lastName") != null
				&& input.get("lastName") != "" && input.get("firstName") != null && input.get("firstName") != ""
				&& input.get("address1") != null && input.get("address1") != "" && input.get("city") != null
				&& input.get("city") != "" && input.get("state") != null && input.get("state") != ""
				&& input.get("country") != null && input.get("country") != "" && input.get("phoneNumber") != null
				&& input.get("phoneNumber") != "" && input.get("pincode") != null && input.get("pincode") != ""
				&& input.get("email_address") != null && input.get("email_address") != "") {

			int pincode_int = Integer.parseInt(pincode);

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

				if (addressId != 0) {

					if (isDefault1 == true) {

						String sql_update = "update address set isPrimary='0' where customerId="
								+ input.get("customerId") + " and isPrimary='1' ";

						String sql1 = "select id from customers where id=" + input.get("customerId") + "";
						ResultSet resultSet = stmt.executeQuery(sql1);

						if (resultSet.next()) {

							int i = stmt.executeUpdate(sql_update);

							String sql_add_update = "update address set type_billing_shipping= '"
									+ type_billing_shipping + "',firstName='" + firstName + "',lastName='" + lastName
									+ "',address1='" + address1 + "',address2='" + address2 + "',address3='" + address3
									+ "',city='" + city + "',state='" + state + "',country='" + country
									+ "',phoneNumber='" + phoneNumber + "',pincode='" + pincode_int
									+ "',email_address='" + email_address + "',isPrimary='1' where customerId="
									+ customerid + " AND id=" + addressId;

							logger.log("\n" + sql_add_update);

							int update = stmt.executeUpdate(sql_add_update);

							if (update > 0) {
								Str_msg = "Updated user Address ";
								jsonObject_customerAddress_insert.put("status", "1");
								jsonObject_customerAddress_insert.put("message", Str_msg);
								return jsonObject_customerAddress_insert;
							} else {
								Str_msg = "Sorry ! user Address Not Updated";
								jsonObject_customerAddress_insert.put("status", "0");
								jsonObject_customerAddress_insert.put("message", Str_msg);
								return jsonObject_customerAddress_insert;
							}

						} else {

							Str_msg = "Record not inserted because customerid not valid";
							jsonObject_customerAddress_insert.put("status", "0");
							jsonObject_customerAddress_insert.put("message", Str_msg);

							return jsonObject_customerAddress_insert;

						}

					} else {

						String sql1 = "select id from customers where id=" + input.get("customerId") + "";
						ResultSet resultSet = stmt.executeQuery(sql1);

						if (resultSet.next()) {

							String sql_add_update = "update address set type_billing_shipping= '"
									+ type_billing_shipping + "',firstName='" + firstName + "',lastName='" + lastName
									+ "',address1='" + address1 + "',address2='" + address2 + "',address3='" + address3
									+ "',city='" + city + "',state='" + state + "',country='" + country
									+ "',phoneNumber='" + phoneNumber + "',pincode='" + pincode_int
									+ "',email_address='" + email_address + "' where customerId=" + customerid
									+ " AND id=" + addressId;

							logger.log("\n" + sql_add_update);

							int update = stmt.executeUpdate(sql_add_update);

							if (update > 0) {
								Str_msg = "Updated user Address ";
								jsonObject_customerAddress_insert.put("status", "1");
								jsonObject_customerAddress_insert.put("message", Str_msg);
								return jsonObject_customerAddress_insert;
							} else {
								Str_msg = "Sorry ! user Address Not Updated";
								jsonObject_customerAddress_insert.put("status", "0");
								jsonObject_customerAddress_insert.put("message", Str_msg);
								return jsonObject_customerAddress_insert;
							}

						}

					}

				} else {

					if (isDefault1 == true) {

						String sql_update = "update address set isPrimary='0' where customerId="
								+ input.get("customerId") + " and isPrimary='1' ";
						final String sql1 = "select id from customers where id=" + input.get("customerId") + "";
						ResultSet resultSet = stmt.executeQuery(sql1);

						if (resultSet.next()) {
							stmt.executeUpdate(sql_update);
							final String sql = "INSERT INTO address (customerId,type_billing_shipping,firstName,lastName,address1,address2,address3,city,state,country,phoneNumber,pincode,email_address,isPrimary) VALUES("
									+ input.get("customerId") + ",'" + input.get("type_billing_shipping").toString()
									+ "','" + input.get("firstName").toString() + "','"
									+ input.get("lastName").toString() + "','" + input.get("address1").toString()
									+ "','" + input.get("address2").toString() + "','"
									+ input.get("address3").toString() + "','" + input.get("city").toString() + "','"
									+ input.get("state").toString() + "','" + input.get("country").toString() + "','"
									+ input.get("phoneNumber").toString() + "'," + pincode_int + ",'" + email_address
									+ "','1')";

							int i = stmt.executeUpdate(sql);
							if (i > 0) {
								Str_msg = "Record insert successfully";
								jsonObject_customerAddress_insert.put("status", "1");
								jsonObject_customerAddress_insert.put("message", Str_msg);

							}

							else {
								Str_msg = "Record not inserted";
								jsonObject_customerAddress_insert.put("status", "0");
								jsonObject_customerAddress_insert.put("message", Str_msg);

							}
						} else {
							Str_msg = "Record not inserted because customerid not valid";
							jsonObject_customerAddress_insert.put("status", "0");
							jsonObject_customerAddress_insert.put("message", Str_msg);

						}

					} else {

						String sql1 = "select id from customers where id=" + input.get("customerId") + "";

						String check_sql = "select * from address where customerId = '" + input.get("customerId")
								+ "'";

						//logger.log("\n sql \n" + check_sql);

						ResultSet check_resultSet = stmt.executeQuery(check_sql);
						String sql;

						if (check_resultSet.next()) {

							//logger.log(check_resultSet.getInt("count"));
							//logger.log(check_resultSet.getString("count"));
							sql = "INSERT INTO address (customerId,type_billing_shipping,firstName,lastName,address1,address2,address3,city,state,country,phoneNumber,isPrimary,pincode,email_address) VALUES("
									+ input.get("customerId") + ",'" + input.get("type_billing_shipping").toString()
									+ "','" + input.get("firstName").toString() + "','"
									+ input.get("lastName").toString() + "','" + input.get("address1").toString()
									+ "','" + input.get("address2").toString() + "','"
									+ input.get("address3").toString() + "','" + input.get("city").toString()
									+ "','" + input.get("state").toString() + "','"
									+ input.get("country").toString() + "','" + input.get("phoneNumber").toString()
									+ "','0'," + pincode_int + ",'" + email_address + "')";
						}
							
						else {
								sql = "INSERT INTO address (customerId,type_billing_shipping,firstName,lastName,address1,address2,address3,city,state,country,phoneNumber,isPrimary,pincode,email_address) VALUES("
										+ input.get("customerId") + ",'" + input.get("type_billing_shipping").toString()
										+ "','" + input.get("firstName").toString() + "','"
										+ input.get("lastName").toString() + "','" + input.get("address1").toString()
										+ "','" + input.get("address2").toString() + "','"
										+ input.get("address3").toString() + "','" + input.get("city").toString()
										+ "','" + input.get("state").toString() + "','"
										+ input.get("country").toString() + "','" + input.get("phoneNumber").toString()
										+ "','1'," + pincode_int + ",'" + email_address + "')";

								logger.log("\n sql \n" + sql);
							}
						
						
						check_resultSet.close();
						ResultSet resultSet = stmt.executeQuery(sql1);

						if (resultSet.next()) {

							int i = stmt.executeUpdate(sql);

							if (i > 0) {
								Str_msg = "Record insert successfully";
								jsonObject_customerAddress_insert.put("status", "1");
								jsonObject_customerAddress_insert.put("message", Str_msg);

							}

							else {
								Str_msg = "Record not inserted";
								jsonObject_customerAddress_insert.put("status", "0");
								jsonObject_customerAddress_insert.put("message", Str_msg);

							}
						} else {
							Str_msg = "No user found.";
							jsonObject_customerAddress_insert.put("status", "0");
							jsonObject_customerAddress_insert.put("message", Str_msg);

						}

					}

				}

			} catch (Exception e) {
				e.printStackTrace();
				logger.log("Caught exception: " + e.getMessage());
				JSONObject jo_catch = new JSONObject();
				jo_catch.put("Exception",e.getMessage());
				return jo_catch;
			}
		} else {
			Str_msg = "Entered Fields are null so not insert record";
			jsonObject_customerAddress_insert.put("status", "0");
			jsonObject_customerAddress_insert.put("message", Str_msg);

		}

		return jsonObject_customerAddress_insert;
	}
}
