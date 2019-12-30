package com.shopNow.Lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class Shop_now_Cart_Apply_Promocode implements RequestHandler<JSONObject, JSONObject> {
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
		if(!input.containsKey("promocode")){
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'promocode' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}
		if(!input.containsKey("device_id")){
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'device_id' is missing");
			throw new RuntimeException(errorPayload.toJSONString());	
		}		
				
		Object userid1 = input.get("userid");
		Object promocode1 = input.get("promocode");
		String promocode = promocode1.toString();
		Object device_id1 = input.get("device_id");
		String device_id = device_id1.toString();

		long userid;
		int promocode_id;
		long cart_id;
		Connection conn=null;

		String Str_msg;
		JSONArray promocodes_array = new JSONArray();
		JSONObject jo_cartInsert = new JSONObject();
		

		if (userid1 == null || userid1 == "") {
			userid = 0;
		} else {

			userid = Long.parseLong(userid1.toString());
		}

		// Get time from DB server

		if (promocode == null || promocode == "") {

			Str_msg = "No Promocode Entered";
			jo_cartInsert.put("status", "0");
			jo_cartInsert.put("message", Str_msg);

			return jo_cartInsert;
		}

		if ((device_id1 == null || device_id1 == "") && userid == 0) {

			Str_msg = "Please Enter either UserId or Device_id";
			jo_cartInsert.put("status", "0");
			jo_cartInsert.put("message", Str_msg);
			return jo_cartInsert;
		}

		
		
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
			
			
			conn = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
			String promo_sql1 = "SELECT * FROM promocodes where promocode='" + promocode + "' and status='1'";
			
			Statement stmt1 = conn.createStatement();
			ResultSet resultSet1 = stmt1.executeQuery(promo_sql1);
			
			if (resultSet1.next()) {
				promocode_id = resultSet1.getInt("id");
				SimpleDateFormat f1 = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
				String s1 = null;
				Date d2, d3 = null;

				String dateStop = resultSet1.getString("end_time");
				String dateStop1 = dateStop.substring(0, dateStop.length() - 3);
				long dateStop2 = Long.parseLong(dateStop1);
				String edate = f1.format(new java.util.Date(dateStop2 * 1000));

				String current = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(Calendar.getInstance().getTime());

				try {
					d2 = f1.parse(edate);
					d3 = f1.parse(current);

					// in milliseconds
					long diff = d2.getTime() - d3.getTime();
					if (diff > 0) {
						if (userid == 0) {
							String sql = "SELECT * FROM cart_items where device_id='" + device_id
									+ "'AND UserId=0";
							ResultSet resultSet = stmt1.executeQuery(sql);
							if (resultSet.next()) {
								
								String sql3 = "INSERT IGNORE INTO cart_promocode (device_id,user_id,promocode_id) VALUES('"
										+ device_id + "','" + userid + "','" + promocode_id + "')";

								int i = stmt1.executeUpdate(sql3);
								if (i > 0) {
									String promo_sql2 = "SELECT id, promocode, description FROM promocodes where id IN(SELECT promocode_id FROM cart_promocode where device_id='"
											+ device_id + "'AND user_id=0)";

									ResultSet resultSet2 = stmt1.executeQuery(promo_sql2);
									while (resultSet2.next()) {
										JSONObject promocodes = new JSONObject();
										Str_msg = "Promocode Applied Successfully";
										jo_cartInsert.put("status", "1");
										jo_cartInsert.put("message", Str_msg);
										promocodes.put("promocode_id", resultSet2.getInt("id"));
										promocodes.put("promoname", resultSet2.getString("description"));
										//promocodes.put("promocode", resultSet2.getString("promocode"));
										promocodes_array.add(promocodes);
											
									}
									jo_cartInsert.put("Promocodes", promocodes_array);
								} else {
									Str_msg = "Promocode Already Applied";
									jo_cartInsert.put("status", "0");
									jo_cartInsert.put("message", Str_msg);

								}
							} else {
								Str_msg = "Poduct Not Found in Cart";
								jo_cartInsert.put("status", "0");
								jo_cartInsert.put("message", Str_msg);
							}
						} else {
							String sql = "SELECT * FROM cart_items where UserId='" + userid + "'";
							ResultSet resultSet_new = stmt1.executeQuery(sql);
							if (resultSet_new.next()) {

								String sql3 = "INSERT IGNORE INTO cart_promocode (user_id,promocode_id) VALUES('" + userid + "','" + promocode_id + "')";

								int i = stmt1.executeUpdate(sql3);
								if (i > 0) {
									String promo_sql2 = "SELECT id, promocode, description FROM promocodes where id IN(SELECT promocode_id FROM cart_promocode where user_id='"
											+ userid + "')";

									ResultSet resultSet4 = stmt1.executeQuery(promo_sql2);
									while (resultSet4.next()) {
										JSONObject promocodes = new JSONObject();
										Str_msg = "Promocode Applied Successfully";
										jo_cartInsert.put("status", "1");
										jo_cartInsert.put("message", Str_msg);
										promocodes.put("promocode_id", resultSet4.getInt("id"));
										promocodes.put("promoname", resultSet4.getString("description"));
										//promocodes.put("promocode", resultSet4.getString("promocode"));
										
										promocodes_array.add(promocodes);	
									}
									
									jo_cartInsert.put("Promocodes", promocodes_array);
								} else {
									Str_msg = "Promocode Already Applied";
									jo_cartInsert.put("status", "0");
									jo_cartInsert.put("message", Str_msg);

								}
							} else {
								Str_msg = "Poduct Not Found in Cart";
								jo_cartInsert.put("status", "0");
								jo_cartInsert.put("message", Str_msg);
							}
						}

					} else {
						Str_msg = "Sorry, Promocode has expired";
						jo_cartInsert.put("status", "0");
						jo_cartInsert.put("message", Str_msg);
					}

				} catch (Exception e) {
					logger.log("Exception " + e);
					jo_cartInsert.put("status", "0");
					jo_cartInsert.put("message", e);

				}

			} else {
				Str_msg = "Invalid Promocode entered";
				jo_cartInsert.put("status", "0");
				jo_cartInsert.put("message", Str_msg);
			}

		} catch (Exception e) {
			logger.log("Exception " + e);
			jo_cartInsert.put("status", "0");
			jo_cartInsert.put("message", e);
		}

		return jo_cartInsert;
	}
}
