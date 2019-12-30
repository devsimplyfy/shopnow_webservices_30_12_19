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

public class Shop_now_Cart_Deletetem implements RequestHandler<JSONObject, JSONObject> {

	private String USERNAME;
	private String PASSWORD;
	private String DB_URL;
		


	
	
	@SuppressWarnings({ "unchecked", "unused" })
	public JSONObject handleRequest(JSONObject input, Context context) {

		LambdaLogger logger = context.getLogger();
		logger.log("Invoked JDBCSample.getCurrentTime");

		JSONArray cart_Add_array = new JSONArray();
		JSONObject jsonObject_cartDelete_result = new JSONObject();

		Object userid = input.get("userid");
		Object product_id = input.get("product_id");
		Object quantity1 = input.get("quantity");
		//Object vendor_id = input.get("vendor_id");
		int quantity = Integer.parseInt(quantity1.toString());

		
		String Str_msg;

		if (quantity == 0) {

			Str_msg = "Cart_Item not deleted beacuse quantity is not valid";
			jsonObject_cartDelete_result.put("status", "0");
			jsonObject_cartDelete_result.put("message", Str_msg);
			return jsonObject_cartDelete_result;
		}

		if (userid == null || userid == "" || product_id == null || product_id == "") {

			Str_msg = "Products not Remove from Cart Because Productid or userID not null";
			jsonObject_cartDelete_result.put("status", "0");
			jsonObject_cartDelete_result.put("message", Str_msg);

			return jsonObject_cartDelete_result;

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

			Statement stmt = conn.createStatement();
			ResultSet srs_product_id = stmt.executeQuery(
					"SELECT * FROM cart_items where ProductId='" + product_id + "' AND UserId='" + userid + "'");

			if (srs_product_id.next() == false) {
				Str_msg = "Productid or userid not present in Cart Table";
				jsonObject_cartDelete_result.put("status", "0");
				jsonObject_cartDelete_result.put("message", Str_msg);

				return jsonObject_cartDelete_result;
			}

			int presantquantity = srs_product_id.getInt("Quantity");
			if (presantquantity > quantity) {

				quantity = presantquantity - quantity;

				String sql = "UPDATE cart_items SET Quantity=" + quantity
						+ " , modified_date = CURRENT_TIMESTAMP() where ProductId='" + product_id + "'AND UserId='"
						+ userid + "'";

				Statement stmt1 = conn.createStatement();
				stmt.executeUpdate(sql);

				Str_msg = "successfuly Update Product quantity from Cart";
				jsonObject_cartDelete_result.put("status", "0");
				jsonObject_cartDelete_result.put("message", Str_msg);

			} else if (presantquantity == quantity || srs_product_id.getInt("Quantity") == 0)

			{

				String sql = "DELETE From cart_items  where ProductId='" + product_id + "'AND UserId='" + userid + "'";
				Statement stmt1 = conn.createStatement();
				stmt.executeUpdate(sql);
				

				Str_msg = "DELETE ROW From cart_items";
				jsonObject_cartDelete_result.put("status", "0");
				jsonObject_cartDelete_result.put("message", Str_msg);

			}

			else {
				Str_msg = "You Remove Product quantity is Greter than Cart Product quantity ";
				jsonObject_cartDelete_result.put("status", "0");
				jsonObject_cartDelete_result.put("message", Str_msg);

			}

		}

		catch (Exception e) {
			e.printStackTrace();
			logger.log("Caught exception: " + e.getMessage());

		}

		return jsonObject_cartDelete_result;

	}

}
