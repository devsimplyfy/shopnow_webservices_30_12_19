package com.shopNow.Lambda;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.Properties;

import javax.servlet.http.HttpServletRequest;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class Shop_now_Home implements RequestHandler<JSONObject, JSONObject> {

	private String USERNAME;
	private String PASSWORD;
	private String DB_URL;
		


	
	@SuppressWarnings("unchecked")
	public JSONObject handleRequest(JSONObject input, Context context) {
		LambdaLogger logger = context.getLogger();
		
		
		
		Properties prop = new Properties();

		try {
			prop.load(getClass().getResourceAsStream("/application.properties"));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		  DB_URL = prop.getProperty("url");
		  USERNAME = prop.getProperty("username");
		  PASSWORD = prop.getProperty("password");
		 Connection conn =null;
		
		 Object customer_id = 0;
		 Statement stmt = null;
		 int customer_id1;
		
		
		
		
		
		try {
			conn = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
			stmt = conn.createStatement();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		   
		
		
			String sql_id="SELECT id FROM customers WHERE email IN (SELECT email FROM Customer_Authentication WHERE accessToken)";
			ResultSet resultSet_id;
			try {
				resultSet_id = stmt.executeQuery(sql_id);
				
				while (resultSet_id.next()) {
					customer_id1=resultSet_id.getInt(1);
					customer_id=customer_id;
					logger.log("Tokan" + customer_id);
					logger.log("\n Tokan1" + customer_id1);
					System.out.println(customer_id1);
					
					
				}
				
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		
			
		
		

		if (input.get("customer_id") != null && input.get("customer_id") != "") {
			customer_id = input.get("customer_id");
		}

		
		logger.log("Invoked JDBCSample.getCurrentTime" + customer_id);
		

		JSONObject jsonObject_final = new JSONObject();

		String sql_top_picks = "SELECT table3.* FROM(SELECT products.id,products.name,products.regular_price,products.sale_price,products.image,\n"
				+ " (CASE WHEN table2.product_id IS NOT NULL THEN 1 ELSE 0 END)AS wishlist FROM products\n"
				+ " LEFT JOIN (SELECT * FROM wish_list) AS table2 ON products.id=table2.product_id and customer_id="
				+ customer_id + ") AS table3 INNER JOIN top_picks ON table3.id=top_picks.product_id";

		String sql_best = "SELECT table3.* FROM(SELECT products.id,products.name,products.regular_price,products.sale_price,products.image,\n"
				+ " (CASE WHEN table2.product_id IS NOT NULL THEN 1 ELSE 0 END)AS wishlist FROM products\n"
				+ " LEFT JOIN (SELECT * FROM wish_list) AS table2 ON products.id=table2.product_id and customer_id="
				+ customer_id + ") AS table3 INNER JOIN best_product ON table3.id=best_product.product_id";

		String sql_deal1 = "SELECT didtinct(table1.id),(CASE WHEN wish_list.product_id IS NOT NULL THEN 1 ELSE 0 END)AS wishlist FROM(\n"
				+ "SELECT p.id,p.name,p.regular_price,p.sale_price,p.stock,p.image,deals.start_date,deals.end_date FROM products AS p INNER JOIN deal_detail AS d ON p.id=d.product_id  INNER JOIN deals ON d.deal_id=deals.id WHERE (deals.end_date/1000) > UNIX_TIMESTAMP())AS table1 \n"
				+ "INNER JOIN wish_list ON table1.id=wish_list.product_id";
				
		String sql_deal="SELECT DISTINCT (table1.id),table1.name,table1.regular_price,table1.sale_price,table1.stock,table1.image,table1.start_date,table1.end_date,(CASE WHEN wish_list.product_id IS NOT NULL THEN 1 ELSE 0 END)AS wishlist FROM(SELECT p.id,p.name,p.regular_price,p.sale_price,p.stock,p.image,deals.start_date,deals.end_date FROM products AS p LEFT JOIN deal_detail AS d ON p.id=d.product_id  LEFT JOIN deals ON d.deal_id=deals.id WHERE (deals.end_date/1000) > UNIX_TIMESTAMP()) AS table1 LEFT JOIN wish_list ON table1.id=wish_list.product_id";	
				
		sql_deal="SELECT table1.*,(CASE WHEN table2.wishlist THEN 1 ELSE 0 END)AS wishlist FROM\r\n" + 
				"(SELECT p.id,p.name,p.regular_price,p.sale_price,p.stock,p.image,deals.start_date,deals.end_date FROM products AS p LEFT JOIN deal_detail AS d ON p.id=d.product_id  LEFT JOIN deals ON d.deal_id=deals.id WHERE (deals.end_date/1000) > UNIX_TIMESTAMP())AS table1\r\n" + 
				"LEFT JOIN (\r\n" + 
				"SELECT id,product_id,customer_id,(CASE WHEN wish_list.customer_id='"+customer_id+"' THEN 1 ELSE 0 END)AS wishlist FROM wish_list WHERE customer_id='"+customer_id+"')AS table2 ON table1.id=table2.product_id;\r\n" + 
				"";	
   
		logger.log("\n sql_deal" + sql_deal);
		
		String banner_sql = "SELECT id,NAME,image FROM banners WHERE STATUS=1";

		try {

			ResultSet resultSet3 = stmt.executeQuery(banner_sql);
			JSONArray json_array_home_banner = new JSONArray();
			while (resultSet3.next()) {

				JSONObject jo_banner = new JSONObject();

				jo_banner.put("id", resultSet3.getString("id"));
				jo_banner.put("name", resultSet3.getString("name"));
				jo_banner.put("image", resultSet3.getString("image"));
				json_array_home_banner.add(jo_banner);
			}

			jsonObject_final.put("banner", json_array_home_banner);

			resultSet3.close();
		} catch (Exception e) {
			e.printStackTrace();
			logger.log("Invoked JDBCSample banner");
		}

		try {

			ResultSet resultSet4 = stmt.executeQuery(sql_deal);
			JSONArray json_array_deal = new JSONArray();

			while (resultSet4.next()) {
				JSONObject jo_Home_deal = new JSONObject();

				jo_Home_deal.put("id", resultSet4.getString("id"));
				jo_Home_deal.put("name", resultSet4.getString("name"));
				jo_Home_deal.put("regular_price", resultSet4.getFloat("regular_price"));
				jo_Home_deal.put("sale_price", resultSet4.getFloat("sale_price"));
				jo_Home_deal.put("stock", resultSet4.getString("stock"));
				jo_Home_deal.put("image", resultSet4.getString("image"));
				jo_Home_deal.put("wishlist", resultSet4.getString("wishlist"));
				

				json_array_deal.add(jo_Home_deal);

				// ---------------------------code for time
				// remaining---------------------------------------------------------------------------------

				SimpleDateFormat f1 = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
				String s1 = null;
				Date d2, d3 = null;

				String dateStop = resultSet4.getString("end_date");
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

						long diffSeconds = diff / 1000 % 60;
						long diffMinutes = diff / (60 * 1000) % 60;
						long diffHours = diff / (60 * 60 * 1000) % 24;
						long diffDays = diff / (24 * 60 * 60 * 1000);

						s1 = Long.toString(diffDays) + " days " + Long.toString(diffHours) + " hours "
								+ Long.toString(diffMinutes) + " minutes " + Long.toString(diffSeconds) + " seconds ";

						jo_Home_deal.put("time_remaining", s1);
					} else {
						System.out.println("deal is over");
						jo_Home_deal.put("time_remaining", "deal is over");
					}
				} catch (Exception e) {
					e.printStackTrace();
					logger.log("Invoked JDBCSample deal");
				}

				// --------------------------------code end of time
				// remaining------------------------------------------------------------------

			}

			jsonObject_final.put("deals", json_array_deal);

			resultSet4.close();
		} catch (Exception e) {
			e.printStackTrace();

		}

		// Get time from DB server
		try {

			ResultSet resultSet = stmt.executeQuery(sql_top_picks);
			JSONArray json_array_home_top_picks = new JSONArray();
			while (resultSet.next()) {

				JSONObject jo_Home_topPiks = new JSONObject();

				jo_Home_topPiks.put("id", resultSet.getString("id"));
				jo_Home_topPiks.put("name", resultSet.getString("name"));
				jo_Home_topPiks.put("regular_price", resultSet.getFloat("regular_price"));
				jo_Home_topPiks.put("sale_price", resultSet.getFloat("sale_price"));
				jo_Home_topPiks.put("wish_list", resultSet.getInt("wishlist"));
				jo_Home_topPiks.put("image", resultSet.getString("image"));
				json_array_home_top_picks.add(jo_Home_topPiks);
			}

			jsonObject_final.put("top_picks", json_array_home_top_picks);
			resultSet.close();
		} catch (Exception e) {
			e.printStackTrace();
			logger.log("Invoked JDBCSample top_picks");

		}

		try {

			ResultSet resultSet1 = stmt.executeQuery(sql_best);
			JSONArray json_array_home_best = new JSONArray();
			while (resultSet1.next())

			{

				JSONObject jo_Home_bestProduct = new JSONObject();

				jo_Home_bestProduct.put("id", resultSet1.getString("id"));
				jo_Home_bestProduct.put("name", resultSet1.getString("name"));
				jo_Home_bestProduct.put("regular_price", resultSet1.getFloat("regular_price"));
				jo_Home_bestProduct.put("sale_price", resultSet1.getFloat("sale_price"));
				jo_Home_bestProduct.put("wish_list", resultSet1.getInt("wishlist"));
				jo_Home_bestProduct.put("image", resultSet1.getString("image"));
				json_array_home_best.add(jo_Home_bestProduct);
			}
			jsonObject_final.put("best_product", json_array_home_best);
			resultSet1.close();
		} catch (Exception e) {
			e.printStackTrace();
			logger.log("Invoked JDBCSample best_product");

		}
		try {
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		jsonObject_final.put("Currency","INR");
		return jsonObject_final;
	}

}
