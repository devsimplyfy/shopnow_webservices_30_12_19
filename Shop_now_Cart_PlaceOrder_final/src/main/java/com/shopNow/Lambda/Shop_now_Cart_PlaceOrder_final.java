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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class Shop_now_Cart_PlaceOrder_final implements RequestHandler<JSONObject, JSONObject> {

	private String USERNAME;
	private String PASSWORD;
	private String DB_URL;
	private String DB;

	@SuppressWarnings({ "unchecked", "unused", "null" })
	public JSONObject handleRequest(JSONObject input, Context context) {

		LambdaLogger logger = context.getLogger();

		JSONObject errorPayload = new JSONObject();

		if (!input.containsKey("userid")) {
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'userid' is missing");
			throw new RuntimeException(errorPayload.toJSONString());
		}
		if (!input.containsKey("transaction_id")) {
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'transaction_id' is missing");
			throw new RuntimeException(errorPayload.toJSONString());
		}
		if (!input.containsKey("paymentMode")) {
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'paymentMode' is missing");
			throw new RuntimeException(errorPayload.toJSONString());
		}
		if (!input.containsKey("payment_gateway")) {
			errorPayload.put("errorType", "BadRequest");
			errorPayload.put("httpStatus", 400);
			errorPayload.put("requestId", context.getAwsRequestId());
			errorPayload.put("message", "JSON Input Object request key named 'payment_gateway' is missing");
			throw new RuntimeException(errorPayload.toJSONString());
		}

		JSONArray cart_Add_array = new JSONArray();
		JSONObject jsonObject_placeorder_result = new JSONObject();

		Object userid1 = input.get("userid");

		float sub_total = 0;
		float total_promo_Discount = 0, tax = 0, shipping = 0, Grand_Total = 0, final_sub_total = 0, Sub_total1 = 0;
		String orderNumber = null;
		String promocode_id = null, pro_delete_id = null, vendor_id_ext = null;
		Connection conn = null;

		String first_name = null, last_name = null, address1 = null, address2 = null, address3 = null, city = null,
				state = null, country = null, email_address = null;
		int pincode = 0, delevery_address_id = 0;
		long phoneNumber = 0;

		int quantity = 0, product_quantity = 0, productId = 0, i = 0, vendor_id_isexternal = 0;
		float sale_price;
		String attribute_value, vendorId = null;
		String delivery_address;
		String orderId = null;
		String name;
		String day = null;

		String city_buyer = null, state_buyer = null, country_buyer = null, zone = null;
		int is_metro = 0, is_special_destination = 0, is_RoI_A = 0, is_RoI_B = 0;

		String city_seller = null, zone_seller = null, state_seller = null, country_seller = null;
		int is_metro_seller = 0, is_special_destination_seller = 0, is_RoI_A_seller = 0, is_RoI_B_seller = 0;

		int flag = 0;

		float shipping_charge = 0;
		int plan_id = 0, courier_id = 0;

		String transaction_id = input.get("transaction_id").toString();
		String paymentMode = input.get("paymentMode").toString();
		String payment_gateway = input.get("payment_gateway").toString();
		// String courier_name = input.get("courier_name").toString();
		// String category_name = input.get("category_name").toString();
		String courier_name = "BlueDart";
		String category_name = "Standard";

		if (paymentMode.equalsIgnoreCase("COD")) {
			flag = 1;
			transaction_id = "-1";
			payment_gateway = "-1";

		} else if (paymentMode.equalsIgnoreCase("internet Banking") || paymentMode.equalsIgnoreCase("Credit Card")
				|| paymentMode.equalsIgnoreCase("Debit Card")) {
			flag = 0;
			transaction_id = input.get("transaction_id").toString();
			payment_gateway = input.get("payment_gateway").toString();

		} else {
			jsonObject_placeorder_result.put("status", "0");
			jsonObject_placeorder_result.put("message", "Please Enter valid payment mode! Thank You Try again");
			return jsonObject_placeorder_result;

		}

		String Str_msg;

		if (userid1 == null || userid1 == "") {

			Str_msg = "Order not placed successfully because userID is null";
			jsonObject_placeorder_result.put("status", "0");
			jsonObject_placeorder_result.put("message", Str_msg);
			return jsonObject_placeorder_result;

		}
		long userid = Long.parseLong(userid1.toString());
		if (flag == 0) {

			if (transaction_id == null || transaction_id == "") {

				Str_msg = "Order not placed successfully because transaction_id is null";
				jsonObject_placeorder_result.put("status", "0");
				jsonObject_placeorder_result.put("message", Str_msg);
				return jsonObject_placeorder_result;

			}
			if (payment_gateway == null || payment_gateway == "") {

				Str_msg = "Order not placed successfully because payment_gateway is null";
				jsonObject_placeorder_result.put("status", "0");
				jsonObject_placeorder_result.put("message", Str_msg);
				return jsonObject_placeorder_result;

			}
		}

		if (courier_name.equalsIgnoreCase("null")) {

			Str_msg = "courier_name cannot be null !! ";
			jsonObject_placeorder_result.put("status", "0");
			jsonObject_placeorder_result.put("message", Str_msg);
			return jsonObject_placeorder_result;

		}

		if (category_name.equalsIgnoreCase("null") || input.get("category_name") == "") {
			category_name = "Standard";
		}

		// -----database connection-----------

		Properties prop = new Properties();

		try {
			prop.load(getClass().getResourceAsStream("/application.properties"));
			DB_URL = prop.getProperty("url");
			USERNAME = prop.getProperty("username");
			PASSWORD = prop.getProperty("password");
			conn = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
		} catch (Exception e) {
			e.printStackTrace();
			logger.log("Caught exception: " + e.getMessage());

		}

		try {

			Statement stmt_customer = conn.createStatement();
			ResultSet srs_customer = stmt_customer
					.executeQuery("SELECT id, status FROM customers where id='" + userid+"'" );

			if (srs_customer.next() == false) {

				Str_msg = "No user found!";
				jsonObject_placeorder_result.put("status", "0");
				jsonObject_placeorder_result.put("message", Str_msg);
				return jsonObject_placeorder_result;
			}
			
			else if(!srs_customer.getString("status").equalsIgnoreCase("1")){
				Str_msg = "User not confirmed !";
				jsonObject_placeorder_result.put("status", "0");
				jsonObject_placeorder_result.put("message", Str_msg);
				return jsonObject_placeorder_result;
			}
			
			srs_customer.close();
			stmt_customer.close();

			Statement stmt = conn.createStatement();
			ResultSet srs_product_id = stmt.executeQuery("SELECT * FROM cart_items where UserId='" + userid + "'");

			if (srs_product_id.next() == false) {

				Str_msg = "Product is not present in cart";
				jsonObject_placeorder_result.put("status", "0");
				jsonObject_placeorder_result.put("message", Str_msg);
				return jsonObject_placeorder_result;
			}
			srs_product_id.close();

			Statement stmt_item = conn.createStatement();
		/*	ResultSet srs_item = stmt_item.executeQuery(
					"SELECT products.id FROM cart_items LEFT JOIN products ON products.id=cart_items.productId WHERE UserId='"
							+ userid + "' and products.stock='true'");*/
		
			String Sql_product_cart="SELECT products.id,products.stock FROM cart_items LEFT JOIN products ON products.id=cart_items.productId WHERE UserId='" + userid +"'";
			logger.log("\n Sql_product_cart\n "+Sql_product_cart);
			ResultSet srs_item = stmt_item.executeQuery(Sql_product_cart);

			ArrayList<Integer> arrlist_for_NA_products = new ArrayList<Integer>();
			while (srs_item.next()) {

				String result = srs_item.getString("stock");
				
				if (result.equals("false")) {
					arrlist_for_NA_products.add(srs_item.getInt("id"));
		/*			Str_msg = "Currently product "+srs_item.getInt("id")+" is out of stock. We request you to first remove that product from cart and then place your order. Thank You !";
					jsonObject_placeorder_result.put("status", "0");
					jsonObject_placeorder_result.put("message", Str_msg);
					return jsonObject_placeorder_result;
*/
				}

			}
			
		if(!arrlist_for_NA_products.isEmpty()) {
				String out_of_stock_products = arrlist_for_NA_products.get(0).toString();
				out_of_stock_products=out_of_stock_products.concat(",");
				for (int b = 1; b < arrlist_for_NA_products.size(); b++) {
					
					logger.log(arrlist_for_NA_products.get(b).toString());
					String prods = arrlist_for_NA_products.get(b).toString();
					out_of_stock_products=out_of_stock_products.concat(prods);
					out_of_stock_products=out_of_stock_products.concat(",");
					

				}
				
				logger.log("\nHERE\n");
				logger.log(out_of_stock_products);
				logger.log("\nTHERE\n");

				Str_msg = "Currently product/s "+out_of_stock_products.substring(0, out_of_stock_products.length()-1)+" is/are out of stock. We request you to first remove from cart and then place your order. Thank You !";
				jsonObject_placeorder_result.put("status", "0");
				jsonObject_placeorder_result.put("message", Str_msg);
				return jsonObject_placeorder_result;

			}
			srs_item.close();
			stmt_item.close();
			
			

			Statement stmt_couriers = conn.createStatement();
			ResultSet resultSet_couriers = stmt_couriers.executeQuery(
					"SELECT * FROM couriers WHERE courier_name='" + courier_name + "' AND couriers.status='1'");
			if (resultSet_couriers.next() == false) {

				Str_msg = "Either courier_name is not present or courier service is not available !! ";
				jsonObject_placeorder_result.put("status", "0");
				jsonObject_placeorder_result.put("message", Str_msg);
				return jsonObject_placeorder_result;

			}
			int couriers_id = resultSet_couriers.getInt("id");
			resultSet_couriers.close();

			Statement stmt_category_name = conn.createStatement();
			ResultSet resultSet_category_name = stmt_category_name
					.executeQuery("SELECT * FROM shipping_categories WHERE category_name='" + category_name
							+ "' AND courier_id=" + couriers_id);
			if (resultSet_category_name.next() == false) {

				Str_msg = "Either category_name is not present or this courier does not provide this service !! ";
				jsonObject_placeorder_result.put("status", "0");
				jsonObject_placeorder_result.put("message", Str_msg);
				return jsonObject_placeorder_result;

			}
			resultSet_category_name.close();
			stmt_category_name.close();

			Statement stmt_vendor = conn.createStatement();
			ResultSet resultSet_vendor = stmt_vendor.executeQuery("SELECT * FROM admin WHERE is_external='0'");

			int vendo_id = 0;

			ArrayList<Integer> arrlist = new ArrayList<Integer>();
			while (resultSet_vendor.next()) {

				vendo_id++;
				vendor_id_isexternal = resultSet_vendor.getInt("id");
				arrlist.add(vendor_id_isexternal);

			}
			logger.log("\nSTARTS HERE\n");
			for (int a = 0; a < arrlist.size(); a++) {

				System.out.print(arrlist.get(a) + " ");

			}
			logger.log("\nENDS HERE\n");

			resultSet_vendor.close();
			stmt_vendor.close();

			ResultSet srs_user_id = stmt
					.executeQuery("SELECT * FROM address WHERE customerId='" + userid + "' AND isPrimary=1");

			if (srs_user_id.next()) {

				first_name = srs_user_id.getString("firstName");
				last_name = srs_user_id.getString("lastName");
				address1 = srs_user_id.getString("address1");
				address2 = srs_user_id.getString("address2");
				address3 = srs_user_id.getString("address3");
				city = srs_user_id.getString("city");
				state = srs_user_id.getString("state");
				country = srs_user_id.getString("country");
				email_address = srs_user_id.getString("email_address");
				pincode = srs_user_id.getInt("pincode");
				phoneNumber = srs_user_id.getLong("phoneNumber");

			} else {

				Str_msg = "Call Insert Address web service";
				jsonObject_placeorder_result.put("status", "0");
				jsonObject_placeorder_result.put("message", Str_msg);
				return jsonObject_placeorder_result;

			}

			// -------------------------------------------------------------
			Statement stmt_pincode_buyer = conn.createStatement();
			ResultSet resultSet_pin = stmt_pincode_buyer
					.executeQuery("SELECT * FROM pincodes WHERE pincode=" + pincode);
			if (resultSet_pin.next() == false) {

				Str_msg = "Either pincode is not valid or products are not available for this pincode !! ";
				jsonObject_placeorder_result.put("status", "0");
				jsonObject_placeorder_result.put("message", Str_msg);
				return jsonObject_placeorder_result;

			}
			resultSet_pin.close();

			String pincode_Sql = "SELECT * FROM pincodes where pincode =" + pincode;
			logger.log("\n pincode_Sql \n" + pincode_Sql);

			ResultSet resultSet = stmt_pincode_buyer.executeQuery(pincode_Sql);
			JSONObject jsonObject_pincode = new JSONObject();
			while (resultSet.next()) {

				jsonObject_pincode.put("id", resultSet.getInt("id"));
				city_buyer = resultSet.getString("city");
				zone = resultSet.getString("zone");

				state_buyer = resultSet.getString("state");
				country_buyer = resultSet.getString("country");

				is_metro = resultSet.getInt("is_metro");
				is_special_destination = resultSet.getInt("is_special_destination");
				is_RoI_A = resultSet.getInt("is_RoI_A");
				is_RoI_B = resultSet.getInt("is_RoI_B");

			}

			resultSet.close();
			stmt_pincode_buyer.close();

			// ---------------------------------------------------------

			Statement stmt_address_insert = conn.createStatement();
			String sql_address_insert = "insert into customer_order_address(customer_id,first_name,last_name,address1,address2,address3,city,state,country,email_address,pincode,phoneNumber)values"
					+ "('" + userid + "','" + first_name + "','" + last_name + "','" + address1 + "','" + address2
					+ "','" + address3 + "','" + city + "','" + state + "','" + country + "','" + email_address + "',"
					+ pincode + "," + phoneNumber + ")";

			logger.log("\n customer_order_address \n" + sql_address_insert);
			int insert_add = stmt_address_insert.executeUpdate(sql_address_insert);

			stmt_address_insert.close();

			if (insert_add > 0) {
				String sql_select_address_id = "SELECT id FROM customer_order_address WHERE customer_id='" + userid
						+ "' AND first_name='" + first_name + "' AND last_name='" + last_name + "' AND address1='"
						+ address1 + "' AND pincode='" + pincode + "'  ORDER BY id DESC LIMIT 1";
				Statement stmt_select_address_id = conn.createStatement();
				ResultSet srs_select_address = stmt_select_address_id.executeQuery(sql_select_address_id);
				if (srs_select_address.next()) {
					delevery_address_id = srs_select_address.getInt("id");

				}

			} else {

				Str_msg = "Please Call place_order again";
				jsonObject_placeorder_result.put("status", "0");
				jsonObject_placeorder_result.put("message", Str_msg);
				return jsonObject_placeorder_result;

			}

			// ----------------Logic for Choose Payment Option----------------

			if (paymentMode.isEmpty()) {

				Str_msg = "Please Enter valid payment mode not empty! Thank You Try again";
				jsonObject_placeorder_result.put("status", "0");
				jsonObject_placeorder_result.put("message", Str_msg);

				return jsonObject_placeorder_result;

			}

			// --------------Logic For Create new Uniqe Order_id--&& order
			// Number---------------------

			long current = System.currentTimeMillis();
			// orderId = Long.toString(current) + "_" + Long.toString(userid);
			int iRandom = new Random().nextInt(900000) + 100000;
			orderNumber = Long.toString(current) + "_" + Long.toString(iRandom);

			String sql = "SELECT products.*,table1.attribute_value,cart_items.UserId,cart_items.Quantity,(cart_items.Quantity * products.sale_price) AS total  FROM products LEFT JOIN \n"
					+ "(SELECT pa.product_id,GROUP_CONCAT(att_group_name,'\":\"',av.att_value) AS attribute_value FROM product_attributes pa INNER JOIN attributes_value av ON av.id=pa.att_group_val_id INNER JOIN attributes a ON a.id=pa.att_group_id GROUP BY pa.product_id) AS table1 ON products.id=table1.product_id \n"
					+ " LEFT JOIN cart_items ON cart_items.ProductId=products.id  WHERE  cart_items.UserId=" + userid
					+ " GROUP BY products.id";

			sql = "SELECT products.*,cart_items.UserId,cart_items.Quantity AS cart_quantity,(cart_items.Quantity * products.sale_price) AS total  FROM products LEFT JOIN \r\n"
					+ "cart_items ON cart_items.ProductId=products.id  WHERE cart_items.UserId='" + userid
					+ "' AND products.stock='true' GROUP BY products.id";

			logger.log("\n sql insert into order_detail  =\n" + sql);

			Statement stmt1 = conn.createStatement();
			ResultSet srs_orderPlace = stmt1.executeQuery(sql);
			int Pid = 0;
			while (srs_orderPlace.next()) {

				vendorId = srs_orderPlace.getString("vendor_id");
				int toCheck = Integer.parseInt(vendorId);
				String pro_id;
				if (arrlist.contains(toCheck)) {
					product_quantity = srs_orderPlace.getInt("quantity");
					quantity = srs_orderPlace.getInt("cart_quantity");
					name = srs_orderPlace.getString("name");
					sale_price = srs_orderPlace.getFloat("sale_price");
					String vendor_product_id = srs_orderPlace.getString("vendor_product_id");
					sub_total = sub_total + Float.parseFloat(srs_orderPlace.getString("total"));
					Date now = new Date();
					SimpleDateFormat simpleDateformat = new SimpleDateFormat("E");
					simpleDateformat = new SimpleDateFormat("EEEE"); // the day of the week spelled out completely
					day = simpleDateformat.format(now);

					Statement stmt_insert_customer_order_detail = conn.createStatement();

					if (product_quantity >= quantity) {

						Pid++;
						productId = srs_orderPlace.getInt("id");
						pro_id = Integer.toString(productId);

						if (Pid == 1) {
							pro_delete_id = pro_id;

						} else {
							pro_delete_id = pro_delete_id.concat("," + pro_id);
						}

						if (day == "Sunday") {

							logger.log("\n sql_place_order \n");

							String sql_place_order = "INSERT INTO customer_order_details(customer_id,product_id,delivery_status_code,vendor_id,vendor_product_id,quantity,discounts,price,expected_date_of_delivery,order_number,promocodes) VALUES\n"
									+ "(" + userid + "," + productId + ",'delivery_status_code','" + vendorId + "','"
									+ vendor_product_id + "'," + quantity + ",0," + sale_price
									+ ",DATE_ADD(Now(), INTERVAL 8 DAY),'" + orderNumber + "','promocode')";

							logger.log("\n sql_place_order \n" + sql_place_order);
							stmt_insert_customer_order_detail.executeUpdate(sql_place_order);

						} else {

							String sql_place_order = "INSERT INTO customer_order_details(customer_id,product_id,delivery_status_code,vendor_id,vendor_product_id,quantity,discounts,price,expected_date_of_delivery,order_number,promocodes) VALUES\n"
									+ "(" + userid + "," + productId + ",'delivery_status_code','" + vendorId + "','"
									+ vendor_product_id + "'," + quantity + ",0," + sale_price
									+ ",DATE_ADD(Now(), INTERVAL 7 DAY),'" + orderNumber + "','promocode')";

							logger.log(sql_place_order);
							stmt_insert_customer_order_detail.executeUpdate(sql_place_order);

						}

						product_quantity = product_quantity - quantity;
						stmt_insert_customer_order_detail.close();

						Statement stmt_update_product = conn.createStatement();
						String sql_product;

						if (product_quantity > 0) {

							sql_product = "UPDATE products set quantity=" + product_quantity + " where id=" + productId;

						} else {

							sql_product = "UPDATE products set quantity=0,stock='false' where id=" + productId;

						}

						logger.log("\n sql_product \n" + sql_product);

						stmt_update_product.executeUpdate(sql_product);
						stmt_update_product.close();

					} else {
						Str_msg = "Available product quantity for "+srs_orderPlace.getInt("id")+" is " + product_quantity + " and your order quantity is "
								+ quantity + ". Please update your cart accordingly and then place order. Thank You !";
						jsonObject_placeorder_result.put("status", "0");
						jsonObject_placeorder_result.put("message", Str_msg);
						return jsonObject_placeorder_result;
					}

				} else {

					quantity = srs_orderPlace.getInt("cart_quantity");
					name = srs_orderPlace.getString("name");
					sale_price = srs_orderPlace.getFloat("sale_price");
					String vendor_product_id = srs_orderPlace.getString("vendor_product_id");
					sub_total = sub_total + Float.parseFloat(srs_orderPlace.getString("total"));
					Date now = new Date();
					SimpleDateFormat simpleDateformat = new SimpleDateFormat("E");
					simpleDateformat = new SimpleDateFormat("EEEE"); // the day of the week spelled out completely
					day = simpleDateformat.format(now);

					Statement stmt_insert_customer_order_detail = conn.createStatement();
					if (quantity > 0) {

						Pid++;
						productId = srs_orderPlace.getInt("id");
						pro_id = Integer.toString(productId);

						if (Pid == 1) {
							pro_delete_id = pro_id;

						} else {
							pro_delete_id = pro_delete_id.concat("," + pro_id);
						}

						if (day == "Sunday") {

							logger.log("\n sql_place_order \n");

							String sql_place_order = "INSERT INTO customer_order_details(customer_id,product_id,delivery_status_code,vendor_id,vendor_product_id,quantity,discounts,price,expected_date_of_delivery,order_number,promocodes) VALUES\n"
									+ "(" + userid + "," + productId + ",'delivery_status_code','" + vendorId + "','"
									+ vendor_product_id + "'," + quantity + ",0," + sale_price
									+ ",DATE_ADD(Now(), INTERVAL 8 DAY),'" + orderNumber + "','promocode')";

							logger.log("\n sql_place_order \n" + sql_place_order);
							stmt_insert_customer_order_detail.executeUpdate(sql_place_order);

						} else {

							String sql_place_order = "INSERT INTO customer_order_details(customer_id,product_id,delivery_status_code,vendor_id,vendor_product_id,quantity,discounts,price,expected_date_of_delivery,order_number,promocodes) VALUES\n"
									+ "(" + userid + "," + productId + ",'delivery_status_code','" + vendorId + "','"
									+ vendor_product_id + "'," + quantity + ",0," + sale_price
									+ ",DATE_ADD(Now(), INTERVAL 7 DAY),'" + orderNumber + "','promocode')";

							logger.log(sql_place_order);
							stmt_insert_customer_order_detail.executeUpdate(sql_place_order);

						}

						product_quantity = product_quantity - quantity;
						stmt_insert_customer_order_detail.close();

					}

				}
				final_sub_total = sub_total;
				Sub_total1 = final_sub_total;
				logger.log("\n sub_total =\n" + sub_total);

			}

		} catch (Exception e) {
			e.printStackTrace();
			logger.log("Caught exception: " + e.getMessage());
			Str_msg = "error in place order !!" + e.getMessage();
			jsonObject_placeorder_result.put("status", "0");
			jsonObject_placeorder_result.put("message", Str_msg);

			return jsonObject_placeorder_result;

		}

		// -----------------------Logic for promo code--------------------------
		try {

			Statement stmt_promocodes = conn.createStatement();
			Statement stmt_insert_customer_orders = conn.createStatement();
			Statement stmt_delete_cart_items = conn.createStatement();
			Statement stmt_delete_promo_cart_items = conn.createStatement();

			String sql_promocode;

			JSONObject jo_promocode = new JSONObject();
			JSONArray json_array_promocode = new JSONArray();

			Date date = new Date();
			long time = date.getTime();

			sql_promocode = "SELECT promocodes.*,cart_promocode.* FROM cart_promocode INNER JOIN promocodes ON promocodes.id=cart_promocode.promocode_id WHERE user_id="
					+ userid + " and end_time>='" + time + "'and status=1";

			logger.log("\n promocode \n " + sql_promocode);

			ResultSet rs_promocode = stmt_promocodes.executeQuery(sql_promocode);

			int i1 = 0;
			while (rs_promocode.next()) {
				i1++;
				JSONObject jo_promocode1 = new JSONObject();
				String promocode = rs_promocode.getString("promo_value");
				float prom = Float.parseFloat(promocode);
				String type = rs_promocode.getString("promo_value_type");

				int temp = rs_promocode.getInt("id");
				String promocode_id1 = Integer.toString(temp);

				if (i1 == 1) {
					promocode_id = promocode_id1;

				} else {
					promocode_id = promocode_id.concat("," + promocode_id1);
				}

				float promovalue = 0;
				if (type.equals("Variable")) {

					Sub_total1 = Sub_total1 - (Sub_total1 * prom) / 100;

				} else {

					Sub_total1 = Sub_total1 - prom;

				}

				total_promo_Discount = total_promo_Discount + promovalue;
				json_array_promocode.add(jo_promocode1);

			}

			stmt_promocodes.close();
			float sub_total_final = final_sub_total;
			total_promo_Discount = final_sub_total - Sub_total1;

			// Logic for shipping Charge

			Statement stmt_vendor_shipping = conn.createStatement();
			String sql_srs_vendor = "SELECT DISTINCT (vendor_id) FROM customer_order_details WHERE order_number='"
					+ orderNumber + "'";
			ResultSet srs_vendor = stmt_vendor_shipping.executeQuery(sql_srs_vendor);

			ArrayList<Integer> arrlist_vendor = new ArrayList<Integer>();

			int vendor_id_isexternal1 = 0;
			while (srs_vendor.next()) {

				vendor_id_isexternal1 = srs_vendor.getInt("vendor_id");
				arrlist_vendor.add(vendor_id_isexternal1);

			}

			srs_vendor.close();
			stmt_vendor_shipping.close();

			int n = arrlist_vendor.size();

			for (int s = 0; s < n; s++) {

				int vendorId_charg = arrlist_vendor.get(s);
				String pincode_Sql_seller = "SELECT * FROM (SELECT * FROM vendor_address WHERE vendor_id ="
						+ vendorId_charg + " )AS table1 INNER JOIN pincodes ON pincodes.pincode=table1.pincode";

				logger.log("\n pincode_Sql_seller \n" + pincode_Sql_seller);

				Statement stmt_pincode_seller = conn.createStatement();
				ResultSet resultSet_seller = stmt_pincode_seller.executeQuery(pincode_Sql_seller);
				JSONObject jsonObject_pincode_seller = new JSONObject();
				while (resultSet_seller.next()) {

					jsonObject_pincode_seller.put("id", resultSet_seller.getInt("id"));

					// vendor_id=resultSet_seller.getInt("vendor_id");

					city_seller = resultSet_seller.getString("city");
					zone_seller = resultSet_seller.getString("zone");
					state_seller = resultSet_seller.getString("state");
					country_seller = resultSet_seller.getString("country");
					is_metro_seller = resultSet_seller.getInt("is_metro");
					is_special_destination_seller = resultSet_seller.getInt("is_special_destination");
					is_RoI_A_seller = resultSet_seller.getInt("is_RoI_A");
					is_RoI_B_seller = resultSet_seller.getInt("is_RoI_A");

				}

				resultSet_seller.close();
				stmt_pincode_seller.close();

				String city_type = null;

				if (city_buyer.equalsIgnoreCase(city_seller)) {

					city_type = "intra_city";

				} else if (zone.equalsIgnoreCase(zone_seller)) {
					city_type = "intra_zone";

				} else {

					logger.log("\n we are in else \n" + city_type);

					if ((is_metro & is_metro_seller) == 1) {

						city_type = "metro_to_metro";

					} else if (is_RoI_A == 1) {

						city_type = "roi_A";

					} else if (is_RoI_B == 1) {

						city_type = "roi_B";

					} else if (is_special_destination == 1) {

						city_type = "special_destination";

					} else {
						city_type = "null";

					}

				}

				logger.log("\n city_type \n" + city_type);

				String sql_shipping = "SELECT shipping_categories.*,couriers.courier_name FROM couriers  INNER JOIN shipping_categories ON shipping_categories.courier_id=couriers.id WHERE courier_name='"
						+ courier_name + "' AND category_name='" + category_name
						+ "' AND weight='0-500 gms' AND vendor_id='" + vendorId_charg + "'";

				logger.log("\n sql_shipping \n" + sql_shipping);

				Statement stmt_shipping = conn.createStatement();
				ResultSet sql_shipping1 = stmt_shipping.executeQuery(sql_shipping);

				if (sql_shipping1.next()) {

					shipping_charge = sql_shipping1.getFloat(city_type);
					plan_id = sql_shipping1.getInt("id");
					courier_id = sql_shipping1.getInt("courier_id");

				} else {

					Str_msg = "Sorry ! Currently this courier doesn't provide service to the said vendor !! ";
					jsonObject_placeorder_result.put("status", "0");

					jsonObject_placeorder_result.put("message", Str_msg);
					return jsonObject_placeorder_result;

				}

				sql_shipping1.close();
				stmt_shipping.close();

				shipping = shipping + shipping_charge;

			}

			Grand_Total = (final_sub_total - total_promo_Discount) + tax + shipping;

			logger.log("\n total_promo_Discount " + total_promo_Discount);
			logger.log("\n final_sub_total " + final_sub_total);
			logger.log("\n Grand_Total " + Grand_Total);
			logger.log("\n shipping_charge " + shipping_charge);

			if (Grand_Total > 0) {

				String sql1;
				if (flag == 1) {

					sql1 = "insert into customer_orders(order_number,customer_id,order_status_code,date_of_placed_order,payment_status,payment_gateway,transaction_id,authorization_id,sub_total,tax,shipping,grand_total,delivery_address_id,billing_address_id,promocode,discounts,mode_of_payment)values \n "
							+ "('" + orderNumber + "'," + userid + ",'Order placed',Now() ,'Remaining','"
							+ payment_gateway + "','" + transaction_id + "','-1'," + sub_total_final + "," + tax + ","
							+ shipping + "," + Grand_Total + "," + delevery_address_id + "," + delevery_address_id
							+ ",'" + promocode_id + "'," + total_promo_Discount + ",'" + paymentMode + "')";

				} else {

					sql1 = "insert into customer_orders(order_number,customer_id,order_status_code,date_of_placed_order,date_of_order_paid,payment_status,payment_gateway,transaction_id,authorization_id,sub_total,tax,shipping,grand_total,delivery_address_id,billing_address_id,promocode,discounts,mode_of_payment)values \n "
							+ "('" + orderNumber + "'," + userid + ",'Order placed',Now(), Now(),'Done','"
							+ payment_gateway + "','" + transaction_id + "','authorization_id'," + sub_total_final + ","
							+ tax + "," + shipping + "," + Grand_Total + "," + delevery_address_id + ","
							+ delevery_address_id + ",'" + promocode_id + "'," + total_promo_Discount + ",'"
							+ paymentMode + "')";

				}

				logger.log("\n customer_order" + sql1);
				stmt_insert_customer_orders.executeUpdate(sql1);
				stmt_insert_customer_orders.close();

				String sql_select_order_id = "SELECT order_id FROM customer_orders WHERE order_number='" + orderNumber
						+ "'";
				Statement stmt_select_order_id = conn.createStatement();
				ResultSet srs_select_order_id = stmt_select_order_id.executeQuery(sql_select_order_id);
				int order_id1 = 0;
				if (srs_select_order_id.next()) {
					order_id1 = srs_select_order_id.getInt("order_id");

				}
				srs_select_order_id.close();
				stmt_select_order_id.close();

				String sql_update = "SELECT order_id FROM customer_orders WHERE order_number='" + orderNumber + "'";
				Statement stmt_update_order_id = conn.createStatement();
				stmt_update_order_id.executeUpdate("UPDATE customer_order_details set order_id=" + order_id1
						+ " where order_number='" + orderNumber + "'");

				stmt_update_order_id.close();

				stmt_delete_cart_items.executeUpdate(
						"DELETE FROM cart_items where UserId='" + userid + "' and productId IN(" + pro_delete_id + ")");
				stmt_delete_cart_items.close();

				String sqldel = "DELETE FROM cart_promocode where user_id='" + userid + "'";
				stmt_delete_promo_cart_items.executeUpdate(sqldel);
				stmt_delete_promo_cart_items.close();

				String sql_insert_order_tracking;
				if (day == "Sunday") {
					sql_insert_order_tracking = "insert into order_tracking(vendor_id,courier_id,order_id,status,expected_date_of_delivery,plan_id)values("
							+ vendorId + "," + courier_id + "," + order_id1
							+ ",'Order placed',DATE_ADD(Now(), INTERVAL 8 DAY)," + plan_id + ")";
				} else {

					sql_insert_order_tracking = "insert into order_tracking(vendor_id,courier_id,order_id,status,expected_date_of_delivery,plan_id)values("
							+ vendorId + "," + courier_id + "," + order_id1
							+ ",'Order placed',DATE_ADD(Now(), INTERVAL 7 DAY)," + plan_id + ")";
				}

				logger.log("\n sql_insert_order_tracking \n " + sql_insert_order_tracking);

				Statement stmt_insert_order_tracking = conn.createStatement();
				stmt_insert_order_tracking.executeUpdate(sql_insert_order_tracking);
				stmt_insert_order_tracking.close();

			} else {

				Str_msg = "Grand Total not valid! Thank You !! ";
				jsonObject_placeorder_result.put("status", "0");
				jsonObject_placeorder_result.put("message", Str_msg);

				return jsonObject_placeorder_result;

			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.log("Caught exception: " + e.getMessage());
			Str_msg = "! Exception !! " + e.getMessage();
			jsonObject_placeorder_result.put("status", "0");
			jsonObject_placeorder_result.put("message", Str_msg);

			return jsonObject_placeorder_result;
		}

		Str_msg = "Order Placed Successfully! Thank You !! ";
		jsonObject_placeorder_result.put("status", "1");
		jsonObject_placeorder_result.put("message", Str_msg);

		return jsonObject_placeorder_result;

	}

}
