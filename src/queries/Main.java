package queries;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;


public class Main {
	public static void main(String[] args) {
//		SearchCritiria search = new SearchCritiria("1", "1", "1");
//		search.setLowBudget("100");
//		search.setHighBudget("200");
//		search.setLowTemperature("50");
//		search.setHighTemperature("100");
//		search.setDepatureMonth("march");
//		search.setComeFrom("Boston");
//		search.setRadius("500");
//		List<CityWithScore> result = new Query().getCities(search);
//		System.out.println("the size of list is: " + result.size());
//		for (CityWithScore city: result) {
//			System.out.println(city.getCity());
//			System.out.println(city.getScore());
//		}
		List<Business> list = new Query().getBusinessByCity("Boston", "restaurant", "chinese");
		for (Business business: list) {
			System.out.println(business.getName() + " " + business.getCateroty());
		}
	}
}

class GetConnection {
	private String user;
	private String pass;
	private String database;
	
	public GetConnection(String user, String pass, String database) {
		this.user = user;
		this.pass = pass;
		this.database = database;
	}
	public Connection getConnection() throws SQLException, ClassNotFoundException {
		Class.forName("com.mysql.jdbc.Driver");
		Connection conn = null;
		Properties connectionProps = new Properties();
		connectionProps.put("user", this.user);
		connectionProps.put("password", this.pass);
		conn = DriverManager.getConnection("jdbc:" + "mysql" + "://" + "localhost" + ":" + "3306/" + database, connectionProps);
		System.out.println("get connected to database " + database);
		return conn;
	}
}

class Query {
	private Connection conn;
	
	private void getConnection() {
		String user = "root";
		String password = "Cc2042266";
		String database = "testdatabase";
		try {
			conn = new GetConnection(user, password, database).getConnection();
		} catch (SQLException se) {
			System.err.println("connection failed");
		} catch (ClassNotFoundException ex) {
			ex.printStackTrace();
		}
	}
	
	public List<Business> getBusinessByCity(String city, String tablename, String keyword) {
		getConnection();
		String query = "select * from " + tablename.toLowerCase() + " where city = '" + city.toLowerCase() + "' and category like '%" + keyword.toLowerCase() + "%';";
		//System.out.println(query);
		List<Business> list = new ArrayList<Business>();
		try {
			Statement st = conn.createStatement();
			ResultSet rs = st.executeQuery(query);
			while (rs.next()) {
				Business business = new Business();
				list.add(business);
				business.setName(rs.getString("name"));
				business.setAddress(rs.getString("address"));
				business.setPhone(rs.getString("phone"));
				business.setRating("" + rs.getFloat("rating"));
				business.setUrl(rs.getString("url"));
				business.setCategory(rs.getString("category"));
				business.setReview(rs.getString("review"));
			}
		} catch (SQLException se) {
			se.printStackTrace();
		}
		return list;
	}
	
	public List<CityWithScore> getCities(SearchCritiria critiria) {
		getConnection();
		//first load cities from file
		String path = "queryResult/part-r-" + critiria.getRestaurantRating() + "" + critiria.getLodgingRating() + "" + critiria.getPointsRating() + ".txt";
		//String path = "part-r-" + critiria.getRestaurantRating() + critiria.getLodgingRating() + critiria.getPointsRating() + ".txt";
		System.out.println(path);
		try {
			BufferedReader buff = new BufferedReader(new FileReader(path));
			ArrayList<CityWithScore> list = new ArrayList<CityWithScore>();
			String line = "";
			while ((line = buff.readLine()) != null) {
				//System.out.println(line);
				String[] tokens = line.split("\t");
				list.add(new CityWithScore(tokens[0], tokens[1]));
			}
			//System.out.println(list.size());
			Statement st = conn.createStatement();
			
			//first filter out cities by budget
			if (!critiria.getLowBudget().equals("-1")) {
				//query from database to get all budget matching
				String budgetQuery = "select city from location where budget >= " + critiria.getLowBudget() + " and budget < " + critiria.getHighBudget() + ";";
				//System.out.println(budgetQuery);
				ResultSet budgetResult = st.executeQuery(budgetQuery);
				HashSet<String> cityBudget = new HashSet<String>();
				while (budgetResult.next()) {
					cityBudget.add(budgetResult.getString("city"));
				}
				System.out.println(cityBudget.size());
				//remove cities that does not in city budget set
				int index = 0;
				while (index < list.size()) {
					//System.out.println(cityBudget.contains("austin"));
					//System.out.println(list.get(index).getCity());
					if (cityBudget.contains(list.get(index).getCity().toLowerCase())) {
						//System.out.println(list.get(index).getCity());
						index++;
					} else {
						list.remove(index);
					}
				}
				System.out.println("size after budget is: " + list.size());
			}
			
			//second filter out cities by temperature
			if (!critiria.getLowTemperature().equals("-1")) {
				System.out.println("program goes into temperature");
				String temperatureQuery = "select city from weather where " + critiria.getDepatureMonth().toLowerCase() + " >= " + critiria.getLowTemperature() + 
						" and " + critiria.getDepatureMonth().toLowerCase() + " < " + critiria.getHighTemperature() + ";";
				System.out.println(temperatureQuery);
				ResultSet temperatureResult = st.executeQuery(temperatureQuery);
				HashSet<String> cityTemperature = new HashSet<String>();
				while (temperatureResult.next()) {
					cityTemperature.add(temperatureResult.getString("city"));
				}
				//remove cities that does not in city budget set
				int index = 0;
				while (index < list.size()) {
					if (cityTemperature.contains(list.get(index).getCity().toLowerCase())) {
						index++;
					} else {
						list.remove(index);
					}
				}
			}
			
			//finally filter out cities by radius
			if (!critiria.getComeFrom().equals("-1")) {
				int index = 0;
				String comeFrom = critiria.getComeFrom().toLowerCase();
				double baseLatitude = getLatitude(conn, comeFrom);
				double baseLongitude = getLongitude(conn, comeFrom);
				double distanceLimit = Double.parseDouble(critiria.getRadius());
				while (index < list.size()) {
					String cityName = list.get(index).getCity().toLowerCase();
					double cityLatitude = getLatitude(conn, cityName);
					double cityLongitude = getLongitude(conn, cityName);
					if (distFrom(cityLatitude, cityLongitude, baseLatitude, baseLongitude) <= distanceLimit) {
						index++;
					} else {
						list.remove(index);
					}
				}
			}
			return list;
		} catch (FileNotFoundException ex) {
			System.err.println("file not found");
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException se) {
			se.printStackTrace();
		} finally {
			//return null;
		}
		return null;
	}
	
	private double getLongitude(Connection conn, String city) {
		String query = "select longitude from location where city = '" + city.toLowerCase() + "';";
		//System.out.println(query);
		try {
			Statement st = conn.createStatement();
			ResultSet rs = st.executeQuery(query);
			while (rs.next()) {
				return rs.getFloat("longitude");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return -1;
	}
	
	private double getLatitude(Connection conn, String city) {
		String query = "select latitude from location where city = '" + city.toLowerCase() + "';";
		try {
			Statement st = conn.createStatement();
			ResultSet rs = st.executeQuery(query);
			while (rs.next()) {
				return rs.getFloat("latitude");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return -1;
	}
	
	private double distFrom(double lat1, double lng1, double lat2, double lng2) {
		double earthRadius = 6731000;
		double dLat = Math.toRadians(lat2 - lat1);
		double dLng = Math.toRadians(lng2 - lng1);
		double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
	               Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
	               Math.sin(dLng/2) * Math.sin(dLng/2);
	    double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
	    double dist = (double) (earthRadius * c) / 1609.34;	//convert meter to mile

	    return dist;
	}
}

class Business {
	private String name;
	private String phone;
	private String address;
	private String url;
	private String rating;
	private String category;
	private String review;
	
	public void setName(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
	
	public void setPhone(String phone) {
		this.phone = phone;
	}
	
	public String getPhone() {
		return phone;
	}
	
	public void setAddress(String address) {
		this.address = address;
	}
	
	public String getAddress() {
		return address;
	}
	
	public void setRating(String rating) {
		this.rating = rating;
	}
	
	public String getRating() {
		return rating;
	}
	
	public void setUrl(String url) {
		this.url = url;
	}
	
	public String getUrl() {
		return url;
	}
	
	public void setCategory(String category) {
		this.category = category;
	}
	
	public String getCateroty() {
		return category;
	}
	
	public void setReview(String review) {
		this.review = review;
	}
	
	public String getReview() {
		return review;
	}
}

class CityWithScore {
	private String city;
	private String score;
	
	public CityWithScore(String city, String score) {
		this.city = city;
		this.score = score;
	}
	
	public String getCity() {
		return city;
	}
	
	public String getScore() {
		return score;
	}
}

class SearchCritiria {
	private String restaurantRating;
	private String lodgingRating;
	private String pointsRating;
	private String comeFrom;
	private String radius;
	private String lowBudget;
	private String highBudget;
	private String lowTemperature;
	private String highTemperature;
	private String depatureMonth;
	
	public SearchCritiria(String restaurant, String lodging, String points) {
		this.restaurantRating = restaurant;
		this.lodgingRating = lodging;
		this.pointsRating = points;
		this.comeFrom = "-1";
		this.radius = "-1";
		this.lowBudget = "-1";
		this.highBudget = "-1";
		this.lowTemperature = "-1";
		this.highTemperature = "-1";
		this.depatureMonth = "-1";
	}
	
	public void setComeFrom(String comeFrom) {
		this.comeFrom = comeFrom;
	}
	
	public String getComeFrom() {
		return comeFrom;
	}
	
	public void setRadius(String radius) {
		this.radius = radius;
	}
	
	public String getRadius() {
		return radius;
	}
	
	public void setLowBudget(String lowBudget) {
		this.lowBudget = lowBudget;
	}
	
	public String getLowBudget() {
		return lowBudget;
	}
	
	public void setHighBudget(String highBudget) {
		this.highBudget = highBudget;
	}
	
	public String getHighBudget() {
		return highBudget;
	}
	
	public void setLowTemperature(String lowTemperature) {
		this.lowTemperature = lowTemperature;
	}
	
	public String getLowTemperature() {
		return lowTemperature;
	}
	
	public void setHighTemperature(String highTemperature) {
		this.highTemperature = highTemperature;
	}
	
	public String getHighTemperature() {
		return highTemperature;
	}
	
	public void setRestaurantRating(String restaurantRating) {
		this.restaurantRating = restaurantRating;
	}
	
	public String getRestaurantRating() {
		return restaurantRating;
	}
	
	public void setLodgingRating(String lodgingRating) {
		this.lodgingRating = lodgingRating;
	}
	
	public String getLodgingRating() {
		return lodgingRating;
	}
	
	public void setPointsRating(String pointsRating) {
		this.pointsRating = pointsRating;
	}
	
	public String getPointsRating() {
		return pointsRating;
	}
	
	public void setDepatureMonth(String depatureMonth) {
		this.depatureMonth = depatureMonth;
	}
	
	public String getDepatureMonth() {
		return depatureMonth;
	}
}
