package com.hnb.memsql;

import java.sql.*;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class Main
{
    
    public static AtomicLong atomicLong;

    private static final String dbClassName = "com.mysql.jdbc.Driver";

    private static final String CONNECTION = "jdbc:mysql://52.187.49.1:3306/";

    private static final String USER = "root";

    private static final String PASSWORD = "";

    private static void executeSQL(Connection conn, String sql) throws SQLException
    {
	try (Statement stmt = conn.createStatement())
	{
	    stmt.execute(sql);
	}
    }

    private static void ResetEnvironment() throws SQLException
    {

	Properties p = new Properties();
	p.put("user", USER);
	p.put("password", PASSWORD);
	try (Connection conn = DriverManager.getConnection(CONNECTION, p))
	{
	    for (String query : new String[]
	    {
		    "DROP DATABASE IF EXISTS mydb", 
		    "CREATE DATABASE mydb", "USE mydb", 
		    "CREATE TABLE products (proId INT AUTO_INCREMENT PRIMARY KEY, name varchar(100), description varchar(500), price double, category varchar(10), imagelink varchar(100))",
		    "Create table customers (cusId INT AUTO_INCREMENT PRIMARY KEY, name varchar(20), age int)"
	    })
	    {
		executeSQL(conn, query);
	    }
	}
    }

    private static void worker1()
    {

	Properties properties = new Properties();
	properties.put("user", USER);
	properties.put("password", PASSWORD);
	try (Connection conn = DriverManager.getConnection(CONNECTION, properties))
	{
	    executeSQL(conn, "USE mydb");
	    while (!Thread.interrupted()) //for(int i = 0; i < 10; i++)	 
	    {
		executeSQL(conn, "INSERT INTO products (name, description, price, category, imagelink ) VALUES ('" + getName() + "', 'The AtomicInteger class provides you with a int variable which can be read and written atomically, and which also contains advanced atomic operations like compareAndSet().', 99.99, 'Atomic','http://192.168.99.100:9000/explore/query')");
	    }
	}
	catch (SQLException e)
	{
	    e.printStackTrace();
	}
    }
    
    public static String getName()
    {
	return "name_" + System.currentTimeMillis();
    }
    
    public static String getName2()
    {
	return "customer_" + System.currentTimeMillis();
    }
    
    public static long getRandomProductID()
    {
	return atomicLong.getAndIncrement();

    }
    
    
    private static void worker2()
    {
	Properties properties = new Properties();
	properties.put("user", USER);
	properties.put("password", PASSWORD);
	try (Connection conn = DriverManager.getConnection(CONNECTION, properties))
	{
	    executeSQL(conn, "USE mydb");
	    while (!Thread.interrupted()) //for(int i = 0; i < 10; i++)	 
	    {
		executeSQL(conn, "INSERT INTO customers (name, age) VALUES ('" + getName2() + "',20)");
	    }
	}
	catch (SQLException e)
	{
	    e.printStackTrace();
	}
    }
    
    private static void worker3()
    {
	Properties properties = new Properties();
	properties.put("user", USER);
	properties.put("password", PASSWORD);
	try (Connection conn = DriverManager.getConnection(CONNECTION, properties))
	{
	    executeSQL(conn, "USE mydb");
	    while (!Thread.interrupted()) //for(int i = 0; i < 10; i++)	 
	    {
		executeSQL(conn, "INSERT INTO customer_product (cusid, proid) VALUES (1, " + getRandomProductID() +")");
	    }
	}
	catch (SQLException e)
	{
	    e.printStackTrace();
	}
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, InterruptedException
    {
	System.out.println("start");
	
	atomicLong = new AtomicLong();
	atomicLong.set(0);
	
	Class.forName(dbClassName);
	//ResetEnvironment();
	ExecutorService executor = Executors.newFixedThreadPool(100);
	for (int i = 0; i < 100; i++)
	{
	    executor.submit(new Runnable()
	    {
		@Override
		public void run()
		{
		    //worker1();
		    //worker2();
		    
		    worker3();
		}
	    });
	    
	    System.out.println("count: " + i);
	}
	Thread.sleep(5000);
	executor.shutdownNow();
	if (!executor.awaitTermination(5, TimeUnit.SECONDS))
	{
	    System.err.println("Pool did not terminate");
	}
    }
}
