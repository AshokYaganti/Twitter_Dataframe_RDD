

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.swing.JOptionPane;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;

@WebServlet("/RDDTwitterQueries")
public class RDDTwitterQueries extends HttpServlet {
	private static final long serialVersionUID = 1L;
	
	 URL url=getClass().getResource("tweetsen.json");

    public RDDTwitterQueries() {
        // TODO Auto-generated constructor stub
    }


	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
	
	}


	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		 int option = Integer.parseInt(request.getParameter("option"));
	    
	      switch(option)
			{
			case 1: 
				query1();
		       
				break;
		case 2: 
			    query2();
			break;
		
			default: JOptionPane.showMessageDialog(null, "Invalid Option please Enter from 1 to 8");
						break;
			}
		
		
	}
	
	 public void query1()
		{
     	   
            String pathToFile = url.toString();
		    SparkConf conf = new SparkConf().setAppName("User mining").setMaster("local[*]");
		    
		    
		    JavaSparkContext sc = new JavaSparkContext(conf);
		   
		    JavaSQLContext sqlContext = new JavaSQLContext(sc);
		   
		    JavaSchemaRDD tweets = sqlContext.jsonFile(pathToFile);

		    tweets.registerAsTable("tweetTable");
		    
		    tweets.printSchema();

		    SparkQuery1(sqlContext);

		    sc.stop();
		    
		   
		}
		
		private void SparkQuery1(JavaSQLContext sqlContext) 
		{		  
			 try
			 {
				 
				 File outputFile = new File("C:/Users/ashok/PBPhase2/RDDTwitterProject/WebContent/Query1.csv");

				   
				
			 FileWriter fw= new FileWriter(outputFile);
			 
		   
		    JavaSchemaRDD trumpcount = sqlContext.sql("SELECT COUNT(*) AS c FROM tweetTable " +
			    										"where text like '%trump%'");
		    JavaSchemaRDD hillarycount = sqlContext.sql("SELECT COUNT(*) AS c FROM tweetTable " +
														"where user.description like '%hillary%'");
		    
		    
		     List<Row> cricket=trumpcount.collect();	 
			 String trumpcount12=cricket.toString();
			 String trumpcount1 = trumpcount12.substring(trumpcount12.indexOf("[") + 2, trumpcount12.indexOf("]"));
		   
			 List<Row> hillarycount123=hillarycount.collect();	 
			 String hillarycount12=hillarycount123.toString();
			 String hillarycount1 = hillarycount12.substring(hillarycount12.indexOf("[") + 2, hillarycount12.indexOf("]"));
		    
		  
		    System.out.println("Total tweets on trump:"+trumpcount1);
		    System.out.println("Total tweets on Hillary:"+hillarycount1);
		    			   
		    fw.append("Name");
			fw.append(',');
			fw.append("Count");
			fw.append("\n");
			fw.append("Trump Tweets");
			fw.append(',');
			fw.append(trumpcount1);
			fw.append("\n");
			fw.append("Hillary Tweets");
			fw.append(',');
			fw.append(hillarycount1);
			fw.append("\n");
			fw.close();
			
			
		 }
			  catch (Exception exp)
			  {
			  }

		  }

		
		public void query2()
		{
			
         String pathToFile = url.toString();
			 SparkConf conf = new SparkConf().setAppName("User mining").setMaster("local[*]");

	         JavaSparkContext sc = new JavaSparkContext(conf);
	 
	         JavaSQLContext sqlContext = new JavaSQLContext(sc);

	         JavaSchemaRDD tweets = sqlContext.jsonFile(pathToFile);

	         tweets.registerAsTable("tweetTable");

	        tweets.printSchema();

	       sparkQuery2(sqlContext);

	        sc.stop();
	        


		}
		
		 private void sparkQuery2(JavaSQLContext sqlContext) {
			  
			  try
			  {
				  
		     File outputFile = new File("C:/Users/ashok/PBPhase2/RDDTwitterProject/WebContent/Query2.csv");
				
			 FileWriter fw= new FileWriter(outputFile);
		   
		    JavaSchemaRDD count = sqlContext.sql("SELECT created_at, COUNT(*) AS c FROM tweetTable " +
		    										"where text like '%hillary%'"+
			    										"Group By created_at " +
			    		                                  "order by c");
		    
		    List<org.apache.spark.sql.api.java.Row> rows = count.collect(); 

		       Collections.reverse(rows);
			    
			   String rows123=rows.toString();
		    
		       String[] array = rows123.split("],"); 
		    
		
		    
		    fw.append("Created_timestamp");
			fw.append(',');
			fw.append("Count");
			fw.append("\n");
			
			

			for(int i = 0; i < 8; i++)
			{
				if(i==0)
				{
					fw.append(array[0].substring(2));
					fw.append(',');
					fw.append("\n");
				}
				else {
				fw.append(array[i].substring(2));
				fw.append(',');
			    fw.append("\n");
				}
			}
			
			fw.close();
			
			
		 }
			  catch (Exception exp)
			  {
			  }
			  

		  }
		

}
