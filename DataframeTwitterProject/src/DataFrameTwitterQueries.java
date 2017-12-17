
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.swing.JOptionPane;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;


@WebServlet("/DataFrameTwitterQueries")
public class DataFrameTwitterQueries extends HttpServlet {
	private static final long serialVersionUID = 1L;
	URL url = getClass().getResource("tweetsen.json");

    public DataFrameTwitterQueries() {
        super();
   
    }

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
	
	}


	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
	
		 int option = Integer.parseInt(request.getParameter("option"));
	     
	      switch(option)
			{
			case 1: 
		        Query1();
		       
		       break;
				
			case 2: 
				Query2();
		       
		      
				break;
		
							
			default: JOptionPane.showMessageDialog(null, "Invalid Option please Enter from 1 to 2");
						break;
			}
	}
	
	public void Query1()
	{
		
       String pathToFile = url.toString();	 
      
        SparkConf conf = new SparkConf();
        conf.setAppName("Spark MultipleContest Test");
        conf.set("spark.driver.allowMultipleContexts", "true");
        conf.setMaster("local");
        
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        DataFrame tweets = sqlContext.read().json(pathToFile);
        
          tweets.registerTempTable("tweetTable");          
     
     
     DataFrame positivetweets = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
				"WHERE text LIKE '%trump%' AND (text LIKE '%abundant%' OR text LIKE '%accessible%' OR text LIKE '%accurate%' OR text LIKE '%award%' OR text LIKE '%awesome%' OR text LIKE '%beautiful%' OR text LIKE '%affirmation%' OR text LIKE '%amicable%' OR text LIKE '%appreciate%' OR text LIKE '%approve%' OR text LIKE '%attractive%' OR text LIKE '%benefit%' OR text LIKE '%bless%' OR text LIKE '%bonus%' OR text LIKE '%brave%' OR text LIKE '%bright%' OR text LIKE '%brilliant%' OR text LIKE '%celebrate%' OR text LIKE '%champion%' OR text LIKE '%charm%' OR text LIKE '%cheer%' OR text LIKE '%clever%' OR text LIKE '%colorful%' OR text LIKE '%comfort%' OR text LIKE '%compliment%' OR text LIKE '%confidence%' OR text LIKE '%congratulation%' OR text LIKE '%cute%' OR text LIKE '%good%' OR text LIKE '%happy%' OR text LIKE '%cool%' OR text LIKE '%easy%' OR text LIKE '%effective%' OR text LIKE '%efficient%' OR text LIKE '%fair%' OR text LIKE '%excite%' OR text LIKE '%fast%' OR text LIKE '%fine%' OR text LIKE '%fortunate%' OR text LIKE '%free%' OR text LIKE '%fresh%' OR text LIKE '%fun%' OR text LIKE '%gain%' OR text LIKE '%gem%' OR text LIKE '%gorgeous%' OR text LIKE '%grand%' OR text LIKE '%handsome%' OR text LIKE '%healthy%' OR text LIKE '%honest%' OR text LIKE '%humor%' OR text LIKE '%important%' OR text LIKE '%impress%' OR text LIKE '%improve%' OR text LIKE '%joy%' OR text LIKE '%love%' OR text LIKE '%perfect%' OR text LIKE '%pleasant%' OR text LIKE '%compliment%' OR text LIKE '%pleasure%' OR text LIKE '%precious%' OR text LIKE '%prolific%' OR text LIKE '%prudent%' OR text LIKE '%happy%' OR text LIKE '%cool%' OR text LIKE '%proven%' OR text LIKE '%effective%' OR text LIKE '%efficient%' OR text LIKE '%restored%')");
     DataFrame negativetweets = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
    		 	"WHERE text LIKE '%trump%' AND (text LIKE '%abuse%' OR text LIKE '%abyss%' OR text LIKE '%absurd%' OR text LIKE '%akward%' OR text LIKE '%adverse%' OR text LIKE '%agony%' OR text LIKE '%annoying%' OR text LIKE '%anti%' OR text LIKE '%arrogant%' OR text LIKE '%assassinate%' OR text LIKE '%aversion%' OR text LIKE '%backward%' OR text LIKE '%bad%' OR text LIKE '%brutal%' OR text LIKE '%battered%' OR text LIKE '%berate%' OR text LIKE '%bewitch%' OR text LIKE '%berate%' OR text LIKE '%blunder%' OR text LIKE '%complain%' OR text LIKE '%conflict%' OR text LIKE '%confound%' OR text LIKE '%contagious%' OR text LIKE '%contaminated%' OR text LIKE '%contravene%' OR text LIKE '%corruption%' OR text LIKE '%corrupt%' OR text LIKE '%coward%' OR text LIKE '%cruel%' OR text LIKE '%sad%' OR text LIKE '%danger%' OR text LIKE '%debase%' OR text LIKE '%decline%' OR text LIKE '%deceive%' OR text LIKE '%defamation%' OR text LIKE '%demon%' OR text LIKE '%demolish%' OR text LIKE '%denied%' OR text LIKE '%demolish%' OR text LIKE '%depress%' OR text LIKE '%deny%' OR text LIKE '%destroy%' OR text LIKE '%devastation%' OR text LIKE '%disadvantage%' OR text LIKE '%disappointed%' OR text LIKE '%discord%' OR text LIKE '%evil%' OR text LIKE '%gossip%' OR text LIKE '%hard%' OR text LIKE '%gloom%' OR text LIKE '%hate%' OR text LIKE '%hazard%' OR text LIKE '%fuck%' OR text LIKE '%horrible%' OR text LIKE '%idiot%' OR text LIKE '%imperfect%' OR text LIKE '%inefficient%' OR text LIKE '%inflammation%' OR text LIKE '%ironic%' OR text LIKE '%irritate%' OR text LIKE '%jealous%' OR text LIKE '%lag%' OR text LIKE '%lie%' OR text LIKE '%malignant%' OR text LIKE '%malign%' OR text LIKE '%noisy%' OR text LIKE '%odd%' OR text LIKE '%offence%' OR text LIKE '%offend%' OR text LIKE '%offensive%' OR text LIKE '%bad%' OR text LIKE '%unhappy%' OR text LIKE '%weak%')");
     
     
     
     Row[] positive = positivetweets.collect();
     Row[] negative = negativetweets.collect();
     
     String positive11= positive[0].get(0).toString();
     String negative11= negative[0].get(0).toString();
    
     
     System.out.println("Positive tweets on Trump:" +positive11 );
     System.out.println("Negative tweets on Trump:" +negative11 );
          
        try
        {
      	  
		     File outputFile = new File("C:/Users/ashok/PBPhase2/DataFrameTwitterProject/WebContent/Query1.csv");

		    FileWriter fw = new FileWriter(outputFile);
		    

		     fw.append("TweetStatus");
			 fw.append(',');
			 fw.append("Count");
			 fw.append("\n");
			 fw.append("Positive");
			 fw.append(',');
			 fw.append(positive11);
			 fw.append("\n");
			 fw.append("Negative");
			 fw.append(',');
			 fw.append(negative11);
			 fw.append("\n");
			
          fw.close();
            
        }
        catch(Exception e)
        {
       	 
        }
        
        
		
       sc.stop();
       
	}
	
	public void Query2()
	{
		 String pathToFile = url.toString();	 
	       
        SparkConf conf = new SparkConf();
        conf.setAppName("Spark MultipleContest Test");
        conf.set("spark.driver.allowMultipleContexts", "true");
        conf.setMaster("local");
        
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        DataFrame tweets = sqlContext.read().json(pathToFile);
        
         tweets.registerTempTable("tweetTable");
          
        DataFrame followers = sqlContext.sql("SELECT DISTINCT user.name, user.followers_count AS c FROM tweetTable " + "order by c DESC");
     
          Row[] rows = followers.collect();
          
        try
        {
      	  
        	 File outputFile = new File("C:/Users/ashok/PBPhase2/DataFrameTwitterProject/WebContent/Query2.csv");

		    FileWriter fw = new FileWriter(outputFile);
		    

			fw.append("Name");
			fw.append(',');
			fw.append("Followers");
			fw.append("\n");
          
          for (int i=0;i<20;i++) {
        	  
        	  System.out.println("Name :"+ rows[i].get(0).toString()+"---" + "Followers:"+ rows[i].get(1).toString());
       	   
       	   fw.append(rows[i].get(0).toString());
			   fw.append(',');
			   fw.append(rows[i].get(1).toString()); 
			   fw.append("\n");
            
            }
          
          fw.close();
            
        }
        catch(Exception e)
        {
       	 
        }
        
        
		
       sc.stop();
	}
	
}
