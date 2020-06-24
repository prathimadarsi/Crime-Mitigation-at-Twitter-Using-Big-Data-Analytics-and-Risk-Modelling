package analyze;


import java.io.*;
import java.util.*;
import java.math.BigDecimal;
import java.math.MathContext;
import java.net.URI;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.apache.commons.lang3.StringUtils;

public class TwitterMapper extends Mapper<LongWritable,Text,NullWritable,Text> {

	JSONParser parser = null;
	Map<String,String> dictionary = null;
	Map<String,Integer> Anger=new HashMap<String,Integer>();
	Map<String,Integer> Joy=new HashMap<String,Integer>();
	Map<String,Integer> Love=new HashMap<String,Integer>();
	Map<String,Integer> Sad=new HashMap<String,Integer>();
	Map<String,Integer> Suprise=new HashMap<String,Integer>();
	
	@Override
	protected void setup(Context context)throws IOException,InterruptedException
	{
		parser = new JSONParser();
		dictionary = new HashMap<String,String>();
		
		
		
			URI[] cacheFiles = context.getCacheFiles();
		
			if (cacheFiles != null && cacheFiles.length > 0)
			  {
			    try
			    {   
			    	String line ="";
			        FileSystem fs = FileSystem.get(context.getConfiguration());
			        Path path = new Path(cacheFiles[0].toString());
			        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
			    
			        //before
			        String [] Emotions={"Anger","Joy","Love","Sad","Suprise"};
					
					
					for(int i=0;i<Arrays.asList(Emotions).size();i++)
					{
//Paths.get("filename.txt"))
					        BufferedReader br = new BufferedReader(new FileReader("./Dataset/"+Emotions[i]));
						String feature;

						while ((feature= br.readLine()) != null) {

							String val[]=feature.split(",");
							if(i==0)
								Anger.put(val[0].replaceAll(" +", " "),Integer.parseInt(val[1]));	
							if(i==1)
								Joy.put(val[0].replaceAll(" +", " "),Integer.parseInt(val[1]));	
							if(i==2)
								Love.put(val[0].replaceAll(" +", " "),Integer.parseInt(val[1]));	
							if(i==3)
								Sad.put(val[0].replaceAll(" +", " "),Integer.parseInt(val[1]));	
							if(i==4)
								Suprise.put(val[0].replaceAll(" +", " "),Integer.parseInt(val[1]));	
						}
					}

			        //after
			        
			        while((line = reader.readLine())!=null)
			        {
			        	String []tokens = line.split("\t");
			        	dictionary.put(tokens[0], tokens[1]);
			        }
			    
			    
			    
			    }catch(Exception e)
			    {
			    System.out.println("Unable to read the cache file");
			    System.exit(1);
			    }
			  }	
		}
	
	
	@Override
	public void map(LongWritable key, Text value,Context context) throws IOException,InterruptedException
	{
		long sentiment_value = 0;
		JSONObject json = null; 
		try{
			
			json = (JSONObject) parser.parse(value.toString());
		    JSONObject object = (JSONObject)json.get("quoted_tweet");
			String id=null;
			String processedTweet= null;
		    String classify=null;
		    String [] Emotions={"Anger","Joy","Love","Sad","Suprise"};
		    
		   
			if(null != object && StringUtils.isNotBlank(String.valueOf(object)))
			{
				
				if(object.get("id") !=null &&  object.get("text")!=null
						&& StringUtils.isNotBlank(String.valueOf(object.get("id")))
								&& StringUtils.isNotBlank(String.valueOf(object.get("text"))))
				{
					
					id = String.valueOf(object.get("id")).trim();
					processedTweet = specialSymbolRemover.remove(String.valueOf(object.get("text")));
					
					processedTweet = stopWordsRemover.remove(processedTweet);
					
					String []words = processedTweet.split(" ");
					
					for(String temp:words)
					{
						if(dictionary.containsKey(temp))
						{
							sentiment_value+=Long.parseLong(dictionary.get(temp));
						}
					}
				
				
				
				//String words[]=processedTweet.split(" ");
				int words_no=Arrays.asList(words).size();
				double[][] probability =new double[words_no][6];
				double[] inter_probs=new double[6];
			 	BigDecimal[] final_probs=new BigDecimal[6];


				for(int i=0;i<words_no;i++)
					for(int j=0;j<5;j++)
					{
						probability[i][j]=0.333;
	                               		final_probs[j]=new BigDecimal(1.0);

				        }

				int curr=0,present=0;
				double probs_sum=0.0;

	                        		
				for(String word:words)
				{
					if(Anger.containsKey(word))
					{
						double times=Anger.get(word);
						double total=Anger.size();
						inter_probs[0]=times/total;
					}
					else
						inter_probs[0]=0;
				
					if(Joy.containsKey(word))
					{
						double times=Joy.get(word);
						double total=Joy.size();
						inter_probs[1]=times/total;
					}
					else
						inter_probs[1]=0;

				
					if(Love.containsKey(word))
					{
						double times=Love.get(word);
						double total=Love.size();
						inter_probs[2]=times/total;
					}
					else
						inter_probs[2]=0;
					
					if(Suprise.containsKey(word))
					{
						double times=Suprise.get(word);
						double total=Suprise.size();
						inter_probs[4]=times/total;
					}
					else
						inter_probs[4]=0;
					if(Sad.containsKey(word))
					{ 
						double times=Sad.get(word);
						double total=Sad.size();
						inter_probs[3]=times/total;
					}
					else
						inter_probs[3]=0;	
				
					for(int i=0;i<5;i++)
					{
						if(inter_probs[i]>0)
						{
							probability[curr][i]=inter_probs[i];
							probs_sum+=inter_probs[i];
							present++;
	                                 
						}
					}
					double rem_sum=1-probs_sum;
				
					if(present>0)
					{  
						for(int j=0;j<5;j++)
						{
							if(probability[curr][j]==0.333)
								probability[curr][j]=0.000001*rem_sum;
							
							System.out.println(word+","+Emotions[j]+": "+probability[curr][j]);			
						} 
	  				}	
	 

					curr++;
					probs_sum=0.0;
					present=0;
				}


				int emo=-1;
				BigDecimal large=new BigDecimal(-34.56);
				MathContext mc=new MathContext(20);

				for(int j=0;j<5;j++)
					for(int i=0;i<words_no;i++)
						final_probs[j]=final_probs[j].multiply(new BigDecimal(probability[i][j]),mc);
				for(int i=0;i<5;i++)
				{  
					System.out.println(final_probs[i]);	
					if(large.compareTo(final_probs[i])==-1)
					{
						large=final_probs[i];
						emo=i;
					}				
				}

	  			classify=String.valueOf(Emotions[emo]);
			
			
			}
			}
			else if (json.get("id")!=null && json.get("text")!=null
					&& StringUtils.isNotBlank(String.valueOf(json.get("id")))
					&& StringUtils.isNotBlank(String.valueOf(json.get("text"))))
			{
				
				id = String.valueOf(json.get("id")).trim();
				
				processedTweet = specialSymbolRemover.remove(String.valueOf(json.get("text")));
				
				processedTweet = stopWordsRemover.remove(processedTweet);
				
				String []words = processedTweet.split(" ");
				
				for(String temp:words)
				{
					if(dictionary.containsKey(temp))
					{
						sentiment_value+=Long.parseLong(dictionary.get(temp));
						
					}
						
				}
				
				int words_no=Arrays.asList(words).size();
				double[][] probability =new double[words_no][6];
				double[] inter_probs=new double[6];
			 	BigDecimal[] final_probs=new BigDecimal[6];


				for(int i=0;i<words_no;i++)
					for(int j=0;j<5;j++)
					{
						probability[i][j]=0.333;
	                               		final_probs[j]=new BigDecimal(1.0);

				        }

				int curr=0,present=0;
				double probs_sum=0.0;

	                        			
				for(String word:words)
				{
					if(Anger.containsKey(word))
					{
						double times=Anger.get(word);
						double total=Anger.size();
						inter_probs[0]=times/total;
					}
					else
						inter_probs[0]=0;
				
					if(Joy.containsKey(word))
					{
						double times=Joy.get(word);
						double total=Joy.size();
						inter_probs[1]=times/total;
					}
					else
						inter_probs[1]=0;

				
					if(Love.containsKey(word))
					{
						double times=Love.get(word);
						double total=Love.size();
						inter_probs[2]=times/total;
					}
					else
						inter_probs[2]=0;
					
					if(Suprise.containsKey(word))
					{
						double times=Suprise.get(word);
						double total=Suprise.size();
						inter_probs[4]=times/total;
					}
					else
						inter_probs[4]=0;
					if(Sad.containsKey(word))
					{ 
						double times=Sad.get(word);
						double total=Sad.size();
						inter_probs[3]=times/total;
					}
					else
						inter_probs[3]=0;	
				
					for(int i=0;i<5;i++)
					{
						if(inter_probs[i]>0)
						{
							probability[curr][i]=inter_probs[i];
							probs_sum+=inter_probs[i];
							present++;
	                                 
						}
					}
					double rem_sum=1-probs_sum;
				
					if(present>0)
					{  
						for(int j=0;j<5;j++)
						{
							if(probability[curr][j]==0.333)
								probability[curr][j]=0.000001*rem_sum;
							
							System.out.println(word+","+Emotions[j]+": "+probability[curr][j]);			
						} 
	  				}	
	 

					curr++;
					probs_sum=0.0;
					present=0;
				}


				int emo=-1;
				BigDecimal large=new BigDecimal(-34.56);
				MathContext mc=new MathContext(20);

				for(int j=0;j<5;j++)
					for(int i=0;i<words_no;i++)
						final_probs[j]=final_probs[j].multiply(new BigDecimal(probability[i][j]),mc);
				for(int i=0;i<5;i++)
				{  
					System.out.println(final_probs[i]);	
					if(large.compareTo(final_probs[i])==-1)
					{
						large=final_probs[i];
						emo=i;
					}				
				}

	  			classify=String.valueOf(Emotions[emo]);
					
			}
				
	
		if(StringUtils.isNotBlank(id) && StringUtils.isNotBlank(processedTweet))
		{
			
			context.write(NullWritable.get(),new Text(id+"\t"+processedTweet+"\t"+sentiment_value+"\t"+classify));
			
		}
			
								
	    }catch(ParseException e)
		{
			e.printStackTrace();
		}
			
	}	
	
}
