package tp_hadoop_id.tp_hadoop_group;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer; 

public class PivotReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	
	//Reduce(k2, list(v2)) → list(k3,v3)
	//Permet, pour chaque ligne du sous-ensemble, 
	//de regrouper et de mettre dans l'ordre
	//les textes de cette même ligne 
	//en les séparant par des "," sauf en fin de ligne
	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		/*List<String> valuesList = new ArrayList<String>();
    	for(Text value : values) {
    		valuesList.add(value.toString());
    	}
    	Collections.sort(valuesList);
		String line = "";
		for (String str : valuesList) {
			str = str.split(",")[1];
			line += str + "," ;
		}
		line = line.substring(0, line.length() - 1);*/
		Map<Long, String> map = new TreeMap<Long, String>();
		String line = "";
		
		for(Text value : values) {
			map.put(Long.parseLong(value.toString().split(",")[0]), value.toString().split(",")[1]);
		}
		
		for(Map.Entry<Long, String> entry : map.entrySet()) {
			line += entry.getValue() + ",";
		}
		line = line.substring(0, line.length()-1);
		
		context.write(key, new Text(line));
	}
}