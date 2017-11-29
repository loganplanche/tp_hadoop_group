package tp_hadoop_id.tp_hadoop_group;
import java.io.IOException; 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper; 


public class PivotMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	
	//Map(k1,v1) → list(k2,v2)
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
		int column = 0;
		long keyLong = key.get();
		String valueStr = value.toString();
		// Découpage de la ligne prise en compte par le mapper
		// en portion de texte à l'aide de la fonction split(), 
		// chaque texte est successivcement stocké dans la variable texte
		for (String texte : valueStr.split(",")) {
			// Enregistrement de la nouvelle ligne 
			// où doit se placer le texte contenu dans la variable texte
			context.write(new IntWritable(column), new Text(Long.toString(keyLong)+","+texte));
			column++;
		 }
	}
}