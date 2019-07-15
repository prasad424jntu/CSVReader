package com.product.productcsvreader.batch.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.internals.Sender;
import org.h2.api.JavaObjectSerializer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.fasterxml.jackson.databind.ser.std.StringSerializer;
import com.product.productcsvreader.Product;


@Configuration
@EnableBatchProcessing
public class SpringBatchConfig {
	
		
	  @Bean
	  public Map<String, Object> producerConfigs() {
	    Map<String, Object> props = new HashMap<>();
	    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
	    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
	    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

	    return props;
	  }
	  
	  @Bean
	  public ProducerFactory<String, String> producerFactory() {
	    return new DefaultKafkaProducerFactory(producerConfigs());
	  }

	  @Bean
	  public KafkaTemplate<String, String> kafkaTemplate() {
	    return new KafkaTemplate<>(producerFactory());
	  }
	  
//	  @Bean
//	  public Sender sender() {
//	    return new Sender();
//	  }


 
	 @Bean
	 public Job job(JobBuilderFactory jobBuilderFactory,
	   StepBuilderFactory stepBuilderFactory,
	   ItemReader<Product> itemReader,
	   ItemProcessor<Product, Product> itemProcessor,
	   ItemWriter<Product> itemWriter) {
	
	  Step step = stepBuilderFactory.get("CSV-file-load")
	     .<Product, Product>chunk(100)
	     .reader(itemReader)
	     .processor(itemProcessor)
	     .writer(itemWriter)
	     .build();
	
	  return jobBuilderFactory.get("CSV-Load")
	    .incrementer(new RunIdIncrementer())
	    .start(step)
	    .build();
	
	 }
 
	 @Bean
	 public FlatFileItemReader<Product> reader()
	 {
	     //Create reader instance
	     FlatFileItemReader<Product> reader = new FlatFileItemReader<Product>();
	      
	     //Set input file location
	     reader.setResource(new FileSystemResource("src/main/resources/dataExample.csv"));
	      
	     //Set number of lines to skips. Use it if file has header rows.
	     reader.setLinesToSkip(1);  
	      
	     //Configure how each line will be parsed and mapped to different values
	     reader.setLineMapper(new DefaultLineMapper<Product>() {
	         {
	             //3 columns in each row
	             setLineTokenizer(new DelimitedLineTokenizer() {
	                 {
	                     setNames(new String[] { "uuid", "name", "description" , "provider" , "available" , "measurementUnits" });
	                 }
	             });
	             //Set values in Employee class
	             setFieldSetMapper(new BeanWrapperFieldSetMapper<Product>() {
	                 {
	                     setTargetType(Product.class);
	                 }
	             });
	         }
	     });
	     return reader;
	 }
 
	 @Bean
	 public LineMapper<Product> lineMapper() {
	  
	  DefaultLineMapper<Product> defaultLineMapper = new DefaultLineMapper<>();
	  DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
	  lineTokenizer.setDelimiter(",");
	  lineTokenizer.setStrict(false);
	  lineTokenizer.setNames(new String[] {"uuid","name","description","provider","available","measurementUnits"});
	  
	  BeanWrapperFieldSetMapper<Product> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
	  fieldSetMapper.setTargetType(Product.class);
	  defaultLineMapper.setLineTokenizer(lineTokenizer);
	  defaultLineMapper.setFieldSetMapper(fieldSetMapper);
	  
	  return defaultLineMapper;
	 }
	 
	 @Bean
	 public ConsoleItemWriter<Product> itemWriter()
	 {
	     return new ConsoleItemWriter<Product>();
	 }
 
 	

}
class ConsoleItemWriter<Product> implements ItemWriter<Product> {
	
	@Autowired
	KafkaTemplate<String, String> kafkaTemplate;

	private static final String TOPIC = "product";
	
    @Override
    public void write(List<? extends Product> items) throws Exception {
        for (Product item : items) {
            System.out.println("product json :" + item.toString());
        	
        	kafkaTemplate.send(TOPIC, item.toString());
        }
    }
}
