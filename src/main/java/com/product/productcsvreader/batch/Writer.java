package com.product.productcsvreader.batch;

import java.util.List;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import com.product.productcsvreader.Product;

@Component
public class Writer implements ItemWriter<Product> {

//@Autowired
//private UserRepository userRepository;

	@Override
public void write(List<? extends Product> users) throws Exception {

System.out.println("Data saved for users: " + users);
  // userRepository.save(users);
 }
}