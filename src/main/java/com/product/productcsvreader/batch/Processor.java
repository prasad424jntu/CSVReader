package com.product.productcsvreader.batch;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import com.product.productcsvreader.model.Product;



@Component
public class Processor implements ItemProcessor<Product, Product> {


public Processor() {
}

public Product process(Product user) throws Exception {
     return user;
  }
}