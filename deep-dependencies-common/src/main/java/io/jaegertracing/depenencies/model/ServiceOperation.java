package io.jaegertracing.depenencies.model;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@UDT(name = "service_operation")
public class ServiceOperation {

  public static final String ALL_OPERATIONS = "";

  @Field(name = "service")
  public String service;

  @Field(name = "operation")
  public String operation;

  public int length() {
    return service.length() + operation.length();
  }
}
