package io.jaegertracing.depenencies.model;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@UDT(name = "key_value")
public class KeyValue {

  @Field(name = "key")
  public String key;

  @Field(name = "value")
  public String value;
}
