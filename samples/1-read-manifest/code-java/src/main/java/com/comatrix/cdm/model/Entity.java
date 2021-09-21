package com.comatrix.cdm.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Entity {
     String name;

     public Entity(String name) {
          this.name = name;
     }
}
