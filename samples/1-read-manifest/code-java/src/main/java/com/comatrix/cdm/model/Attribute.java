package com.comatrix.cdm.model;

import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class Attribute extends Version{
    String name;
    // Map<String, Object> properties = new HashMap<>();
    // List<Entity> entities;
    // List<CoMatrixCategory> categories;
    // List<Trait> traits;

    public Attribute(String name) {
        this.name = name;
    }
}
