package com.comatrix.cdm.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Trait {
    String name;
    String description;

    public Trait(String name) {
        this.name = name;
    }

    public Trait(String name, String description) {
        this.name = name;
        this.description = description;
    }
}
