package com.comatrix.cdm;
import com.comatrix.cdm.model.Attribute;
import com.comatrix.cdm.model.Entity;
import com.comatrix.cdm.model.Trait;
import com.google.gson.Gson;
import technology.semi.weaviate.client.Config;
import technology.semi.weaviate.client.WeaviateClient;
import technology.semi.weaviate.client.base.Result;
import technology.semi.weaviate.client.v1.data.model.WeaviateObject;
import technology.semi.weaviate.client.v1.misc.model.Meta;
import technology.semi.weaviate.client.v1.schema.model.DataType;
import technology.semi.weaviate.client.v1.schema.model.Property;
import technology.semi.weaviate.client.v1.schema.model.WeaviateClass;

import java.util.*;

public class ComatrixWeaviateSchemaCreator {

    public static void main(final String[] args) {
        Config config = new Config("http", "localhost:8080");
        WeaviateClient client = new WeaviateClient(config);
        Result<Meta> meta = client.misc().metaGetter().run();
        if (meta.getResult() != null) {
            System.out.printf("meta.hostname: %s\n", meta.getResult().getHostname());
            System.out.printf("meta.version: %s\n", meta.getResult().getVersion());
            System.out.printf("meta.modules: %s\n", meta.getResult().getModules());
        } else {
            System.out.printf("Error: %s\n", meta.getError().getMessages());
        }
       createSchema(client);
    }

    public static void createSchema(WeaviateClient client) {
        createEntitySchema(client);
        createAttributeSchema(client);
    }

    private static void createEntitySchema(WeaviateClient client) {
        WeaviateClass clazz = WeaviateClass.builder()
                .className("Entity")
                .description("An Entity")
                .vectorIndexType("hnsw")
              //   .vectorizer("text2vec-contextionary")
                .properties(new ArrayList() { {
                    add(Property.builder()
                            .dataType(new ArrayList(){ { add(DataType.STRING); } })
                            .description("Entity name")
                            .name("name")
                            .build());
                } })
                .build();

        Result<Boolean> result = client.schema().classCreator().withClass(clazz).run();
        if (result.hasErrors()) {
            System.out.println(result.getError());
            return;
        }
        System.out.println(result.getResult());
    }

    private static void createAttributeSchema(WeaviateClient client) {
        WeaviateClass clazz = WeaviateClass.builder()
                .className("Attribute")
                .description("An attribute")
                .vectorIndexType("hnsw")
               //  .vectorizer("text2vec-contextionary")
                .properties(new ArrayList() { {
                    add(Property.builder()
                            .dataType(new ArrayList(){ { add(DataType.STRING); } })
                            .description("Attribute Name")
                            .name("name")
                            .build());
                    add(Property.builder()
                            .dataType(new ArrayList(){ { add("Entity"); } })
                            .description("Attribute's entity")
                            .name("ent")
                            .build());
                } })
                .build();

        Result<Boolean> result = client.schema().classCreator().withClass(clazz).run();
        if (result.hasErrors()) {
            System.out.println(result.getError());
            return;
        }
        System.out.println(result.getResult());

        // reference property
        Property referenceProperty = Property.builder()
                .dataType(Arrays.asList("Entity"))
                .description("reference to entities this attribute is part of ")
                .name("entities")
                .build();
        Result<Boolean> entitiesRefAdd = client.schema().propertyCreator().withClassName("Attribute").withProperty(referenceProperty).run();
        if (result.hasErrors()) {
            System.out.println(result.getError());
            return;
        }
        System.out.println(result.getResult());
    }
}
