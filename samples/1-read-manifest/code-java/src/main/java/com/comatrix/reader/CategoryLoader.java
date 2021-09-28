package com.comatrix.reader;

import com.comatrix.cdm.WeaviateGateway;
import technology.semi.weaviate.client.Config;
import technology.semi.weaviate.client.WeaviateClient;
import technology.semi.weaviate.client.v1.data.model.WeaviateObject;

import java.util.List;

public class CategoryLoader {
    public static void main(String[] args) {
        Config config = new Config("http", "localhost:8080");
        WeaviateClient client = new WeaviateClient(config);
        try {
            List<WeaviateObject> wObjs = WeaviateGateway.loadCategoriesIntoWeaviate(client);
            System.out.println(wObjs.size() +" catgories loaded into weaviate");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
