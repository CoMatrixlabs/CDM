package com.comatrix.cdm;
import com.comatrix.cdm.model.Attribute;
import com.comatrix.cdm.model.Entity;
import com.google.gson.Gson;
import com.microsoft.commondatamodel.objectmodel.cdm.*;
import technology.semi.weaviate.client.Config;
import technology.semi.weaviate.client.WeaviateClient;
import technology.semi.weaviate.client.base.Result;
import technology.semi.weaviate.client.v1.batch.api.ObjectsBatcher;
import technology.semi.weaviate.client.v1.batch.api.ReferencesBatcher;
import technology.semi.weaviate.client.v1.batch.model.BatchReference;
import technology.semi.weaviate.client.v1.batch.model.BatchReferenceResponse;
import technology.semi.weaviate.client.v1.batch.model.ObjectGetResponse;
import technology.semi.weaviate.client.v1.data.model.WeaviateObject;
import technology.semi.weaviate.client.v1.misc.model.Meta;

import java.util.*;
import java.util.stream.Collectors;

public class WeaviateGateway {
    public WeaviateGateway() {
    }

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
       // postWeaviateExampleModel(client);
        postSimpleCoMatrixModel(client);
    }

    private static void postWeaviateExampleModel(WeaviateClient client) {
        Map<String, Object> dataSchema = new HashMap<>();
        dataSchema.put("name", "Jodi Kantor");
        dataSchema.put("writesFor", new HashMap() { {
            put("beacon", "weaviate://localhost/f81bfe5e-16ba-4615-a516-46c2ae2e5a80");
        } });
        WeaviateObject obj = WeaviateObject.builder()
                .className("Author")
                .properties(dataSchema)
                .id("36ddd591-2dee-4e7e-a3cc-eb86d30a4303")
                .build();
        System.out.println(new Gson().toJson(obj));
        Result<WeaviateObject> result = client.data().creator()
                .withClassName("Author")
                .withID("36ddd591-2dee-4e7e-a3cc-eb86d30a4303")
                .withProperties(dataSchema)
                .run();

        if (result.hasErrors()) {
            System.out.println(result.getError());
            return;
        }
        System.out.println(result.getResult());
    }

    private static void postSimpleCoMatrixModel(WeaviateClient client) {

        Attribute attr  = new Attribute("addressNumber");

        // Entity ent1 = new Entity("Address");
        // Entity ent2 = new Entity("Contact");
//        List<Entity> entList = new ArrayList<>();
//        entList.add(ent1);
//        entList.add(ent2);
        // attr.setEntities(entList);

        // attr.getProperties().put("entity", entList);
        // attr.getProperties().put("entity", ent1);
       // "is.dataFormat.character",
        //  "is.dataFormat.array"
        // means.measurement.distance.pixels
        // Are traits singleton ? Or are Trait Types Singletons
//        Trait trait1 = new Trait("is.dataFormat.character", "List of characters");
//        Trait trait2 = new Trait("pii", "PII information");
//        List<Trait> traits = new ArrayList<>();
//        traits.add(trait1);
//        traits.add(trait2);
        // attr.setTraits(traits);
       // attr.getProperties().put("traits", traits);
        Map<String, Object> dataSchema = new HashMap<>();
        dataSchema.put("name", attr.getName());
        dataSchema.put("ent", new ArrayList() {{
            add(new Entity("Address"));
        } });
        UUID attrUuid = UUID.randomUUID();
        System.out.println(attrUuid.toString());
        String className = "Attribute";
//        WeaviateObject obj = WeaviateObject.builder()
//                .className(className)
//                .properties(dataSchema)
//                .id(attrUuid.toString())
//                .build();
//        System.out.println(new Gson().toJson(obj));
        Result<WeaviateObject> result = client.data().creator()
                .withClassName(className)
                .withID(attrUuid.toString())
                .withProperties(dataSchema)
                .run();

        if (result.hasErrors()) {
            System.out.println(result.getError());
            return;
        }
        System.out.println(result.getResult());

        // Now create Entities
        List<WeaviateObject> entities = createEntitiesBatch(client);

        // Now add Attribute references to entities
        createAttributeEntityReferences(client, result.getResult(), entities);


    }

    private static Result<BatchReferenceResponse[]> createAttributeEntityReferences(
            WeaviateClient client, WeaviateObject attribute, List<WeaviateObject> entities) {
        ReferencesBatcher refBatcher =  client.batch().referencesBatcher();
        entities.stream().forEach(e -> {
            refBatcher.withReference(BatchReference.builder()
                    .from("weaviate://localhost/Attribute/"+attribute.getId()+"/entities")
                    .to("weaviate://localhost/"+e.getId())
                    .build());
        });
        Result<BatchReferenceResponse[]> result = refBatcher.run();
        if (result.hasErrors()) {
            System.out.println(result.getError());
        }
        System.out.println(result.getResult());
        return result;
    }

    public static Result<BatchReferenceResponse[]> createAttributeEntityReferences(
            WeaviateClient client, List<WeaviateObject> attributes, WeaviateObject entity) {
        ReferencesBatcher refBatcher =  client.batch().referencesBatcher();
        attributes.stream().forEach(a -> {
            refBatcher.withReference(BatchReference.builder()
                    .from("weaviate://localhost/Attribute/"+a.getId()+"/entities")
                    .to("weaviate://localhost/"+entity.getId())
                    .build());
        });
        Result<BatchReferenceResponse[]> result = refBatcher.run();
        if (result.hasErrors()) {
            System.out.println(result.getError());
        }
        System.out.println(result.getResult());
        return result;
    }

    private static List<WeaviateObject> createEntitiesBatch(WeaviateClient client) {
        List<WeaviateObject> objects = new ArrayList(){ {
            add(
                    WeaviateObject.builder()
                            .className("Entity")
                            .id("36ddd591-2dee-4e7e-a3cc-eb86d30a4303")
                            .properties(new HashMap() { {
                                put("name", "Address");
//                                put("writesFor", new HashMap() { {
//                                    put("beacon", "weaviate://localhost/f81bfe5e-16ba-4615-a516-46c2ae2e5a80");
//                                } });
                            } })
                            .build()
            );
            add(
                    WeaviateObject.builder()
                            .className("Entity")
                            .id("36ddd591-2dee-4e7e-a3cc-eb86d30a4304")
                            .properties(new HashMap() { {
                                put("name", "Contact");
//                                put("writesFor", new HashMap() { {
//                                    put("beacon", "weaviate://localhost/f81bfe5e-16ba-4615-a516-46c2ae2e5a80");
//                                } });
                            } })
                            .build()
            );
        } };

        Result<ObjectGetResponse[]> result = client.batch().objectsBatcher()
                .withObject(objects.get(0))
                .withObject(objects.get(1))
                .run();

        if (result.hasErrors()) {
            System.out.println(result.getError());
            return null;
        }
        System.out.println(result.getResult());
        // For some reason, batch result does not have response objects;
        // Hence just return  the request objects directly if the call succeeded for now
        return objects;
    }

    public static List<WeaviateObject> createEntitiesBatch(
            WeaviateClient client, CdmEntityCollection cdmEntityCollection) {
        ObjectsBatcher objBatcher =  client.batch().objectsBatcher();
        List<WeaviateObject> objects = new ArrayList<>();
        cdmEntityCollection.forEach(e -> {
            String uuid = UUID.randomUUID().toString();
            WeaviateObject object = WeaviateObject.builder()
                    .className("Entity")
                    .id(uuid)
                    .properties(new HashMap() { {
                        put("entityName", e.getEntityName());
                        put("name", e.getName());
                        put("entityId", e.getId());
                        put("explanation", e.getExplanation());
                        put("objectTypeName", e.getObjectType().name());
                        put("entityPath", e.getEntityPath());
//                                put("writesFor", new HashMap() { {
//                                    put("beacon", "weaviate://localhost/f81bfe5e-16ba-4615-a516-46c2ae2e5a80");
//                                } });
                    } })
                    .build();
            objects.add(object);
            objBatcher.withObject(object);
        });

        Result<ObjectGetResponse[]> result = objBatcher.run();

        if (result.hasErrors()) {
            System.out.println(result.getError());
            return null;
        }
        System.out.println(result.getResult());
        // For some reason, batch result does not have response objects;
        // Hence just return  the request objects directly if the call succeeded for now
        return objects;
    }

    public static WeaviateObject createEntity(
            WeaviateClient client, CdmEntityDeclarationDefinition eDecDef,
            CdmEntityDefinition eDef) {
        ObjectsBatcher objBatcher =  client.batch().objectsBatcher();
        List<WeaviateObject> objects = new ArrayList<>();
        String uuid = UUID.randomUUID().toString();
        WeaviateObject object = WeaviateObject.builder()
                .className("Entity")
                .id(uuid)
                .properties(new HashMap() { {
                    put("entityName", eDecDef.getEntityName());
                    put("name", eDecDef.getName());
                    put("entityId", eDecDef.getId());
                    put("explanation", eDecDef.getExplanation());
                    put("entityPath", eDecDef.getEntityPath());
                    put("description", eDef.getDescription());
                    put("displayName", eDef.getDisplayName());

//                                put("writesFor", new HashMap() { {
//                                    put("beacon", "weaviate://localhost/f81bfe5e-16ba-4615-a516-46c2ae2e5a80");
//                                } });
                } })
                .build();
        objects.add(object);
        objBatcher.withObject(object);

        Result<ObjectGetResponse[]> result = objBatcher.run();

        if (result.hasErrors()) {
            System.out.println(result.getError());
            return null;
        }
        System.out.println(result.getResult());
        // For some reason, batch result does not have response objects;
        // Hence just return  the request objects directly if the call succeeded for now
        return objects.get(0);
    }

    public static List<WeaviateObject> createAttributesBatch(
            WeaviateClient client, CdmEntityDefinition cdmEntityDefinition) {
        ObjectsBatcher objBatcher =  client.batch().objectsBatcher();
        List<WeaviateObject> objects = new ArrayList<>();
        // This way of getting the attributes only works well for 'resolved' entities
        // that have been flattened out.
        // An abstract entity can be resolved by calling createResolvedEntity on it.
        cdmEntityDefinition.getAttributes().forEach(a -> {
            String uuid = UUID.randomUUID().toString();
            if (a instanceof CdmTypeAttributeDefinition) {
                final CdmTypeAttributeDefinition typeAttributeDefinition =
                        (CdmTypeAttributeDefinition) a;
                WeaviateObject object = WeaviateObject.builder()
                        .className("Attribute")
                        .id(uuid)
                        .properties(new HashMap() {
                            {
                                put("name", typeAttributeDefinition.getName());
                                // put("ownerObjectType", a.getOwner().getObjectType());
                                put("ownerId", a.getOwner().getId());
                                put("attributeId", a.getId());
                                // put("objectTypeName", a.getObjectType().name());
                                put("corpusPath", a.getAtCorpusPath());
                                // put("cdmDataType", typeAttributeDefinition.getDataType().);
                                put("dataFormat", typeAttributeDefinition.fetchDataFormat().name());
                                // put("attributeDefObjTypeName", typeAttributeDefinition.getObjectType().name());
                                put("defaultValue", typeAttributeDefinition.fetchDefaultValue());
                                put("maximumValue", typeAttributeDefinition.fetchMaximumValue());
                                put("minimumValue", typeAttributeDefinition.fetchMinimumValue());
                                put("description", typeAttributeDefinition.fetchDescription());
                                put("displayName", typeAttributeDefinition.fetchDisplayName());
                                put("maximumLength", typeAttributeDefinition.fetchMaximumLength());
                                put("displayName", typeAttributeDefinition.fetchDisplayName());
                                put("sourceName", typeAttributeDefinition.fetchSourceName());
                                put("explanation", typeAttributeDefinition.getExplanation());
//                                put("writesFor", new HashMap() { {
//                                    put("beacon", "weaviate://localhost/f81bfe5e-16ba-4615-a516-46c2ae2e5a80");
//                                } });
                            }
                        })
                        .build();
                objects.add(object);
                objBatcher.withObject(object);
            }
        });

        Result<ObjectGetResponse[]> result = objBatcher.run();

        if (result.hasErrors()) {
            System.out.println(result.getError());
            return null;
        }
        System.out.println(result.getResult());
        // For some reason, batch result does not have response objects;
        // Hence just return  the request objects directly if the call succeeded for now
        return objects;
    }


    static void printTrait(CdmTraitReferenceBase trait) {
        if (!com.microsoft.commondatamodel.objectmodel.utilities.StringUtils.isNullOrEmpty(trait.fetchObjectDefinitionName())) {
            System.out.println("      " + trait.fetchObjectDefinitionName());

            if (trait instanceof CdmTraitReference) {
                for (CdmArgumentDefinition argDef : ((CdmTraitReference) trait).getArguments()) {
                    if (argDef.getValue() instanceof CdmEntityReference) {
                        System.out.println("         Constant: [");

                        CdmConstantEntityDefinition contEntDef =
                                ((CdmEntityReference)argDef.getValue()).fetchObjectDefinition();

                        for (List<String> constantValueList : contEntDef.getConstantValues()) {
                            System.out.println("             " + constantValueList);
                        }
                        System.out.println("         ]");
                    }
                    else
                    {
                        // Default output, nothing fancy for now
                        System.out.println("         " + argDef.getValue());
                    }
                }
            }
        }
    }
}
