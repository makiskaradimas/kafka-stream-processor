package io.github.makiskaradimas.dataenrichment.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import io.github.makiskaradimas.dataenrichment.post.Post;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

public class PostDeserializer<T extends Post> implements Deserializer<T> {

    private final static Logger log = LoggerFactory.getLogger(PostDeserializer.class);

    private ObjectMapper objectMapper = new ObjectMapper();
    private Class<T> tClass;

    public PostDeserializer() {
    }

    public void configure(Map<String, ?> props, boolean isKey) {
        this.tClass = (Class) props.get("DBObjectClass");
    }

    public T deserialize(String topic, byte[] bytes) {
        if (bytes == null) {
            return null;
        } else {
            try {
                T entry = this.objectMapper.readValue(bytes, this.tClass);
                BasicDBObject payload = entry.payload;
                if (entry.getId() != null) {
                    return entry;
                } else if (payload != null && payload.get("_id") != null) {
                    String id = payload.get("_id").toString();
                    if (id.startsWith("{") && JSON.parse(payload.get("_id").toString()) instanceof DBObject) {
                        id = ((DBObject) JSON.parse(payload.get("_id").toString())).get("_id").toString();
                    } else {
                        id = payload.get("_id").toString();
                    }
                    entry.setId(id);
                } else if (payload != null && payload.get("after") != null) {
                    BasicDBObject dbObject = (BasicDBObject) JSON.parse(payload.get("after").toString());
                    Object input;
                    if ((input = dbObject.get("id")) != null) {
                        entry.setId(input.toString());
                    } else if ((input = dbObject.get("_id")) != null) {
                        entry.setId(input.toString());
                    } else {
                        throw new SerializationException("Post id not found during deserialization");
                    }
                    if ((input = dbObject.get("title")) != null) {
                        entry.setTitle(input.toString());
                    }
                    if ((input = dbObject.get("text")) != null) {
                        entry.setText(input.toString());
                    }
                    if ((input = dbObject.get("author")) != null) {
                        entry.setAuthor(input.toString());
                    }
                    if ((input = dbObject.get("category")) != null) {
                        entry.setCategory(input.toString());
                    }
                    if ((input = dbObject.get("status")) != null) {
                        entry.setStatus(input.toString());
                    }
                } else if (payload != null && payload.get("patch") != null) {
                    BasicDBObject dbObject = (BasicDBObject) JSON.parse(payload.get("patch").toString());
                    String input;
                    if (dbObject.get("$set") != null) {
                        if ((input = (String) ((LinkedHashMap) dbObject.get("$set")).get("id")) != null) {
                            entry.setId(input);
                        } else if ((input = (String) ((LinkedHashMap) dbObject.get("$set")).get("_id")) != null) {
                            entry.setId(input);
                        }
                        if ((input = (String) ((LinkedHashMap) dbObject.get("$set")).get("title")) != null) {
                            entry.setTitle(input);
                        }
                        if ((input = (String) ((LinkedHashMap) dbObject.get("$set")).get("text")) != null) {
                            entry.setText(input);
                        }
                        if ((input = (String) ((LinkedHashMap) dbObject.get("$set")).get("author")) != null) {
                            entry.setAuthor(input);
                        }
                        if ((input = (String) ((LinkedHashMap) dbObject.get("$set")).get("category")) != null) {
                            entry.setCategory(input);
                        }
                        if ((input = (String) ((LinkedHashMap) dbObject.get("$set")).get("status")) != null) {
                            entry.setStatus(input);
                        }
                    }
                } else {
                    log.warn("Incomplete data for DBObject: ", new String(bytes));
                }
                return entry;
            } catch (Exception var5) {
                log.error("Exception deserialising post ", var5.getMessage());
                return null;
            }
        }
    }

    public void close() {
    }
}
