/*
 * Project 4.3 - Implement a uber-like service by Kafka and Samza
 * Hao Wang - haow2
 */

package com.cloudcomputing.samza.pitt_cabs;

import java.util.*;
import java.io.*;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

/**
 * Consumes the stream of driver location updates and rider cab requests.
 * Outputs a stream which joins these 2 streams and gives a stream of rider
 * to driver matches.
 */
public class DriverMatchTask implements StreamTask, InitableTask, WindowableTask {

  /* Define per task state here. (kv stores etc) */
  private KeyValueStore<String, Map<String, Object>> driver_list;
  private final int MAX_DIST = 500;


  @Override
  @SuppressWarnings("unchecked")
  public void init(Config config, TaskContext context) throws Exception {
    System.out.println("Begin init!");
    // Initialize stuff (maybe the kv stores?)
    driver_list = (KeyValueStore<String, Map<String, Object>>) context.getStore("driver-list");
    System.out.println("End init!");
  }


  @Override
  @SuppressWarnings("unchecked")
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    // The main part of your code. Remember that all the messages for a particular partition
    // come here (somewhat like MapReduce). So for task 1 messages for a blockId will arrive
    // at one task only, thereby enabling you to do stateful stream processing.
    System.out.println("Begin process!");
    String incomingStream = envelope.getSystemStreamPartition().getStream();

    if (incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())) {
      process_location((Map<String, Object>) envelope.getMessage());
    } else if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {
      process_event((Map<String, Object>) envelope.getMessage(), collector);
    } else {
      throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
    }
    System.out.println("End process!");
  }


  private void process_location(Map<String, Object> message) {
    System.out.println("Begin process_location!");
    
    // Get parameters contained in the message
    int block_id  = (int) message.get("blockId");
    int driver_id = (int) message.get("driverId");
    int latitude  = (int) message.get("latitude");
    int longitude = (int) message.get("longitude");
    System.out.println("Finished getting parameters");
    System.out.println(block_id + ";" + driver_id + ";" + latitude + ";" + longitude);

    // Update current location
    System.out.println("driver_list.get(block_id): " + driver_list.get(""+block_id));
    Map<String, Object> map = driver_list.get(""+block_id);
    System.out.println("list: " + map);
    if (map == null) {
      map = new HashMap<String, Object>();
    }
    if (!map.containsKey(""+driver_id)) {
      map.put(""+driver_id, latitude + ";" + longitude);
    } else {
      String item = (String) map.get(""+driver_id);
      String[] items = item.split(";");
      if (items.length == 2) {
        map.put(""+driver_id, latitude + ";" + longitude);
      } else if (items.length == 5) {
        map.put(""+driver_id, latitude + ";" + longitude + ";" + items[2] + ";" + items[3] + ";" + items[4]);
      }
    }
    driver_list.put(""+block_id, map);
    System.out.println("End process_location!");
  }


  private void process_event(Map<String, Object> message, MessageCollector collector) {
    System.out.println("Begin process_event!");
    
    // Get parameters contained in the message
    int driver_id, block_id, latitude, longitude, salary, client_id;
    double rating;
    String status, gender, gender_preference, type;
    block_id  = (int) message.get("blockId");
    type   = message.get("type").toString();

    // Get the type from the message and so to the function
    switch(type) {
      case "LEAVING_BLOCK":
        status = (String) message.get("status");
        if (!status.equals("AVAILABLE")) {
          driver_id = (int) message.get("driverId");
          process_leaving(block_id, driver_id);
        }
        break;
      case "ENTERING_BLOCK":
        driver_id = (int) message.get("driverId");
        latitude  = (int) message.get("latitude");
        longitude = (int) message.get("longitude");
        status = (String) message.get("status");
        gender = (String) message.get("gender");
        rating = (double) message.get("rating");
        salary    = (int) message.get("salary");
        process_entering(block_id, driver_id, latitude, longitude, status, gender, rating, salary);
        break;
      case "RIDE_REQUEST":
        client_id = (int) message.get("clientId");
        latitude  = (int) message.get("latitude");
        longitude = (int) message.get("longitude");
        gender_preference = (String) message.get("gender_preference");
        process_request(block_id, client_id, latitude, longitude, gender_preference, collector);
        break;
      case "RIDE_COMPLETE":
        driver_id = (int) message.get("driverId");
        latitude  = (int) message.get("latitude");
        longitude = (int) message.get("longitude");
        gender = (String) message.get("gender");
        rating = (double) message.get("rating");
        salary    = (int) message.get("salary");
        process_complete(block_id, driver_id, latitude, longitude, gender, rating, salary);
        break;
      default:
        throw new IllegalStateException("Unexpected type in processEvent: " + type);
    }
    System.out.println("End process_event!");
  }


  /*
   * Handle the situation when the type is LEAVING_BLOCK
   */
  private void process_leaving(int block_id, int driver_id) {
    System.out.println("Begin process_leaving!");
    System.out.println(block_id + ";" + driver_id);
    
    // Remove driver in driver_list
    // If the list doesn't contain driver_list, then needn't to remove it
    Map<String, Object> map = driver_list.get(""+block_id);
    if (map != null) {
      map.remove(""+driver_id);
      driver_list.put(""+block_id, map);
    }
    System.out.println("End process_leaving!");
  }


  /*
   * Handle the situation when the type is ENTERING_BLOCK
   */
  private void process_entering(int block_id, int driver_id, int latitude, int longitude, String status, String gender, double rating, int salary) {
    System.out.println("Begin process_entering!");
    System.out.println(block_id + ";" + driver_id + ";" + latitude + ";" + longitude + ";" + status + ";" + gender + ";" + rating + ";" + salary);
    
    // If the list doesn't contain driver_list, then add it
    Map<String, Object> map = driver_list.get(""+block_id);
    if (map == null) {
      map = new HashMap<String, Object>();
      driver_list.put(""+block_id, map);
    }
    // Update the location and personal information
    if (status.equals("AVAILABLE")) {
      map.put(""+driver_id, latitude + ";" + longitude + ";" + gender + ";" + rating + ";" + salary);
      driver_list.put(""+block_id, map);
    }
    System.out.println("End process_entering!");
  }


  /*
   * Handle the situation when the type is RIDE_REQUEST
   */
  private void process_request(int block_id, int client_id, int latitude, int longitude, String gender_preference, MessageCollector collector) {
    System.out.println("Begin process_request!");
    System.out.println(block_id + ";" + client_id + ";" + latitude + ";" + longitude + ";" + gender_preference);
    
    // If the list doesn't contain driver_list, then return
    Map<String, Object> map  = driver_list.get(""+block_id);
    if (map == null) {
      return;
    }
    double best_score = Double.MIN_VALUE;
    int best_driver = -1;

    // Traverse the list to find the best driver
    for (String driver_id : map.keySet()) {
      String item = (String) map.get(""+driver_id);
      String[] items = item.split(";");

      // Handle Illegal Input
      if (items.length != 5) {
        continue;
      }

      // Get parameters
      int driver_latitude = Integer.parseInt(items[0]);
      int driver_longitude = Integer.parseInt(items[1]);
      String driver_gender = items[2];
      double driver_rating = Double.parseDouble(items[3]);
      int salary = Integer.parseInt(items[4]);

      // Compute distance score
      double distance_score = 1.0 - ((latitude - driver_latitude)*(latitude - driver_latitude) + 
                              (longitude - driver_longitude)*(longitude - driver_longitude)) / MAX_DIST;
      
      // Compute gender score
      double gender_score = 0.0;
      if (gender_preference.equals("N")) {
        gender_score = 1.0;
      } else {
        if (gender_preference.equals(driver_gender)) {
          gender_score = 1.0;
        } else {
          gender_score = 0.0;
        }
      }

      // Compute rating score
      double rating_score = driver_rating / 5.0;

      // Compute salary score
      double salary_score = 1.0 - salary / 100.0;

      // Compute match score
      double match_score = distance_score * 0.4 + gender_score * 0.2 + rating_score * 0.2 + salary_score * 0.2;

      // Update the best score and the best driver
      if (match_score > best_score) {
        best_score = match_score;
        best_driver = Integer.parseInt(driver_id);
      }
    }

    // Remove the driver from the list
    if (best_driver != -1) {
      map.remove(""+best_driver);
      driver_list.put(""+block_id, map);
    }

    // Send the information
    Map<String, Object> new_match = new HashMap<String, Object>();
    new_match.put("driverId", best_driver);
    new_match.put("clientId", client_id);

    // Send to match stream
    collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.MATCH_STREAM, new_match));
    System.out.println("End process_request!");
  }


  /*
   * Handle the situation when the type is RIDE_COMPLETE
   */
  private void process_complete(int block_id, int driver_id, int latitude, int longitude, String gender, double rating, int salary) {
    System.out.println("Begin process_complete!");
    System.out.println(block_id + ";" + driver_id + ";" + latitude + ";" + longitude + ";" + gender + ";" + rating + ";" + salary);
    
    // If the list doesn't contain driver_list, then add it
    Map<String, Object> map = driver_list.get(""+block_id);
    if (map == null) {
      map = new HashMap<String, Object>();
      driver_list.put(""+block_id, map);
    }
    // Update the location and personal information
    map.put(""+driver_id, latitude + ";" + longitude + ";" + gender + ";" + rating + ";" + salary);
    driver_list.put(""+block_id, map);  
    System.out.println("End process_complete!");
  }


  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) {
	//this function is called at regular intervals, not required for this project
  }
}
