/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ounl.elasticsearchtest;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import org.elasticsearch.search.SearchHit;


/**
 *
 * @author gla
 */
public class ElasticSearchTest {
    public static void main(String[] args) {
        //testPutRecord();
        //testUpdateRecord();
        testSearchAll();
    }
    
    static void testPutRecord(){
        try {
            // Create a client
            TransportClient client = TransportClient.builder().build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
            
            // Prepare a document for indexing
            Map<String, Object> json = new HashMap<String, Object>();
            json.put("taskid","level2");
            json.put("trial", 1);
            json.put("max", 0);
            json.put("min", 0);
            json.put("sum", 0);
            json.put("variance", 0);
            json.put("mean", 0);
            json.put("stddev", 0);
            json.put("skewness", 0);
            json.put("kurtosis", 0);
            json.put("n", 1);
            json.put("normal", false);
            json.put("help1", 0);
            json.put("help2", 0);
            json.put("help3", 0);
            
            // Do some indexing (PUT the document)
            IndexResponse response = client.prepareIndex("perfstat","descriptive")
                    .setSource(json)
                    .get();
            
            // Prepare an additional document for indexing
            json.put("taskid","level2");
            json.put("trial", 2);
            json.put("max", 0);
            json.put("min", 0);
            json.put("sum", 0);
            json.put("variance", 0);
            json.put("mean", 0);
            json.put("stddev", 0);
            json.put("skewness", 0);
            json.put("kurtosis", 0);
            json.put("n", 1);
            json.put("normal", false);
            json.put("help1", 0);
            json.put("help2", 0);
            json.put("help3", 0);
            
            // Indexing an additional document
            response = client.prepareIndex("perfstat","descriptive")
                    .setSource(json)
                    .get();
            
            // Finally
            client.close();    
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
    
    static void testUpdateRecord(){
        try {
            // Create a client
            TransportClient client = TransportClient.builder().build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
            
            // Prepare a document for indexing
            Map<String, Object> json = new HashMap<String, Object>();
            json.put("taskid","level2");
            json.put("trial", 1);
            json.put("max", 0);
            json.put("min", 0);
            json.put("sum", 0);
            json.put("variance", 0);
            json.put("mean", 5);
            json.put("stddev", 0);
            json.put("skewness", 0);
            json.put("kurtosis", 0);
            json.put("n", 1);
            json.put("normal", false);
            json.put("help1", 0);
            json.put("help2", 0);
            json.put("help3", 0);
            
            QueryBuilder qb = boolQuery()
                    .must(termQuery("taskid", json.get("taskid")))
                    .must(termQuery("trial", json.get("trial")));

            // Query to see if the new document already exists
            SearchResponse response = client.prepareSearch("perfstat")
                    .setTypes("descriptive")
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setQuery(qb)
                    .execute().actionGet();
            
            // Get the found id
            // Extract individual results from the query
            SearchHit[] results = response.getHits().getHits();
            
            String id = "";
            Map<String, Object> output = new HashMap<String, Object>();
            for (SearchHit hit : results) {
                
                if (hit != null) {
                    id = hit.getId();
                }
            }
            
            
            // Do an upsert if a previous document was found else just insert
            if (id != "") {
                // Upsert
                IndexRequest indexRequest = new IndexRequest("perfstat","descriptive")
                        .source(json);
                UpdateRequest updateRequest = new UpdateRequest("perfstat","descriptive",id)
                        .doc(json)
                        .upsert(indexRequest);
                client.update(updateRequest).get();
            } else {
                // New insert here
                
            }
            
            // Finally
            client.close();    
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
    
    static void testSearchAll(){
        try {
            // Create a client
            TransportClient client = TransportClient.builder().build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
            
            //Search all documents in elasticsearch and print them            
            // Prepare a document for collecting search output
            Map<String, Object> json = new HashMap<String, Object>();
            
            // Build a (must) query to find a specific task/trial combination
            QueryBuilder qb = boolQuery()
                    .must(termQuery("taskid", "level2"))
                    .must(termQuery("trial", 1));
            
            // Run a query
            SearchResponse response = client.prepareSearch("perfstat")
                    .setTypes("descriptive")
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setQuery(qb)
                    .execute().actionGet();
            
//            // Query all documents (for testing)
//            SearchResponse response = client.prepareSearch().execute().actionGet();
            
            // Extract individual results from the query
            SearchHit[] results = response.getHits().getHits();
            
            // Print the query results as json objects
            Map<String, Object> output = new HashMap<String, Object>();
            for (SearchHit hit : results) {
                
                if (hit != null) {
                    output = hit.getSource();
                    System.out.println(output);
                }
            }
            
            // Finally
            client.close();    
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
}