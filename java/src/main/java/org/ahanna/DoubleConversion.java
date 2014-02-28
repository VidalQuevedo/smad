package org.ahanna;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.io.EOFException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.apache.hadoop.mapreduce.Mapper.Context;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.*;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.json.JSONArray;

class DoubleConversionMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
    final String Digits     = "(\\p{Digit}+)";
    final String HexDigits  = "(\\p{XDigit}+)";
    // an exponent is 'e' or 'E' followed by an optionally
    // signed decimal integer.
    final String Exp        = "[eE][+-]?"+Digits;
    final String fpRegex    =
      ("[\\x00-\\x20]*"+  // Optional leading "whitespace"
       "[+-]?(" + // Optional sign character
       "NaN|" +           // "NaN" string
       "Infinity|" +      // "Infinity" string

       // A decimal floating-point string representing a finite positive
       // number without a leading sign has at most five basic pieces:
       // Digits . Digits ExponentPart FloatTypeSuffix
       //
       // Since this method allows integer-only strings as input
       // in addition to strings of floating-point literals, the
       // two sub-patterns below are simplifications of the grammar
       // productions from section 3.10.2 of
       // The Java™ Language Specification.

       // Digits ._opt Digits_opt ExponentPart_opt FloatTypeSuffix_opt
       "((("+Digits+"(\\.)?("+Digits+"?)("+Exp+")?)|"+

       // . Digits ExponentPart_opt FloatTypeSuffix_opt
       "(\\.("+Digits+")("+Exp+")?)|"+

       // Hexadecimal strings
       "((" +
        // 0[xX] HexDigits ._opt BinaryExponent FloatTypeSuffix_opt
        "(0[xX]" + HexDigits + "(\\.)?)|" +

        // 0[xX] HexDigits_opt . HexDigits BinaryExponent FloatTypeSuffix_opt
        "(0[xX]" + HexDigits + "?(\\.)" + HexDigits + ")" +

        ")[pP][+-]?" + Digits + "))" +
       "[fFdD]?))" +
       "[\\x00-\\x20]*"); // Optional trailing "whitespace"

    final String sqlDateRegex = Digits + "-" + Digits + "-" + Digits + " " + Digits + ":" + Digits + ":" + Digits; 

    public void fixATweet(JSONObject jsonObj){
        if (jsonObj == null)
            return;
        String str = "";
        if (jsonObj.has("created_at")) {
            str = (String)jsonObj.get("created_at");
            if (!str.equals("null") && !Pattern.matches(sqlDateRegex, str)) {
                String[] splits = str.split(" ");
                String time = splits[5]+" "+splits[1]+" "+splits[2]+" "+splits[3];
                SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy MMM dd HH:mm:ss");
                Date myParsedDate = null;



                try {
                    myParsedDate = timeFormat.parse(time);
                } catch (ParseException e) {
                    // nope
                }
                if (myParsedDate != null) {
                    timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    jsonObj.put("created_at", timeFormat.format(myParsedDate));
                }
            }
        }
        /** Get Coordinates within Geo fixed**/
        if (jsonObj.has("geo")){
            str = (String)jsonObj.get("geo").toString();
            if (!str.equals("null")) {
                //System.out.println(str);
                JSONObject jobjGeo = (JSONObject)jsonObj.get("geo");
                if (jobjGeo.has("coordinates")) {
                    str = (String)jobjGeo.get("coordinates").toString();
                    //System.out.println(str);
                    if (!str.equals("null")) {
                        JSONArray jobj_geo_coordinates = (JSONArray)jobjGeo.get("coordinates");
                        for(int i=0; i<jobj_geo_coordinates.length(); i++) {
                            //System.out.println(jobj_geo_coordinates.toString());
                           // if (jobj_geo_coordinates.get(i) instanceof Integer) {
                            if (Pattern.matches(fpRegex, jobj_geo_coordinates.get(i).toString())) {
                                jobj_geo_coordinates.put(i, Double.valueOf(jobj_geo_coordinates.get(i).toString()));
                                //System.out.println("GEO: " + jobj_geo_coordinates.toString());
                            }
                        }
                    }
                }
            }
        }
        /** Get Coordinates within Coordinates fixed**/
        if (jsonObj.has("coordinates")) {
            str = (String)jsonObj.get("coordinates").toString();
            if (!str.equals("null")) {
                //System.out.println(str);
                JSONObject jobjCoordinates = (JSONObject)jsonObj.get("coordinates");
                str = (String)jobjCoordinates.get("coordinates").toString();
                //System.out.println(str);
                if (!str.equals("null")) {
                    if (jobjCoordinates.has("coordinates")) {
                        JSONArray jobj_coord_coordinates = (JSONArray)jobjCoordinates.get("coordinates");
                        for(int i=0; i<jobj_coord_coordinates.length(); i++) {
                            //System.out.println(jobj_coord_coordinates.toString());
                            //if (jobj_coord_coordinates.get(i) instanceof Integer) {
                            //System.out.println("Coordinates: " + jobj_coord_coordinates.toString());                            
                            if (Pattern.matches(fpRegex, jobj_coord_coordinates.get(i).toString())) {                            
                                jobj_coord_coordinates.put(i, Double.valueOf(jobj_coord_coordinates.get(i).toString()));                                
                            }
                        }
                    }
                }
            }
        }
        /** Get Coordinates within place fixed**/
        if (jsonObj.has("place")){
            str = (String)jsonObj.get("place").toString();
            if (!str.equals("null")) {
                //System.out.println(jsonObj.get("place"));
                JSONObject jobjPlace = (JSONObject)jsonObj.get("place");
                if (jobjPlace != null && jobjPlace.has("bounding_box")){
                    str = (String)jobjPlace.get("bounding_box").toString();
                    //System.out.println(str);
                    if (!str.equals("null")) {
                        JSONObject jobjBB = (JSONObject)jobjPlace.get("bounding_box");
                        if (jobjBB != null && jobjBB.has("coordinates")) {
                            str = (String)jobjBB.get("coordinates").toString();
                            //System.out.println(str);
                            JSONArray jobCoordinates_0 = (JSONArray)jobjBB.get("coordinates");
                            for (int i=0; i<jobCoordinates_0.length(); i++) {
                                JSONArray jobCoordinates_1 = jobCoordinates_0.getJSONArray(i);
                                for (int j=0; j<jobCoordinates_1.length(); j++) {
                                    JSONArray jobCoordinates_2 = jobCoordinates_1.getJSONArray(j);
                                    for(int k=0; k<jobCoordinates_2.length(); k++) {
                                        //if (jobCoordinates_2.get(k) instanceof Integer) {
                                        if (Pattern.matches(fpRegex, jobCoordinates_2.get(k).toString())) {
                                            jobCoordinates_2.put(k, Double.valueOf(jobCoordinates_2.get(k).toString()));
                                            //System.out.println("PLACE: " + jobCoordinates_2.toString());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }//end if
    }

  public void map(Text json, Text nothing, OutputCollector<Text, Text> output,
      Reporter reporter) throws IOException {

        try {
            Text outJson   = new Text();
            String jsonStr = json.toString();

            JSONTokener tokener = new JSONTokener(jsonStr);
            JSONObject jsonObj  = new JSONObject(tokener);

            fixATweet(jsonObj);
            if (jsonObj.has("retweeted_status")) {
                Object retweetObj = jsonObj.get("retweeted_status");
                if(retweetObj.toString() != "null") {
                    fixATweet((JSONObject) retweetObj);
                }
            }

            outJson.set(jsonObj.toString());
            output.collect(outJson, nothing);
        } catch (EOFException e) {
            // do nothing
        } catch (JSONException e) {
            // do nothing
        }
    }
}

public class DoubleConversion {
  public static void main(String[] args) {
    JobConf conf = new JobConf(DoubleConversion.class);
    conf.setJobName("DoubleConversation");

    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(Text.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(DoubleConversionMapper.class);
    conf.setReducerClass(org.apache.hadoop.mapred.lib.IdentityReducer.class);

    // KeyValueTextInputFormat treats each line as an input record, 
    // and splits the line by the tab character to separate it into key and value 
    conf.setInputFormat(KeyValueTextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    try {
        JobClient.runJob(conf);
    } catch (IOException e) {
        // do nothing
    } 
  }
}
