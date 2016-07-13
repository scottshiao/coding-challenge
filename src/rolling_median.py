# -*- coding: utf-8 -*-
"""
Created on Fri Jul  8 23:44:12 2016

@author: PCUSER
"""

import datetime
import json
import statistics
import sys
import time

"""
Convert a string timestamp into total seconds represented by an integer.
Behavior is similar to strptime but attempts to speed up parsing for the most
common timestamp string (%Y-%m-%dT%H:%M:%SZ) in the venmo file
"""
def convert_timestamp(ts_str: str, format_str: str):
    l = len(ts_str);
    
    # We know the format of our timestamps:
    # %Y-%m-%dT%H:%M:%S ex. 2016-04-07T03:34:18Z
    # So use a custom formatting to speed up parsing
    if format_str == '%Y-%m-%dT%H:%M:%SZ' and l == 20:
        dt = datetime.datetime(
                                 int(ts_str[0:4]), # Year
                                 int(ts_str[5:7]), # Month
                                 int(ts_str[8:10]), # Day
                                 int(ts_str[11:13]), # Hours
                                 int(ts_str[14:16]), # Minutes
                                 int(ts_str[17:19]), # Seconds
                                 );
        return int(time.mktime(dt.timetuple()));

    # Alternative is to use strptime
    dt = datetime.datetime.strptime(ts_str, format_str);
    return int(time.mktime(dt.timetuple()));

"""
Connect two actors together.
Uses an adjacency list to represent the edges of the graph.
Duplicates are allowed in actor_list in order to keep a running count of the 
edges and should be used in conjunction with the payments data structure when
determining degrees of edges.
"""
def connect_actors(actor_list, actor1, actor2):

    if not actor_list.get(actor1):
        actor_list[actor1] = [];

    actor_list[actor1].append(actor2);

    if not actor_list.get(actor2):
        actor_list[actor2] = [];

    actor_list[actor2].append(actor1);

    return actor_list;

"""
Remove any edges beyond the 60s window.
'payments' is assumed to have the structure [actor1, actor2, pay_time].
'actors_list' is assumed to be an adjacency list of the edges of the graph.
"""
def evict_edge(payments, actor_list, min_time):
    if payments:

        # List comprehension for any payments older than 60s
        old_payments = [payment for payment in payments if int(payment[2]) <= min_time];

        # List comprehension to get any payment not an old payment (still within 60s)
        new_payments = [payment for payment in payments if not payment in old_payments];

        for payment in old_payments:
            actor1 = payment[0];
            actor2 = payment[1];
            try:
                # Assumes that both will fail or succeed
                actor_list[actor1].remove(actor2);
                actor_list[actor2].remove(actor1);
            except ValueError:
                continue;

        return new_payments, actor_list;
    return payments, actor_list;

"""
Calculate median
"""
def calculate_median(actor_list):
    degrees = [];
    for key, edges in actor_list.items():
        # We get rid of the duplicates by using set()
        degree = len(set(edges));

        # Exclude all actors with 0 edges
        if degree > 0:
            degrees.append(degree);

    median = statistics.median(degrees);
    return median;

"""
Write median to output file
"""
def write_median(median, f_out):
    median_str = '{:3.2f}'.format(median) + '\n';
    f_out.write(median_str);


def main():

    # load data
    payments = [];
    actors = {};
    max_time = 0;
    min_time = 0;
    median = 0;
    date_format_str = '%Y-%m-%dT%H:%M:%SZ';
    out_file = "venmo_output/output.txt";
    in_file = "venmo_input/venmo-trans.txt";
    
    # We are expecting both input and output
    # If either is not there just use default strings
    if len(sys.argv) == 3:
        in_file = sys.argv[1];
        out_file = sys.argv[2];
        
    f_out = open(out_file,'w');
    
    with open(in_file) as f_in:
    
        for line in f_in:
            # Parse JSON
            j_line = json.loads(line);
    
            # If any field is missing ignore the entry and move to the next one
            if not all (k in j_line.keys() for k in ('actor', 'target', 'created_time')):
                continue;
    
            actor = j_line['actor'];
            target = j_line['target'];
            ts = j_line['created_time'];
    
            # If either actor or target are empty strings
            # or if actor == target, it is an invalid entry and should be ignored
            if not actor or not target or actor == target:
                continue;
    
            # Convert timestamp to total seconds
            try:
                curr_time = convert_timestamp(ts, date_format_str);
            except ValueError:
                continue;
    
            # Update max time if necessary
            if curr_time > max_time:
                max_time = curr_time;
                min_time = max_time - 60;
    
                # Remove expired data
                payments, actors = evict_edge(payments, actors, min_time);
    
            # Transactions that are below our min threshold output a new median
            # but otherwise are not processed
            if curr_time <= min_time:
                write_median(median, f_out);
                continue;
    
            # Connect actor and target to each other
            actors = connect_actors(actors, actor, target);
    
            # Store data entry
            payments.append([actor,target,curr_time]);
    
            # Calculate median
            median = calculate_median(actors);
    
            # Output median
            write_median(median, f_out);
    f_out.close();

if __name__ == "__main__":
    main();

