#!/usr/bin/env python
# coding: utf-8

# #### LATAM QMS : Fast Track Module
# ##### Run quality / fraud queries on presto and tag / note courier partners
# ###### Runs 3 times a day: 0800, 1400, 2200 UTC

# Eats LATAM QMS : Fast Track
# @ bhama@uber.com, shane.macnamara@uber.com, aescalante@uber.com

# authorize Fast Track Actions Control Center (global)

# Standard Library Imports
from datetime import (
  datetime,
  timedelta
)
import json
from os import path

# External package imports
import pandas as pd
import pygsheets
from queryrunner_client import Client
from tchannel import thrift
from tchannel.sync import TChannel

rule_list_template = """SELECT
        city_id,
        rule_name,
        threshold,
        lookback_period,
        Rule_Pillar,
        score_in_pillar
    FROM (
        VALUES
%s
    ) as actions_control_center (city_id, rule_name, threshold, lookback_period, Rule_Pillar, score_in_pillar)\
"""

rule_template = """\
            (%d, '%s', %s, %d, '%s', %d)\
"""

root_dir = path.dirname(__file__)
saf_path = path.join(root_dir,'fast-track-user.json')
gc = pygsheets.authorize(service_account_file=saf_path)
# gsheet_name = 'LatAm Fast-Track - Actions Control Center'
gsheet_key = '1FvqZ0mmLtaTmJ1xOUFHCUU3F1nPLkMofand0FK6TFPk'
# gs = gc.open(gsheet_name)
gs = gc.open_by_key(gsheet_key)

# initializing populous services
with open("/etc/uber/hyperbahn/hosts.json") as f:
    known_peers = json.load(f)
global tchannel 
tchannel = TChannel(name="tcurl", known_peers=known_peers)
global populous_service
populous_service= thrift.load(path=path.join(root_dir, 'populous.thrift'), service="populous")

def decode_value(value):
    if type(value)==str:
        return value.decode('UTF-8')
    else:
        return value

# check if fast track is switched off
def is_fast_track():
    
    ws_name = 'locked_actions'

    # open worksheet and load settings
    ws = gs.worksheet_by_title(ws_name)
    c1 = ws.cell('C1').value
    
    return c1=='Run Fast Track'

# local function to call saved queries from queryrunner_client 
def saved_query(query, datestr, rules, city_ids):
    q = Client(user_email = 'brunoa@uber.com')
    param = {'rule_list': rules, 'datestr': datestr, 'city_ids': str(city_ids)}
    e = q.execute_report(query, parameters = param)
    d = e.fetchall()
    return d

# query runner engine for building parameters and executing query
def qb_engine(df, query, datestr):
    result_list = []
    try:
        if not df.empty:
            # Build query parameters (from the enabled rules in df)
            city_ids = ','.join(map(str,df['City ID'].unique()))
            rule_list = []
            for index,row in df.iterrows():
                threshold = row['Threshold']
                assert isinstance(threshold, float), 'Float assumption is incorrect'
                adjusted_threshold = int(threshold) if threshold.is_integer() else threshold
                rule_list.append(rule_template % (row['City ID'], row['Rule'], adjusted_threshold, row['Lookback'], row['Pillar'], row['Pillar Score']))
            rule_list_string = rule_list_template % ',\n'.join(rule_list)
            result_list = saved_query(query, datestr, rule_list_string, city_ids)
    except:
        print 'Exception was thrown in qb_engine for: %s' % query
        result_list = None
    return result_list

# cool off and query runner engine for building parameters and executing cool off logic
def co_engine(df, query, datestr):
    
    try:  
        # separating the dataframe by rule group " notification"
        df_n = df[df['Rule'].str.contains("notification")]

        if df_n.empty:
            print 'No notification rules are enabled. Returning empty list'
            rd = []
        else:
            # build the cool down parameters
            cool_down_list = []
            enabled_cities = df_n['City ID'].unique()
            for city_id in enabled_cities:
                # Get maximum Lookback for each enabled city
                city_df = df_n[df_n['City ID']==city_id]
                cool_down_list.append({
                        'City ID': str(city_id),
                        'Lookback': str(city_df['Lookback'].max())
                })

            df_c = pd.Dataframe(cool_down_list)
            # Cooloff logic to exclude the couriers
            # from the actioning process

            if not df_c.empty:
                # initiate query parameters for cooldown
                rules = "select 0 as city_id, 0 as lookback_period"
                for index,row in df_c.iterrows():
                    rules += " union select "+str(row['City ID'])+ ", "+str(row['Lookback'])
                city_ids = ','.join(map(str,df_c['City ID'].unique()))
                rd = saved_query(query,datestr,rules,city_ids)
            else:
                print 'df_c was empty in co_engine'
    except:
        rd = None
    return rd

# load city / rule settings for fast track
def load_fast_track(active_status, city_filter=None):
    
    ws_name = 'fast_track'

    # open gsheet
    ws = gs.worksheet_by_title(ws_name)
    rd_set = ws.get_all_records()

    # make data frame for manipulation
    df_set = pd.DataFrame(rd_set)

    # keep settings for enabled rules only 
    df_set = df_set[df_set['Active'] == active_status]
    
    # Use this parameter to test in only one city
    if city_filter:
        df_set = df_set[df_set['City ID'] == city_filter]

    return df_set

# write to fast track courier log 
# def courier_log(df_final,gsheet_name):
def courier_log(df_final,gsheet_key):
    # creating log 
    df_final = df_final.applymap(decode_value)
    # open gsheet
    gs = gc.open_by_key(gsheet_key)
#     gs = gc.open(gsheet_name)

    if run_cycle == 1:
        # pop old data and push new data
        ws = gs.worksheet_by_title('today_5')
        df_temp = ws.get_as_df(has_header=True)
        ws = gs.worksheet_by_title('today_6')
        ws.clear()
        df_temp = df_temp.applymap(decode_value)
        ws.set_dataframe(df_temp,(1,1),copy_index=False, copy_head=True, fit=True)

        ws = gs.worksheet_by_title('today_4')
        df_temp = ws.get_as_df(has_header=True)
        ws = gs.worksheet_by_title('today_5')
        ws.clear()
        df_temp = df_temp.applymap(decode_value)
        ws.set_dataframe(df_temp,(1,1),copy_index=False, copy_head=True, fit=True)

        ws = gs.worksheet_by_title('today_3')
        df_temp = ws.get_as_df(has_header=True)
        ws = gs.worksheet_by_title('today_4')
        ws.clear()
        df_temp = df_temp.applymap(decode_value)
        ws.set_dataframe(df_temp,(1,1),copy_index=False, copy_head=True, fit=True)

        ws = gs.worksheet_by_title('today_2')
        df_temp = ws.get_as_df(has_header=True)
        ws = gs.worksheet_by_title('today_3')
        ws.clear()
        df_temp = df_temp.applymap(decode_value)
        ws.set_dataframe(df_temp,(1,1),copy_index=False, copy_head=True, fit=True)

        ws = gs.worksheet_by_title('today_1')
        df_temp = ws.get_as_df(has_header=True)
        ws = gs.worksheet_by_title('today_2')
        ws.clear()
        df_temp = df_temp.applymap(decode_value)
        ws.set_dataframe(df_temp,(1,1),copy_index=False, copy_head=True, fit=True)

        ws = gs.worksheet_by_title('today')
        df_temp = ws.get_as_df(has_header=True)
        ws = gs.worksheet_by_title('today_1')
        ws.clear()
        df_temp = df_temp.applymap(decode_value)
        ws.set_dataframe(df_temp,(1,1),copy_index=False, copy_head=True, fit=True)

        ws = gs.worksheet_by_title('today')
        ws.clear()
        ws.set_dataframe(df_final,(1,1),copy_index=False, copy_head=True, fit=True)

    else: 
        ws = gs.worksheet_by_title('today')
        df_temp = ws.get_as_df(has_header=True)
        ws.set_dataframe(df_final,(df_temp.shape[0]+2,1),copy_index=False, copy_head=False, fit=True)

def get_dataframes(fast_track_df):
    df_dict = {}
    qb_report_ids = fast_track_df['Query'].unique()
    for qb_report_id in qb_report_ids:
        df_dict[qb_report_id] = fast_track_df[fast_track_df['Query']==qb_report_id]
    return df_dict

def update_status(rd,rule,time):

    ws = gs.worksheet_by_title('locked_actions')
    numRows = len(ws.get_all_values(returnas='matrix'))
    ws.add_rows(1)
    
    rule_cell = "A" + str((numRows+1))
    time_cell = "B" + str((numRows+1))
    status_cell = "C" + str((numRows+1))
    
    ws.cell(rule_cell).value = rule
    ws.cell(time_cell).value = time
    if rd is None:
        ws.cell(status_cell).value = 'Query exception'
    elif len(rd) == 0:
        ws.cell(status_cell).value = 'Empty results'
    else:
        ws.cell(status_cell).value = 'Success'

# check existence of notification tag, and tag / note only for new courier partners
def tag_notes_courier(all_results):

    for row in all_results:
        # check for notification tag
        t1 = tchannel.thrift(populous_service.UserService.getUserTag(row['driver_uuid'], row['tag']))

        try:
            t2=t1.result()
            # Courier alreaady had the tag (did not take action)
            row['Status'] = "Not Actioned"
        except:
            tchannel.thrift(populous_service.UserService.createUserTag(row['driver_uuid'], row['tag']))
            tchannel.thrift(populous_service.UserService.createUserNote(row['driver_uuid'], row['note']))
            # Courier did not have tag (action was taken)
            row['Status'] = "Actioned"

            if row['tag'].find('notification') <> -1:
                tchannel.thrift(populous_service.UserService.createUserTag(row['driver_uuid'],notification_tracking_tag))

            if row['tag'].find('immediatewl') <> -1:
                tchannel.thrift(populous_service.UserService.createUserTag(row['driver_uuid'],waitlist_tracking_tag))

if is_fast_track():
    # Load enabled rules from Fast-Track - Actions Control Center
    df = load_fast_track('Enable')
    # Divide the enabled rules by Query
    df_dict = get_dataframes(df)
    
    now = datetime.now().replace(microsecond=0)
    nowstr = str(now)
    days_ago = now - timedelta(days=60)
    query_datestr = str(days_ago.date())
    
    if now.hour < 10:
        # This is the first run of the day
        # Remove 7-day-old data, push data one day, and add today's data in first sheet
        run_cycle = 1
    else:
        # Append data in the first sheet
        run_cycle = 2
      
    all_results = []
    for qb_report_id in df_dict:
        results = qb_engine(df_dict[qb_report_id], qb_report_id, query_datestr)
        update_status(results, 'QB Report ID: %s' % qb_report_id, nowstr)
        if results is not None:
            all_results += results
    
    # notification and tracking tags 
    notification_tracking_tag = 'latam_fraud_fasttrack_soft_tag'
    waitlist_tracking_tag = 'latam_fraud_fasttrack_wl_queue'

    #tag and add notes
    tag_notes_courier(all_results)
    
    col_report_id = 'e5kwFFUNB' # cool off logic
    col_results = co_engine(df, col_report_id, query_datestr)
    update_status(col_results, 'Cool off logic: %s' % col_report_id, nowstr)
    #add cooloff output to all_results after tagging and actioning
    if col_results is not None:
        for row in col_results:
            tchannel.thrift(populous_service.UserService.createUserTag(row['driver_uuid'], row['tag']))
            row['Status'] = "Actioned"
        all_results += col_results
    
        
    # if output from all queries is null 
    if all_results:
        df_f = pd.DataFrame(all_results)
        df_f['timestamp'] = nowstr
    else:
        df_f = pd.DataFrame()
        df_f['Status'] = ''
        df_f['city_id'] = ''
        df_f['driver_uuid'] = ''
        df_f['note'] = ''
        df_f['tag'] = ''
        df_f['timestamp'] = ''
        df_f = df_f.append({'Status': 'Null query output', 'city_id': 'Null query output', 'driver_uuid': 'Null query output', 'note': 'Null query output', 'tag': 'Null query output', 'timestamp': nowstr}, ignore_index=True)

    # Write to the google sheets log
    courier_log(df_f,'1lWlxD5t49uPEOj_1QOb8W2HdqqRrC_JA-qS8QzBB9gY')
    # End of execution   
else:
    print 'Fast Track is Off because locked_actions!C1 != "Run Fast Track"'
