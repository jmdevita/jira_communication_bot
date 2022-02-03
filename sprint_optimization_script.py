# %%
import pandas as pd
import numpy as np
from atlassian import Jira
import os, re, random
from decimal import Decimal
from time import sleep
from dateutil.parser import isoparse

import boto3, random
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb')

# %%
def create_table(table_name):
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'sprint_id',
                'KeyType': 'HASH'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'sprint_id',
                'AttributeType': 'N'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )

    # Wait until the table exists.
    table.meta.client.get_waiter('table_exists').wait(TableName=table_name)

    # Print out some data about the table.
    print("Table Made")

def create_ticket_table(table_name):
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'ticket_id',
                'KeyType': 'HASH'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'ticket_id',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )

def create_id_table(table_name):
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'id',
                'KeyType': 'HASH'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'id',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )

    # Wait until the table exists.
    table.meta.client.get_waiter('table_exists').wait(TableName=table_name)

    # Print out some data about the table.
    print("Table Made")

    # Wait until the table exists.
    table.meta.client.get_waiter('table_exists').wait(TableName=table_name)

    # Print out some data about the table.
    print("Table Made")

def get_sprint_id(sprint_name):
    table= dynamodb.Table('sprint_information')

    response = table.query(
        IndexName='sprint_name-index',
        KeyConditionExpression=Key('sprint_name').eq(sprint_name))

    sprint_id = response["Items"][0]['sprint_id']

    return int(sprint_id)

def new_sprint(sprint_name):
    #Capture sprint information
    project = "DATA"

    user = os.getenv('JIRA_USERNAME')
    api_key = os.getenv('JIRA_API')
    jira = Jira('https://wellapp.atlassian.net', user, api_key)
    fields = ['key']
    df = pd.DataFrame()

    count = 0
    jql = 'project = "%s" AND Sprint = "%s"' % (project, sprint_name)

    results = jira.jql(jql, start=count,  fields = fields)
    ticket = jira.issue(results['issues'][0]['key'])

    sprint_type = ticket['fields']['customfield_10003']

    number = 0
    for sprint in sprint_type:
        if sprint['name'] == sprint_name:
            break
        else:
            number += 1

    sprint_id = ticket['fields']['customfield_10003'][number]['id']
    sprint_name = ticket['fields']['customfield_10003'][number]['name']
    quarter = pd.Timestamp(isoparse(ticket['fields']['customfield_10003'][number]['startDate'])).quarter
    year = isoparse(ticket['fields']['customfield_10003'][number]['startDate']).year

    global start_date
    global end_date

    start_date = str(pd.to_datetime(isoparse(ticket['fields']['customfield_10003'][number]['startDate']) - pd.tseries.offsets.QuarterBegin(startingMonth=1)).date())
    end_date = str(isoparse(ticket['fields']['customfield_10003'][number]['endDate']).date())

    ## Add a record with a list
    sprint_key = random.getrandbits(64)
    table= dynamodb.Table('sprint_information')
    table.put_item(
        Item={
                'quarter': quarter,
                'year': year,
                'sprint_name': sprint_name,
                'sprint_number': re.findall("\d+", sprint_name)[0],
                'sprint_id': sprint_key
            }
        )
    print("Sprint Record Added")

def sprint_bug_stats(bug_count, avg_time_resolved, st_dev, sprint_id):
    
    table= dynamodb.Table('sprint_bug_information')
    ## Add a record with a list
    table.put_item(
        Item={
                'qtr_bug_count': str(bug_count),
                'avg_time_resolved': str(avg_time_resolved),
                'st_dev': str(st_dev),
                'sprint_id': int(sprint_id)
            }
        )
    print("Sprint Bugs Added")

def sprint_story_points(sprint_name):
    count = 0

    project = "DATA"
    jql = 'project = DATA and Sprint = "%s" and (type != Sub-Task AND type != Epic) and "Flagged[Checkboxes]" IS EMPTY' % sprint_name
    user = os.getenv('JIRA_USERNAME')
    api_key = os.getenv('JIRA_API')

    jira = Jira('https://wellapp.atlassian.net', user, api_key)
    fields = ['key','summary','status', 'assignee','priority',"issuetype", "customfield_10008", "customfield_10899", "description", "customfield_10827", "created", "labels"]

    results = jira.jql(jql, start=count, fields = fields)
    df = pd.json_normalize(results["issues"])
    count += 50

    while count < jira_count(project):
        results = jira.jql(jql, start=count, fields=fields)
        if results['issues'] == []:
            break

        df = df.append(pd.json_normalize(results["issues"]))
        count += 50

    df["fields.url"] = 'https://wellapp.atlassian.net/browse/'+df["key"]
    df = df[['key', 'fields.issuetype.name','fields.created', 'fields.customfield_10008','fields.customfield_10827', 'fields.summary', 'fields.status.name', 'fields.description', 'fields.assignee.displayName', 'fields.customfield_10899', 'fields.priority.name', 'fields.url', 'fields.labels']]

    df = df.reset_index(drop=True)
    df['fields.customfield_10008'] = df['fields.customfield_10008'].fillna(0)

    for num in range(len(df)):
        if df['fields.issuetype.name'][num] == "Task":
            df.at[num, 'fields.customfield_10008'] = 0.5
        elif df['fields.issuetype.name'][num] == "Story":
            continue
        else:
            df.at[num, 'fields.customfield_10008'] = 0
    df_2 = df.agg(Sum = ('fields.customfield_10008', 'sum'))
    result = df_2['fields.customfield_10008'][0]
    return result

def sprint_ticket_stats(ticket_count, story_points, avg_time_tickets_completed, percent_completed, sprint_id):
    
    table= dynamodb.Table('sprint_ticket_information')
    ## Add a record with a list
    table.put_item(
        Item={
                'ticket_count': str(ticket_count),
                'story_points': str(story_points),
                'avg_time_tickets_completed': str(avg_time_tickets_completed),
                'percent_completed': str(percent_completed),
                'sprint_id': int(sprint_id)
            }
        )
    print("Sprint Ticket Analysis Added")

def jira_count(project):
    user = os.getenv('JIRA_USERNAME')
    api_key = os.getenv('JIRA_API')

    jira = Jira('https://wellapp.atlassian.net', user, api_key)

    results = jira.get_project_issues_count(project)

    return results



def main(sprint_name = 'Panda Dash 74'):
    if sprint_name == "":
        exit
    
    # To add a new cell, type '# %%'
    # To add a new markdown cell, type '# %% [markdown]'
    # %%
    try:
        create_table('sprint_information')
    except:
        print("Sprint Table already created")

    print("Connecting to Jira")
    project = "DATA"

    user = os.getenv('JIRA_USERNAME')
    api_key = os.getenv('JIRA_API')
    jira = Jira('https://wellapp.atlassian.net', user, api_key)
    fields = ['key']
    df = pd.DataFrame()

    count = 0
    jql = 'project = "%s" AND status = "Done" AND type = "Story" AND (labels NOT IN ("ad_hoc") OR labels = EMPTY) AND Sprint = "%s" and "Flagged[Checkboxes]" IS EMPTY' % (project, sprint_name)
    #jql = 'project = "%s" AND status = "Done" AND type = "Story" AND (labels NOT IN ("ad_hoc") OR labels = EMPTY) AND Sprint != EMPTY AND "Data Team Specialty[Checkboxes]" in ("Business Intelligence") AND createdDate > "2021/06/01"' % project

    results = jira.jql(jql, start=count, fields = fields)
    df_2 = pd.json_normalize(results["issues"])
    count += 50
    while count < jira_count(project):
        results = jira.jql(jql, start=count, fields=fields)
        if results['issues'] == []:
            break

        df_2 = df_2.append(pd.json_normalize(results["issues"]))
        count += 50
    df = pd.concat(
        [df, df_2],
        axis=0,
        join="outer",
        ignore_index=False,
        keys=None,
        levels=None,
        names=None,
        verify_integrity=False,
        copy=True,
    )

    from datetime import datetime

    time_diff = []

    for key in df['key']:
        try:
            issue = jira.issue(key, expand = "changelog")
            sprint_date_committed = None
            done_date = None
            _format = "%Y-%m-%dT%H:%M:%S.%f"
            #parsed_string = issue['fields']['created']
            #sprint_date_committed = datetime.strptime(parsed_string[:-5], _format) # when did the change happen?
            for history in issue['changelog']['histories']:   
                for item in history['items']:
                    if item['field'] == "Sprint":
                        parsed_string_2 = history['created']
                        sprint_date_committed = datetime.strptime(parsed_string_2[:-5], _format) # when did the change happen?
                    if item['field'] == "resolution":
                        parsed_string_3 = history['created']
                        done_date = datetime.strptime(parsed_string_3[:-5], _format) # when did the change happen?
            time_diff.append((done_date - sprint_date_committed).total_seconds() / 60 / 60 / 24)
        except:
            pass

    import numpy as np
    np_time = np.array(time_diff)[np.array(time_diff) >= 0]
    tickets = len(df)
    completion_rate = np.mean(np_time)
    percent_complete_under_sprint = round(sum(np_time <= 14) / len(np_time) * 100, 2)

    try:
        create_table('sprint_ticket_information')
    except:
        print("Sprint Ticket Table already created")

    new_sprint(sprint_name)

    sprint_ticket_stats(tickets, sprint_story_points(sprint_name), completion_rate, percent_complete_under_sprint, get_sprint_id(sprint_name))


    # %%
    print("Connecting to Jira - Round 2")
    count = 0
    project = "DATA"
    ## Requires create_sprint definition to run first (global variables start_date and end_date)
    jql = 'project = "%s" AND status = "Done" AND type = "Bug" AND createdDate >= "%s" AND createdDate <= "%s"' % (project, start_date, end_date)

    jira = Jira('https://wellapp.atlassian.net', user, api_key)
    fields = ['key']

    results = jira.jql(jql, start=count, fields = fields)
    df_bug = pd.json_normalize(results["issues"])
    count += 50

    while count < jira_count(project):
        results = jira.jql(jql, start=count, fields=fields)
        if results['issues'] == []:
            break

        df_bug = df_bug.append(pd.json_normalize(results["issues"]))
        count += 50

    from datetime import datetime

    bug_time_diff = []

    for key in df_bug['key']:
        try:
            issue = jira.issue(key, expand = "changelog")
            sprint_date_committed = None
            done_date = None
            _format = "%Y-%m-%dT%H:%M:%S.%f"
            parsed_string = issue['fields']['created']
            sprint_date_committed = datetime.strptime(parsed_string[:-5], _format) # when did the change happen?
            for history in issue['changelog']['histories']:   
                for item in history['items']:                
                    if item['field'] == "resolution":
                        parsed_string_2 = history['created']
                        done_date = datetime.strptime(parsed_string_2[:-5], _format) # when did the change happen?
            bug_time_diff.append((done_date - sprint_date_committed).total_seconds() / 60 / 60 / 24)  #converted to days
        except:
            print("Skipped %s" % key)
            pass

    bug_np_time = np.array(bug_time_diff)

    try:
        create_table('sprint_bug_information')
    except:
        print("Bug Table already created")

    sprint_bug_stats(len(df_bug['key']), np.mean(bug_np_time), np.std(bug_np_time), get_sprint_id(sprint_name))

# %%
def get_info(sprint_name):
    table= dynamodb.Table('sprint_information')
    query_response = table.query(
            IndexName='sprint_number-index',
            KeyConditionExpression=Key('sprint_number').eq(str(int(re.findall("\d+", sprint_name)[0])-1))
    )

    previous_sprint_id = query_response['Items'][0]['sprint_id']

    query_response = table.query(
            IndexName='sprint_number-index',
            KeyConditionExpression=Key('sprint_number').eq(str(int(re.findall("\d+", sprint_name)[0])))
    )

    current_sprint_id = query_response['Items'][0]['sprint_id']


    table= dynamodb.Table('sprint_ticket_information')
    sprint_results = table.query(
            KeyConditionExpression=Key('sprint_id').eq(current_sprint_id)
    )
    tickets = sprint_results['Items'][0]['ticket_count']
    completion_rate = sprint_results['Items'][0]['avg_time_tickets_completed']
    percent_complete_under_sprint = sprint_results['Items'][0]['percent_completed']


    table= dynamodb.Table('sprint_bug_information')
    bug_results = table.query(
            KeyConditionExpression=Key('sprint_id').eq(previous_sprint_id)
    )

    previous_bug_resolved_days = float(bug_results['Items'][0]['avg_time_resolved'])

    bug_results = table.query(
            KeyConditionExpression=Key('sprint_id').eq(current_sprint_id)
    )

    current_bug_resolved_days = float(bug_results['Items'][0]['avg_time_resolved'])


    # %%
    sprint_id = get_sprint_id(sprint_name)
    ## Calculations for sprint tickets

    table= dynamodb.Table('sprint_ticket_information')
    results = table.scan()

    avg_ticket_list = []
    ticket_count_list = []
    percent_completed_list = []
    sp_list = []

    for item in results["Items"]:
        if item['sprint_id'] == sprint_id:
            pass
        else:
            avg_ticket_list.append(float(item['avg_time_tickets_completed']))
            ticket_count_list.append(float(item['ticket_count']))
            percent_completed_list.append(float(item['percent_completed']))
            sp_list.append(float(item['story_points']))

    previous_ticket_avg = np.mean(avg_ticket_list)
    previous_ticket_count_avg = np.mean(ticket_count_list)
    previous_percent_completion = np.mean(percent_completed_list)
    previous_sprint_sp_avg = np.mean(sp_list)

    avg_ticket_list = []
    ticket_count_list = []
    percent_completed_list = []
    sp_list = []

    for item in results["Items"]:
        avg_ticket_list.append(float(item['avg_time_tickets_completed']))
        ticket_count_list.append(float(item['ticket_count']))
        percent_completed_list.append(float(item['percent_completed']))
        sp_list.append(float(item['story_points']))

    current_ticket_avg = np.mean(avg_ticket_list)
    current_ticket_count_avg = np.mean(ticket_count_list)
    current_percent_completion = np.mean(percent_completed_list)
    current_sprint_sp_avg = np.mean(sp_list)

    ## Comparisons Calculations

    if current_ticket_count_avg > previous_ticket_count_avg:
        ticket_comparison = "Increased"
    else:
        ticket_comparison = "Decreased"

    if current_ticket_avg > previous_ticket_avg:
        ticket_avg_comparison = "Increased"
    else:
        ticket_avg_comparison = "Decreased"

    if current_sprint_sp_avg > previous_sprint_sp_avg:
        sp_comparison = "Increased"
    else:
        sp_comparison = "Decreased"

    if current_percent_completion > previous_percent_completion:
        completion_comparison = "Increased"
    else:
        completion_comparison = "Decreased"

    if current_bug_resolved_days > previous_bug_resolved_days:
        bug_resolved_comparison = "Increased"
    else:
        bug_resolved_comparison = "Decreased"


    # %%
    ## Construct Message


    current_ticket_count = tickets
    current_ticket_count_avg = round(current_ticket_count_avg,2)
    ticket_comparison = ticket_comparison
    previous_ticket_count = round(previous_ticket_count_avg,2)
    current_sprint_story_points = sprint_story_points(sprint_name)
    sp_comparison = sp_comparison
    previous_sprint_sp_avg = round(previous_sprint_sp_avg,2)
    current_sprint_sp_avg = round(current_sprint_sp_avg,2)
    current_ticket_completion = round(float(completion_rate), 2)
    current_ticket_avg = round(current_ticket_avg,2)
    previous_ticket_avg = round(previous_ticket_avg,2)
    ticket_avg_comparison = ticket_avg_comparison
    current_bug_resolved_days = round(current_bug_resolved_days,2)
    bug_resolved_comparison = bug_resolved_comparison
    previous_bug_resolved_days = round(previous_bug_resolved_days,2)
    percent_complete_under_sprint = percent_complete_under_sprint
    current_sprint_avg_completion = round(current_percent_completion,2)
    previous_sprint_avg_completion = round(previous_percent_completion,2)
    completion_comparison = completion_comparison

    return [
        current_ticket_count,
        current_ticket_count_avg,
        ticket_comparison,
        previous_ticket_count,
        current_sprint_story_points,
        sp_comparison,
        previous_sprint_sp_avg,
        current_sprint_sp_avg,
        current_ticket_completion,
        current_ticket_avg,
        previous_ticket_avg,
        ticket_avg_comparison,
        current_bug_resolved_days,
        bug_resolved_comparison,
        previous_bug_resolved_days,
        percent_complete_under_sprint,
        current_sprint_avg_completion,
        previous_sprint_avg_completion,
        completion_comparison
    ]

def release_notes(sprint_name):
    project = "DATA"
    count = 0
    user = os.getenv('JIRA_USERNAME')
    api_key = os.getenv('JIRA_API')
    jira = Jira('https://wellapp.atlassian.net', user, api_key)
    jql = 'project = "%s" AND Sprint = "%s" AND type != "Sub-task"' % (project, sprint_name)
    fields = ['key','summary','fields.status.name', 'assignee.displayName', 'priority.name',"issuetype", \
            "customfield_10008", "customfield_10899", "customfield_10889", "customfield_10827", \
            "created", "labels"]

    results = jira.jql(jql, start=count)#, fields=fields)
    df = pd.json_normalize(results["issues"])
    count += 50
    while count < results['total']:
        results_new = jira.jql(jql, start=count, fields=fields)
        if results_new['issues'] == []:
            break
        df = df.append(pd.json_normalize(results_new["issues"]))
        count += 50
    
    df["fields.url"] = 'https://wellapp.atlassian.net/browse/'+df["key"]
    # Take main df with fields and reduce to only relevant columns and only if the ticket is completed
    df_reduced = df.filter(items=["key", "fields.issuetype.name", "fields.status.name", 'fields.customfield_10899', "fields.customfield_10008", \
                        "fields.customfield_10889", "fields.customfield_10827", "fields.labels", "fields.url"])[df['fields.status.name'] == 'Done']
    
    # Start cleaning data
    team_name_column = []
    new_story_points = []
    for index, row in df_reduced.iterrows():
        data_team_type = []
        if row['fields.customfield_10899'] != None:
            for val in row['fields.customfield_10899']:
                data_team_type.append(val['value'])
            
            team_name_column.append(data_team_type)
        else:
            team_name_column.append(None)
        
        if row['fields.issuetype.name'] == 'Task':
            new_story_points.append(0.5)
        else:
            new_story_points.append(row['fields.customfield_10008'])
        

    df_reduced['fields.customfield_10899'] = team_name_column
    df_reduced['fields.customfield_10008'] = new_story_points

    # Push to Table
    try:
        create_ticket_table('release_notes')
    except:
        print("release_notes table already made")

    table= dynamodb.Table('release_notes')
    with table.batch_writer() as batch:
        sprint_id = get_sprint_id(sprint_name)
        for index, row in df_reduced.iterrows():
            try:
                story_points = int(row['fields.customfield_10008'])
            except ValueError:
                story_points = None
            batch.put_item(
                Item={
                    'ticket_id': row['key'],
                    'sprint_id': sprint_id,
                    'ticket_type': row['fields.issuetype.name'],
                    'story_points': story_points,
                    'team': row['fields.customfield_10899'],
                    'label': row['fields.labels'],
                    'release_notes': row['fields.customfield_10889']
                }
            )
    print('Exported into DB')

    # Processing to send a Payload
    ticket_count = len(df_reduced['key'])
    story_points = int(df_reduced['fields.customfield_10008'].sum())
    release_notes = {
        'keys': df_reduced['key'][df_reduced['fields.customfield_10889'].notnull()].to_list(),
        'links': df_reduced['fields.url'][df_reduced['fields.customfield_10889'].notnull()].to_list(),
        'notes': df_reduced['fields.customfield_10889'][df_reduced['fields.customfield_10889'].notnull()].to_list()
    }
    return {
        "ticket_count": ticket_count,
        "story_points": story_points,
        "release_notes": release_notes
    }

def individual_performance(sprint_name):
    project = "DATA"
    count = 0
    user = os.getenv('JIRA_USERNAME')
    api_key = os.getenv('JIRA_API')
    jira = Jira('https://wellapp.atlassian.net', user, api_key)
    jql = 'project = "%s" AND Sprint = "%s" AND type != "Sub-task"' % (project, sprint_name)
    #fields = ['key','summary','assignee.displayName', 'fields.status.name', 'priority.name',"issuetype", \
#            "customfield_10008", "customfield_10899", "customfield_10889", "customfield_10827", \
#            "created", "labels"]

    results = jira.jql(jql, start=count)#, fields=fields)
    df = pd.json_normalize(results["issues"])
    count += 50
    while count < results['total']:
        results_new = jira.jql(jql, start=count)#, fields=fields)
        if results_new['issues'] == []:
            break
        df = df.append(pd.json_normalize(results_new["issues"]))
        df = df.reset_index(drop=True)
        count += 50

    df["fields.url"] = 'https://wellapp.atlassian.net/browse/'+df["key"]
    # Take main df with fields and reduce to only relevant columns and only if the ticket is completed
    df_reduced = df.filter(items=["key", "fields.issuetype.name", "fields.assignee.displayName","fields.status.name", 'fields.customfield_10899', "fields.customfield_10008", \
                        "fields.labels", "fields.url"])
    df_reduced.to_csv('test.csv')
    # Start cleaning data
    team_name_column = []
    new_story_points = []
    for index, row in df_reduced.iterrows():
        data_team_type = []
        if row['fields.customfield_10899'] != None:
            for val in row['fields.customfield_10899']:
                data_team_type.append(val['value'])
            
            team_name_column.append(data_team_type)
        else:
            team_name_column.append(None)
        
        if row['fields.issuetype.name'] == 'Task':
            new_story_points.append(0.5)
        else:
            new_story_points.append(row['fields.customfield_10008'])
        

    df_reduced['fields.customfield_10899'] = team_name_column
    df_reduced['fields.customfield_10008'] = new_story_points

    tickets_finished = \
        df_reduced[df_reduced['fields.status.name'] == 'Done']\
            .groupby('fields.assignee.displayName')\
            .agg({
                'fields.customfield_10008': 'sum',
                'key': 'count'
            })\
            .reset_index()\
            .rename(columns= {
                'fields.assignee.displayName': 'name',
                'fields.customfield_10008': 'finished_story_points',
                'key': 'finished_ticket_count'
            })

    tickets_unfinished = \
        df_reduced[df_reduced['fields.status.name'] != 'Done']\
            .groupby('fields.assignee.displayName')\
            .agg({
                'fields.customfield_10008': 'sum',
                'key': 'count'
            })\
            .reset_index()\
            .rename(columns= {
                'fields.assignee.displayName': 'name',
                'fields.customfield_10008': 'unfinished_story_points',
                'key': 'unfinished_ticket_count'
            })

    # Create main dataframe
    individual_performance = tickets_finished.join(tickets_unfinished.set_index('name'), on='name').fillna(0)

    # Adding calculation columns
    individual_performance['percent_finished_tickets'] = individual_performance['finished_ticket_count']/(individual_performance['finished_ticket_count']+individual_performance['unfinished_ticket_count'])
    individual_performance['percent_story_point_finished'] = individual_performance['finished_story_points']/(individual_performance['finished_story_points']+individual_performance['unfinished_story_points'])
    individual_performance['bug_time_completed'] = [np.nan] * len(individual_performance) # To potentially be added in the future
    individual_performance['percent_tickets_completed_in_one_sprint'] = [np.nan] * len(individual_performance) # To potentially be added in the future

    # Cleaning
    individual_performance.replace([np.nan], [None])

    # Push to Table
    try:
        create_id_table('individual_performance')
    except:
        print("individual_performance table already made")

    table= dynamodb.Table('individual_performance')
    with table.batch_writer() as batch:
        sprint_id = get_sprint_id(sprint_name)
        for index, row in individual_performance.iterrows():
            batch.put_item(
                Item={
                    'id': str(random.getrandbits(64)),
                    'sprint_id': int(sprint_id),
                    'name': row['name'],
                    'finished_story_points': Decimal(row['finished_story_points']),
                    'finished_ticket_count': Decimal(row['finished_ticket_count']),
                    'unfinished_story_points': Decimal(row['unfinished_story_points']),
                    'unfinished_ticket_count': Decimal(row['unfinished_ticket_count'])
                }
            )
    print('Exported into DB')

    # Processing for Slack Script

    return individual_performance.set_index('name').to_json()