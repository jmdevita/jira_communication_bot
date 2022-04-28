from sprint_optimization_script import main, get_info, release_notes, individual_performance, get_webhook_token, correct_key, get_project_key
from flask import Flask, request
import json
import requests, sys, os
from atlassian import Confluence
from time import sleep
from datetime import datetime
#from dotenv import load_dotenv
#load_dotenv()

app = Flask(__name__)

def processing_sprint_optimization(sprint_name, project, url):
    # Intake Jira Webhook
    main(sprint_name, project)
    sleep(2)
    payload = get_info(sprint_name, project)
    ### Send Slack Webhook
    slack_data = {
        "current_ticket_count": str(payload[0]),
        "current_ticket_count_avg": str(payload[1]),
        "ticket_comparison": str(payload[2]),
        "previous_ticket_count": str(payload[3]),
        "current_sprint_story_points": str(payload[4]),
        "sp_comparison": str(payload[5]),
        "previous_sprint_sp_avg": str(payload[6]),
        "current_sprint_sp_avg": str(payload[7]),
        "current_ticket_completion": str(payload[8]),
        "current_ticket_avg": str(payload[9]),
        "previous_ticket_avg": str(payload[10]),
        "ticket_avg_comparison": str(payload[11]),
        "current_bug_resolved_days": str(payload[12]),
        "bug_resolved_comparison": str(payload[13]),
        "previous_bug_resolved_days": str(payload[14]),
        "percent_complete_under_sprint": str(payload[15]),
        "current_sprint_avg_completion": str(payload[16]),
        "previous_sprint_avg_completion": str(payload[17]),
        "completion_comparison": str(payload[18])
    }
    byte_length = str(sys.getsizeof(slack_data))
    headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
    slack_response = requests.post(url, data=json.dumps(slack_data), headers=headers)
    if slack_response.status_code != 200:
        raise Exception(slack_response.status_code, slack_response.text)

def release_note_bot(sprint_name, project):
    # Intake Jira Webhook
    payload = release_notes(sprint_name, project)
    ### Send Slack Webhook
    url = os.getenv('RELEASE_NOTE_WEBHOOK')
    user = os.getenv('JIRA_USERNAME')
    api_key = os.getenv('JIRA_API')
    date = datetime.today().strftime('%m/%d/%Y')
    confluence = Confluence('https://wellapp.atlassian.net', user, api_key)
    # Loop for all messages in release notes
    full_release_slack = []
    full_release_confluence = []
    confluence_body = \
        """
        <h2>{sprint_name}</h2>
        <h3>{date}</h3>
        """.format(sprint_name=sprint_name, date=date)
    for count in range(0, len(payload['release_notes']['keys'])):
        single_release_slack = '<{url}|{key}> | {release_notes}'.format(url=payload['release_notes']['links'][count], key=payload['release_notes']['keys'][count], \
            release_notes=payload['release_notes']['notes'][count])
        full_release_slack.append(single_release_slack)
        
        single_release_confluence = '\n<a href="{url}">{key}</a> | {release_notes}'.format(url=payload['release_notes']['links'][count], key=payload['release_notes']['keys'][count], \
            release_notes=payload['release_notes']['notes'][count])
        full_release_confluence.append(single_release_confluence)

    slack_data = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "The Data Team has completed their sprint!"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "Ticket Count: {tc}\nStory Points: {sp}\n\n -- <https://wellapp.atlassian.net/wiki/spaces/EN/pages/2381283406/Releases|Release Notes> --\n{rn}"\
                        .format(
                            tc=payload['ticket_count'],
                            sp=payload['story_points'],
                            rn='\n\n'.join(full_release_slack)
                        )
                }
            }
        ]
    }
    byte_length = str(sys.getsizeof(slack_data))
    headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
    slack_response = requests.post(url, data=json.dumps(slack_data), headers=headers)
    if slack_response.status_code != 200:
        raise Exception(slack_response.status_code, slack_response.text)
    # Send Confluence
    confluence.append_page(title='Releases', page_id='2381283406', append_body=confluence_body+'\n'.join(full_release_confluence), representation='storage', minor_edit=True)

def individual_performance_update(sprint_name, project):
    # Intake Jira Webhook
    payload = individual_performance(sprint_name, project)
    ### Send Slack Webhook
    #url = os.getenv('Individual_Performance_WEBHOOK')


@app.route('/response', methods=['GET','POST'])
def response():
    # Definition for Process

    print('Recieved Message')
    try:
        post_data = request.get_json()
    except TypeError:
        return {"message": "No Data Passed Through"}, 401
    try:
        key_validation = correct_key(post_data['token'])
    except KeyError:
        key_validation = "INVALID"
    if key_validation:
        print("token valid")
        #Grabbing webhook based off of key
        # Since we grab board ID, we need to convert to project name
        team_id = get_project_key(post_data['team'])
        processing_sprint_optimization(post_data['data'], team_id, get_webhook_token(post_data['token']))
        if team_id == 'DATA':
            release_note_bot(post_data['data'], team_id)
            individual_performance(post_data['data'], team_id)

        return {"message": "Accepted"}, 202
    elif key_validation == "INVALID":
        return {"message": "No Token"}, 401
    else:
        return {"message": "Not Authorized"}, 401

if __name__ == '__main__':
    app.run(port=5050)