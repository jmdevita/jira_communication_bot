from sprint_optimization_script import main, get_info, release_notes
from flask import Flask, request
import json
import requests, sys, os
from time import sleep

app = Flask(__name__)

WEBHOOK_VERIFY_TOKEN = os.getenv('WEBHOOK_VERIFY_TOKEN')

def processing_sprint_optimization(sprint_name):
    # Intake Jira Webhook
    main(sprint_name)
    sleep(2)
    payload = get_info(sprint_name)
    ### Send Slack Webhook
    url = os.get_env('SPRINT_OPTIMIZATION_BOT_WEBHOOK')
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

def release_note_bot(sprint_name):
    # Intake Jira Webhook
    payload = release_notes(sprint_name)
    ### Send Slack Webhook
    url = os.get_env('RELEASE_NOTE_WEBHOOK')

    slack_data = payload
    byte_length = str(sys.getsizeof(slack_data))
    headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
    slack_response = requests.post(url, data=json.dumps(slack_data), headers=headers)
    if slack_response.status_code != 200:
        raise Exception(slack_response.status_code, slack_response.text)


@app.route('/response', methods=['GET','POST'])
def response():
    # Definition for Process

    print('Recieved Message')
    post_data = request.get_json()
    try:
        verify_token = post_data['token']
    except KeyError:
        verify_token = "INVALID"
    if verify_token == WEBHOOK_VERIFY_TOKEN:
        #thread = Process(target=processing, args=(post_data['data'],))
        #thread.start() # Doesn't work with Lambda (as it shuts down as soon as it returns a message)
        processing_sprint_optimization(post_data['data'])
        release_note_bot(post_data['data'])

        return {"message": "Accepted"}, 202
    elif verify_token == "INVALID":
        return {"message": "No Token"}, 401
    else:
        return {"message": "Not Authorized"}, 401

if __name__ == '__main__':
    app.run()