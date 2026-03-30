"""This script generates a summary of statistics for a sprint to better understand worker completion based on story point allocation.
The goal is to help have fruitful discussions during sprint retrospectives on how we can improve completion of points/better estiamtion going forward.
"""

import common_functions as cf
import pandas as pd
import os
import ast

def sprint_dates(sprint_name, project):
    sprint_info = project.get_sprint_details()
    sprint = sprint_info[sprint_info['name'] == sprint_name].iloc[0]
    start_date = sprint['startDate']
    end_date = sprint['endDate']
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    dates = [date.date() for date in date_range]
    return dates

# Instead of the current cartesian approach, use this method:
def get_status_for_each_day(issue_ids, audit_issues, dates, sprint_start_date):
    """
    Function that defines a given status for each day of the sprint for all stories. Leverages Jira history to define.
    
    :param issue_ids: All issue IDs in the sprint
    :param audit_issues: All issues in current sprint with historical changes to status
    :param dates: The sprint days to loop through to define status on each date
    :param sprint_start_date: The start date of the sprint to determine baseline status
    """
    result_data = []
    current_status_df = project.get_all_issues(issue_ids=issue_ids, cust_fields=None, maxResults=False)
    current_status_lookup = dict(zip(current_status_df['Issue ID'], current_status_df['Status']))
    
    # Create a lookup for the status at sprint start for each issue
    sprint_start_status_lookup = {}
    for issue_id in issue_ids:
        issue_changes = audit_issues[audit_issues['Issue ID'] == issue_id]
        
        if len(issue_changes) > 0:
            # Find the most recent status change ON OR BEFORE the sprint start date
            changes_before_sprint = issue_changes[issue_changes['ChangeDate'] <= sprint_start_date]
            
            if len(changes_before_sprint) > 0:
                # Use the most recent status change before/on sprint start
                latest_change_before_sprint = changes_before_sprint.sort_values('ChangeDate').iloc[-1]
                sprint_start_status_lookup[issue_id] = latest_change_before_sprint['ToStatus']
            else:
                # No changes before sprint start - use the "from" status of the first change
                earliest_change = issue_changes.sort_values('ChangeDate').iloc[0]
                sprint_start_status_lookup[issue_id] = earliest_change['FromStatus']
        else:
            # No status changes at all - use current status as best guess
            sprint_start_status_lookup[issue_id] = current_status_lookup.get(issue_id, 'Unknown')
    
    print('-'*60)
    print(f'Checking Status on Each Day (Sprint starts: {sprint_start_date})')
    print('-'*60)
    
    for date in dates:
        print(f'Processing date: {date}')
        for issue_id in issue_ids:
            issue_changes = audit_issues[audit_issues['Issue ID'] == issue_id]
            # Find the most recent change on or before this date
            relevant_changes = issue_changes[issue_changes['ChangeDate'] <= date]
            
            if len(relevant_changes) > 0:
                # Get the most recent status change on or before this date
                latest_change = relevant_changes.sort_values('ChangeDate').iloc[-1]
                status = latest_change['ToStatus']
            else:
                # No changes before this date - use the status at sprint start
                status = sprint_start_status_lookup.get(issue_id, 'Unknown')

            result_data.append({
                'Sprint Day': date,
                'Issue ID': issue_id,
                'Status': status
            })
    
    return pd.DataFrame(result_data)

def prep_issues_for_velocity(status_day_report, project):
    print('Status day report: ', status_day_report)
    print('DEBUG: DataFrame columns:', list(status_day_report.columns))
    print('DEBUG: DataFrame shape:', status_day_report.shape)
    print('DEBUG: First few rows:')
    print(status_day_report.head())
    
    # Check if Story Points column exists
    if 'Story Points' not in status_day_report.columns:
        print("ERROR: 'Story Points' column missing from DataFrame!")
        print("Available columns:", list(status_day_report.columns))
        # Add a default Story Points column with 0 values
        status_day_report['Story Points'] = 0
        print("WARNING: Added default 'Story Points' column with 0 values")
    
    status_day_report[['In Review', 'Ready for Development', 'Ready for Prod', 'Done', 'In Progress']] = 0.0
    for i in range(len(status_day_report)):
        story_points = status_day_report['Story Points'].iloc[i] if pd.notna(status_day_report['Story Points'].iloc[i]) else 0
        if status_day_report['Status'].iloc[i] == 'In Review':
            status_day_report.loc[i, 'In Review'] = story_points 
        elif status_day_report['Status'].iloc[i] == 'Ready for Development':
            status_day_report.loc[i, 'Ready for Development'] = story_points 
        elif status_day_report['Status'].iloc[i] == 'In Progress':
            status_day_report.loc[i, 'In Progress'] = story_points 
        elif status_day_report['Status'].iloc[i] == 'Ready for Prod':
            status_day_report.loc[i, 'Ready for Prod'] = story_points 
        elif status_day_report['Status'].iloc[i] == 'Done':
            status_day_report.loc[i, 'Done'] = story_points 

    status_day_report = status_day_report[['Sprint Day', 'Issue ID', 'Story Points', 'In Progress', 'In Review', 'Ready for Development', 'Ready for Prod', 'Done', 'Sprint', 'Jira Board', 'Sprint Start', 'Sprint End']]
    issue_lookup_df = status_day_report['Issue ID'].unique().tolist()
    issue_epic_details = project.get_parent_details(issue_list = issue_lookup_df, maxResults=False)
    issue_epic_details = issue_epic_details[['Issue ID', 'Issue Assignee', 'Parent ID', 'Parent Summary']]
    epic_lookup_df = issue_epic_details['Parent ID']
    issue_initiative_details = project.get_parent_details(issue_list = epic_lookup_df, maxResults=False)

    epic_join_output = pd.merge(status_day_report, issue_epic_details, how='left', left_on='Issue ID', right_on='Issue ID', suffixes=('_Child', '_Parent'))
    # epic_join_output = epic_join_output[['Sprint Day', 'Issue ID', 'Story Points', 'In Progress', 'In Review', 'Ready for Prod', 'Done', 'Parent ID' ]]
    initiative_join_output = pd.merge(epic_join_output, issue_initiative_details, how='left', left_on='Parent ID', right_on='Issue ID', suffixes=('_Child', '_Parent'))
    initiative_join_output = initiative_join_output[['Sprint Day', 'Issue ID_Child', 'Story Points', 'In Progress', 'In Review', 'Ready for Prod', 'Done', 'Issue Assignee_Child', 'Parent Summary_Parent', 'Sprint', 'Sprint Start', 'Sprint End', 'Jira Board' ]]
    initiative_join_output = initiative_join_output.rename(columns={'Issue ID_Child': 'Issue ID', 'Issue Assignee_Child': 'Assignee',
                                                                    'Parent Summary_Parent': 'Initiative Name'})
    return initiative_join_output

if __name__ == '__main__':
    tableau_user = os.environ['TABLEAU_USER']
    tableau_password = os.environ['TABLEAU_PASSWORD']
    tableau_site = os.environ['TABLEAU_SITE']
    tableau_server = os.environ['TABLEAU_SERVER']
    jira_url = os.environ['JIRA_URL']
    jira_user = os.environ['JIRA_USER']
    api_token = os.environ['JIRA_TOKEN']
    custom_fields = ast.literal_eval(os.environ['CUSTOM_FIELDS'])

    # Initiatlize projects and create empty dataframe that will update with contents from loop below
    isg_project = cf.JiraProject('Project 1', '1111', '11111', jira_url, jira_user, api_token, custom_fields)
    asg_project = cf.JiraProject('Project 2', '1111', '11111', jira_url, jira_user, api_token, custom_fields)

    project_list = [isg_project, asg_project]
    tableau_output_columns = ['Assignee', 'Initiative Name', 'Issue ID', 'Jira Board', 'Sprint', 'Sprint Day',
                              'Sprint Start', 'Sprint End', 'Ready for Development', 'Done', 'In Progress', 'In Review', 'Ready for Prod',
                              'Story Points']
    tableau_output_df = pd.DataFrame(columns=tableau_output_columns)

    # Loop through all JIRA boards of interest
    for project in project_list:
        # Get Sprints and create a list of current sprint + 4 historical sprints
        sprints = project.get_sprint_details()
        sprints.index.name = 'id'
        sprint = sprints.reset_index(inplace=True)
        sprint_name = sprints[sprints['state'] == 'active']['name'].iloc[0]
        active_sprint = sprints[sprints['state'] == 'active'].index.astype(int)
        hist_sprint = sprints[sprints['state'] == 'active'].index - 4
        hist_sprint = int(hist_sprint[0])
        active_sprint = active_sprint[0]
        sprint_list = sprints[hist_sprint: active_sprint+1]['name'].unique().tolist()
        # Looping through each sprint to get unique IDs and storing in a set to avoid duplicates across sprints
        all_sprint_data = []
        # Loop through each sprint to get a status of each issue by the sprint day
        for sprint_name in sprint_list:
            sprint_issues_list = set() 
            # Fetch bugs and stories in the sprint
            sprint_issues = project.get_issues_in_sprint(sprint_name, cust_fields=None,type=['Bug', 'Story'], maxResults=False)
            sprint_issues_list.update(list(sprint_issues['Issue ID']))
            sprint_issues_list = list(sprint_issues_list)

            # Retrieve audit history of status changes for each issue
            issue_list = project.get_audit_log(sprint_issues_list, maxResults=False)
            issue_list = issue_list[issue_list['Field'] == 'status']
            issue_ids_list = issue_list['Issue ID'].unique().tolist()
            # Define the days in the sprint and retrieve status for each day 
            sprint_date_range = sprint_dates(sprint_name, project)
            sprint_start_date = min(sprint_date_range)
            sprint_status_df = get_status_for_each_day(sprint_issues_list, issue_list, sprint_date_range, sprint_start_date)
            sprint_status_df['Sprint'] = sprint_name
            sprint_status_df['Jira Board'] = project.project
            sprint_status_df['Sprint Start'] = min(sprint_date_range)
            sprint_status_df['Sprint End'] = max(sprint_date_range) 
            # Merge into 1 dataframe
            all_sprint_data.append(sprint_status_df)

        # Join the resulting outputs together to create an output that you would like to publish to tableau
        print(f'all sprint data: {all_sprint_data}')
        output = pd.concat(all_sprint_data, ignore_index=True)
        output_list = output['Issue ID'].unique().tolist()
        addtl_info = project.get_all_issues(issue_ids=output_list, cust_fields=custom_fields, maxResults=False)
        print(f'ready for parents: {addtl_info}')
        initiative_info = project.get_parent_details(addtl_info['Parent ID'], maxResults=False)
        initiative_info.rename(columns={'Parent Summary': 'Initiative Name', 'Issue ID': 'Epic ID', 'Issue Summary': 'Epic Name', 
                                            'Issue Assignee': 'Epic Assignee', 'Issue Type': 'Epic Type', 'Parent ID': 'Intiative ID', 
                                            'Parent Type': 'Initiative Type', 'Parent Status': 'Initiative Status'}, inplace=True)
        addtl_info_combined_items = pd.merge(addtl_info, initiative_info, left_on='Parent ID', right_on='Epic ID', suffixes=('_Child', '_Parent'))
        
        final_output = pd.merge(output, addtl_info_combined_items, how='left', left_on='Issue ID', right_on='Issue ID', suffixes=('_Child', '_Parent'))
        final_output = final_output.drop(columns=(['Status_Parent', 'Jira Board_Parent']))
        final_output = final_output.rename(columns={'Status_Child': 'Status', 'Jira Board_Child': 'Jira Board'})
        prepped_items = prep_issues_for_velocity(final_output, project)
        tableau_output_df = pd.concat([tableau_output_df, prepped_items], ignore_index=True)
    # Publish to Tableau as a Sprint Report extract
    new_extract = cf.TableauExtract('Sprint Report', tableau_server, tableau_site, tableau_user, tableau_password)
    tableau_output_df = new_extract.clean_dataframe_for_tableau(tableau_output_df)
    new_extract.publish_extract(tableau_output_df)
