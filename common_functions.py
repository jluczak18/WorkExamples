from jira import JIRA
import os
import pandas as pd 
import requests
from requests.auth import HTTPBasicAuth
import math
from bs4 import BeautifulSoup as Soup
from email.mime.text import MIMEText
import smtplib
from datetime import date , timedelta
import time
import json
import pantab as pt
from tableauhyperapi import HyperProcess, Connection, TableDefinition, SqlType, Telemetry, Inserter, CreateMode, TableName
import tableauserverclient as TSC
from atlassian import Confluence


class JiraProject:
    """
    The JiraProject class gives users the ability to access various common functions with interacting with the JIRA REST API such as retrieving issues, sprints, releases, and more.
    
    :param: project: This is the name of the JIRA project found on JIRA's site
    :param: board_id: This is a unique identifier for a JIRA project that can only be found through JIRA api
    :param: project_id: 
    """
    def __init__(self, project, board_id, project_id, jira_url, jira_user, api_token, custom_fields):
        self.project = project
        self.board_id = board_id
        self.project_id = project_id
        self.api_token = api_token
        self.jira_url = jira_url 
        self.jira_user = jira_user
        self.jira = JIRA(server=jira_url, basic_auth=(jira_user, api_token))
        self.custom_fields = custom_fields
        
        # Debug information for troubleshooting GitHub Actions
        print(f"DEBUG: JiraProject {project} - custom_fields type: {type(custom_fields)}")
        print(f"DEBUG: JiraProject {project} - custom_fields value: {custom_fields}")
        if isinstance(custom_fields, dict):
            print(f"DEBUG: JiraProject {project} - keys in custom_fields: {list(custom_fields.keys())}")
        else:
            print(f"DEBUG: JiraProject {project} - WARNING: custom_fields is not a dict!")

    def create_issue(self, summary, description, aha_workspace, ):
        api_endpoint = '/rest/api/3/issue'
        auth = HTTPBasicAuth(self.jira_user, self.api_token)
        headers = {
                    "Accept": "application/json",
                    "Content-Type": "application/json"
                }
        issue_data = json.dumps({
                "fields": {
                        "project": {
                        "id": self.project_id
                        },
                        "summary": summary,
                        "description": {
                        "type": "doc",
                        "version": 1,
                        "content": [
                                        {
                                        "type": "paragraph",
                                        "content": [
                                            {
                                            "type": "text",
                                            "text": description
                                            }
                                        ]
                                        }
                                    ]
                                        },
                                        "issuetype": {
                                        "name": "Epic"
                                        },
                            }
                            })
        response = requests.post(f'{self.jira_url}{api_endpoint}', data=issue_data, headers=headers, auth=auth)
        print(response.json())
                    
    def get_all_issues(self, issue_ids=None, cust_fields=None, type=['Bug', 'Story'], maxResults=True):
        # if not issue_ids:
        #     return pd.DataFrame()
        issue_dict = {}

        if issue_ids != [None]:
            # Fix maxResults parameter
            max_results_value = 1000 if maxResults is True else (maxResults if maxResults else 50)
        
            batch_size = 50
            
            for i in range(0, len(issue_ids), batch_size):
                batch = issue_ids[i:i + batch_size]
                issue_keys = "', '".join(batch)
                
                # Build JQL without the problematic type filter
                if len(type) == 1:
                    jql = f"project in ('{self.project}') and issuekey in ('{issue_keys}') and issuetype = '{type[0]}'" 
                elif len(type)> 1:
                    issue_type = ", ".join(type)
                    jql = f"project in ('{self.project}') and issuekey in ('{issue_keys}') and issuetype in ({issue_type})" 
                else:
                    jql = f"project in ('{self.project}') and issuekey in ('{issue_keys}')" 
                
                try:
                    # Use basic search_issues with proper parameters
                    issues = self.jira.search_issues(jql, maxResults=max_results_value)
                    # Process the issues
                    for issue in issues:
                        try:
                            issue_dict[issue.key] = {
                                'Summary': issue.fields.summary,
                                'Description': issue.fields.description,
                                'Jira Board': issue.fields.project.name,
                                'Status': issue.fields.status.name,
                                'Due Date': issue.fields.duedate if issue.fields.duedate else None,
                                'Issue Type': issue.fields.issuetype.name,
                                'Issue ID': issue.key,
                                'Parent': None if issue.fields.issuetype.name == 'Initiative' else issue.fields.parent.fields.summary if  issue.fields.parent else None,
                                'Parent ID': None if issue.fields.issuetype.name == 'Initiative' else issue.fields.parent.key if issue.fields.parent else None,
                                'Assignee': issue.fields.assignee.displayName if issue.fields.assignee else None,
                                'Fix Versions': ', '.join([fv.name for fv in issue.fields.fixVersions]) if issue.fields.fixVersions else None, 
                                'Fix Versions Count': len(issue.fields.fixVersions) if issue.fields.fixVersions else 0,
                                'Reporter': issue.fields.reporter.displayName if issue.fields.reporter else None,
                                'Created Date': issue.fields.created,
                                'Updated Date': issue.fields.updated,
                            }
                            
                            # Add custom fields if specified
                            if cust_fields is not None:
                                for x in cust_fields[self.project]:
                                    field_name = x
                                    field_value = cust_fields[self.project][x]
                                    field_data = issue.fields.__getattribute__(field_value)
                                    if hasattr(field_data, 'value'):
                                        issue_dict[issue.key][field_name] = field_data.value
                                    else:
                                        issue_dict[issue.key][field_name] = field_data
                                    
                        except AttributeError as e:
                            print(f"Warning: Could not process issue {issue.key}: {e}")
                            continue
                    
                    print(f"Retrieved {len(issues)} issues from batch {i//batch_size + 1}")
                    time.sleep(0.2)
                    
                except Exception as e:
                    print(f"Error fetching batch {i//batch_size + 1}: {e}")
                    continue
            
        else:
            # Handle when no specific issue_ids provided - fetch all issues for project
            # Fix maxResults parameter  
            max_results_value = 1000 if maxResults is True else (maxResults if maxResults else 50)
            
            if len(type) == 1:
                jql = f"project in ('{self.project}') and issuetype = '{type[0]}'" 
            elif len(type) > 1:
                issue_type = ", ".join([f"'{t}'" for t in type])
                jql = f"project in ('{self.project}') and issuetype in ({issue_type})" 
            else:
                # If no type specified, get all issues
                jql = f"project in ('{self.project}')" 

            print(f"Fetching all issues for project '{self.project}' with JQL: {jql}")
            issues = self.jira.search_issues(jql, maxResults=max_results_value)
            
            for issue in issues:
                try:
                    issue_dict[issue.key] = {
                        'Summary': issue.fields.summary,
                        'Description': issue.fields.description,
                        'Jira Board': issue.fields.project.name,
                        'Status': issue.fields.status.name,
                        'Due Date': issue.fields.duedate if issue.fields.duedate else None,
                        'Issue Type': issue.fields.issuetype.name,
                        'Issue ID': issue.key,
                        'Parent': None if issue.fields.issuetype.name == 'Initiative' else issue.fields.parent.fields.summary if  issue.fields.parent else None,
                        'Parent ID': None if issue.fields.issuetype.name == 'Initiative' else issue.fields.parent.key if issue.fields.parent else None,
                        'Assignee': issue.fields.assignee.displayName if issue.fields.assignee else None,
                        'Fix Versions': ', '.join([fv.name for fv in issue.fields.fixVersions]) if issue.fields.fixVersions else None, 
                        'Fix Versions Count': len(issue.fields.fixVersions) if issue.fields.fixVersions else 0,
                        'Reporter': issue.fields.reporter.displayName if issue.fields.reporter else None,
                        'Created Date': issue.fields.created,
                        'Updated Date': issue.fields.updated,
                    }
                    # Add custom fields if specified
                    if cust_fields is not None:
                        for x in cust_fields[self.project]:
                            field_name = x
                            field_value = cust_fields[self.project][x]
                            field_data = issue.fields.__getattribute__(field_value)
                            if hasattr(field_data, 'value'):
                                issue_dict[issue.key][field_name] = field_data.value
                            else:
                                issue_dict[issue.key][field_name] = field_data
                            
                except AttributeError as e:
                    print(f"Warning: Could not process issue {issue.key}: {e}")
                    continue
    
        # Print final results
        if issue_ids is not None:
            print(f"Successfully processed {len(issue_dict)} issues from {len(issue_ids)} requested issue IDs")
        else:
            print(f"Successfully processed {len(issue_dict)} issues from project '{self.project}'")

        
        return pd.DataFrame.from_dict(issue_dict, orient='index').reset_index(drop=True)
    
    def get_sprint_details(self):
        """
        Function that will return all sprint details for a given board id
        
        :return: Dataframe of sprint details"""
        auth = HTTPBasicAuth(self.jira_user, self.api_token)
        headers = {
        "Accept": "application/json"
    }
        url = self.jira_url + '/rest/agile/1.0/board/{}/sprint'.format(self.board_id)
        response = requests.get(url, headers=headers, auth=auth)
        response = response.json()
        total_sprints = response['total']
        iterations = math.ceil(total_sprints / 50)
        z=0
        i=0
        sprint_dict = {}
        while z < iterations:
            startVal = i
            newUrl = self.jira_url + 'rest/agile/1.0/board/{}/sprint?startAt={t}'.format(self.board_id, t=startVal)
            x = requests.get(newUrl, headers=headers, auth=auth)
            x = x.json()
            sprint_cnt = len(x['values'])
            for q in range(sprint_cnt):
                sprint_dict[x['values'][q]['id']] = {
                    'state': x['values'][q]['state'], 'name': x['values'][q]['name'], 
                    'startDate': x['values'][q]['startDate'], 'endDate': x['values'][q]['endDate']
                }
            i+=50
            z+=1
        df= pd.DataFrame.from_dict(sprint_dict, orient='index')
        df['endDate'] = pd.to_datetime(df['endDate']).dt.tz_localize(None)
        df['startDate'] = pd.to_datetime(df['startDate']).dt.tz_localize(None)
        return df
    
    def get_parent_details(self, issue_list = [], maxResults=False):
        """
        Function that will return a dataframe with parent details for the provided issues. Works for any child parent relationship setup in your JIRA instance. 
        
        :param issue_list: List of issue IDs you would like to get parent details for.
        """

        parent_dict = {}
        
        # Batch issue IDs to avoid CloudFront 413 errors
        batch_size = 20  # Process 20 issues at a time
        all_issues = []
        print('-'*60)
        print(f"Fetching Parent Details")
        print('-'*60)    
        print(f"Processing {len(issue_list)} issues in batches of {batch_size}")
        
        for i in range(0, len(issue_list), batch_size):
            batch_issues = issue_list[i:i + batch_size]
            print(f"Batch {i//batch_size + 1}: Processing issues {i+1}-{min(i+batch_size, len(issue_list))}")
            if len(batch_issues) == 1:
                batch_issues_result = self.jira.search_issues(f"project in ('{self.project}') and issuekey = '{batch_issues[0]}' ", maxResults = maxResults)
            else:
                batch_issues_result = self.jira.search_issues(f"project in ('{self.project}') and issuekey in {tuple(batch_issues)} ", maxResults = maxResults)
            all_issues.extend(batch_issues_result)
            
            # Add small delay to prevent rate limiting
            time.sleep(0.5)
        
        for i in range(len(all_issues)):
            parent_dict[all_issues[i].key] = {'Issue Summary': all_issues[i].fields.summary,
                                          'Issue Assignee': all_issues[i].fields.assignee.displayName if all_issues[i].fields.assignee else None,
                                          'Issue Type': all_issues[i].fields.issuetype.name,
                                          'Parent ID': all_issues[i].fields.parent.key if hasattr(all_issues[i].fields, 'parent') else None,
                                          'Parent Summary': all_issues[i].fields.parent.fields.summary if hasattr(all_issues[i].fields, 'parent') else None,
                                          'Parent Type': all_issues[i].fields.parent.fields.issuetype.name if hasattr(all_issues[i].fields, 'parent') else None,
                                          'Parent Status': all_issues[i].fields.parent.fields.status.name if hasattr(all_issues[i].fields, 'parent') else None
            }
        parent_df = pd.DataFrame.from_dict(parent_dict, orient='index')
        parent_df = parent_df.reset_index()   
        parent_df = parent_df.rename(columns={'index': 'Issue ID'}) 
        return parent_df   

    def get_board_statuses(self):
        # Fetch Workflow Scheme associated with project
        url = self.jira_url + f'/rest/api/3/workflowscheme/project?projectId={self.project_id}'

        # url = self.jira_url + f'rest/agile/1.0/board/{self.board_id}/configuration'
        auth = HTTPBasicAuth(self.jira_user, self.api_token)
        headers = {
            "Accept": "application/json"
        }
        x = requests.get(url, headers=headers, auth=auth)
        output = x.json()
        scheme = output['values'][0]['workflowScheme']['id']

        # Fetch ID for Story Issue Type
        print(f'project id: {self.project_id}')
        url = self.jira_url + f'/rest/api/3/issuetypescreenscheme/{self.project_id}'
        x = requests.get(url, headers=headers, auth=auth)
        print(x.json())
        # print(output)
        # status_output = output['columnConfig']['columns']
        # status_list = []
        # for item in range(len(status_output)):
        #     status_list.append(status_output[item]['name'])

        # print(status_list)            
        
    def get_audit_log(self, issue_list = None, maxResults=False):
        """
        Function that will return a dataframe with a history of changes that occurred during the lifecycle of the provided issues.
        
        :param issue_list: List of issue IDs you would like to get audit log details for. Must contain more than 1.

        :return: a Dataframe with Issue ID, Change Type, From, To, Changed By, Changed On
        """
        auth = HTTPBasicAuth(self.jira_user, self.api_token)
        headers = {
            "Accept": "application/json"
        }
        audit_dict = {}
        df = pd.DataFrame()
        print('-'*60)
        print(f"Getting Audit Log")
        print('-'*60)     
        print(f"Processing audit log for {len(issue_list)} issues...")
        
        for t in range(len(issue_list)):
            if t % 10 == 0:  # Progress update every 10 issues
                print(f"Processing issue {t+1}/{len(issue_list)}: {issue_list[t]}")
            url = f'{self.jira_url}/rest/api/3/issue/{issue_list[t]}/changelog'
            
            # Add retry logic with exponential backoff
            max_retries = 3
            retry_delay = 1
            
            for retry_attempt in range(max_retries):
                try:
                    x = requests.get(url, headers=headers, auth=auth, timeout=30)
                    break  # Success, exit retry loop
                except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                    if retry_attempt < max_retries - 1:
                        print(f"Connection error for {issue_list[t]}, retrying in {retry_delay} seconds... (attempt {retry_attempt + 1}/{max_retries})")
                        import time
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                    else:
                        print(f"Failed to fetch {issue_list[t]} after {max_retries} retries: {e}")
                        audit_dict[issue_list[t]] = {}
                        continue
            else:
                # This executes if the for loop wasn't broken (all retries failed)
                continue
            
            # Check for rate limiting or errors
            if x.status_code != 200:
                print(f"Error fetching {issue_list[t]}: HTTP {x.status_code}")
                # Add placeholder for failed requests
                audit_dict[issue_list[t]] = {}
                continue
                
            x = x.json()

            # Add delay to prevent rate limiting
            import time
            time.sleep(1.0)  # Increased to 1 second delay between requests

            inner_dict = {}
            q=0
            for i in range(len(x['values'])):
                for j in range(len(x['values'][i])):
                    try:
                        created_date = x['values'][i]['created']
                        val = x['values'][i]['items'][j]
                        new_val = val['fromString']
                        final_val = val['toString']
                        field = val['field']
                        inner_dict[q] = {'Type': 'Field Change', 'Field': field,'FromStatus': new_val, 'ToStatus': final_val, 'ChangeDate': created_date}
                        q = q +1
                    except:
                        continue
            audit_dict[issue_list[t]] = inner_dict
            d = []
            
            # Handle issues with no history
            if len(audit_dict[issue_list[t]]) == 0:
                # Add a placeholder entry for issues with no change history
                # d.append({'Issue ID': issue_list[t], 'ChangeID': 0, 'Type': 'No Changes', 'Field': 'N/A', 'FromStatus': 'N/A', 'ToStatus': 'N/A', 'ChangeDate': None})
                continue
            else:
                for z in range(len(audit_dict[issue_list[t]])):
                    d.append({'Issue ID': issue_list[t], 'ChangeID': z, 'Type': audit_dict[issue_list[t]][z]['Type'], 'Field': audit_dict[issue_list[t]][z]['Field'], 'FromStatus': audit_dict[issue_list[t]][z]['FromStatus'], 'ToStatus': audit_dict[issue_list[t]][z]['ToStatus'], 'ChangeDate': audit_dict[issue_list[t]][z]['ChangeDate']})
            
            if len(df) == 0:
                df= pd.DataFrame.from_dict(d)
            else:
                df = pd.concat([df, pd.DataFrame.from_dict(d)], ignore_index=True)
        df = df[df['ChangeDate'].notna()].copy()
        df['ChangeDate'] = pd.to_datetime(df['ChangeDate'], errors='coerce', utc=True).dt.date
        return df
    
    def return_releases(self, start_date, end_date):
        """
        Function that will return a list of release names and dates 
        
        :param start_date: Start date to filter releases (format: 'YYYY-MM-DD')
        :param end_date: End date to filter releases (format: 'YYYY-MM-DD')
        
        :return: Dataframe with Release Name, date of Release, and whether it was deployed or not (True/False) and whether it was archived (True/False)"""
        #Authenticate and login
        auth = HTTPBasicAuth(self.jira_user, self.api_token)
        headers = {
        "Accept": "application/json"
        }
        release_url = f'{self.jira_url}/rest/api/3/project/{self.project_id}/versions'
        val = requests.get(release_url, headers=headers, auth=auth)

        #Store the release name and date as a dataframe
        output = val.json()
        release_df = pd.DataFrame(columns=['ID', 'ReleaseName', 'ReleaseDate', 'Released', 'Archived', 'Description'], index=range(len(output)))

        for i in range(len(output)):
            try:
                release_df.loc[i, 'ID'] = output[i]['id']
                release_df.loc[i, 'ReleaseName'] = output[i]['name']
                release_df.loc[i, 'ReleaseDate'] = output[i]['releaseDate']
                release_df.loc[i, 'Released'] = output[i]['released']
                release_df.loc[i, 'Archived'] = output[i]['archived']
                release_df.loc[i, 'Description'] = output[i]['description'].split('T')[0]
            except:
                continue
        
        #filter results between time intervals for the current and future timeframes
        release_df['ReleaseDate'] = pd.to_datetime(release_df['ReleaseDate'], format='%Y-%m-%d')
        start_date = pd.to_datetime(str(start_date), format='%Y-%m-%d')
        end_date = pd.to_datetime(str(end_date), format='%Y-%m-%d')
        release_df = release_df[(release_df['ReleaseDate'] >= start_date) & (release_df['ReleaseDate'] <= end_date)]
        release_df = release_df.reset_index(drop=True)
        return release_df
    
    def search_issues_by_version(self,version_id=None):
        release_list = ', '.join([f"'{item}'" for item in version_id])
        issue_list = self.jira.search_issues(f"project in ('{self.project}') and fixVersion in ({release_list}) ")
        issue_df = pd.DataFrame(columns=['Issue ID', 'Issue Type', 'Summary', 'Description', 'Status', 'Assignee', 'Reporter', 'Created Date', 'Updated Date', 'Parent', 'Parent ID', 'FixVersion', 'FixVersion Date', 'Environment'], index=range(len(issue_list)))
        for i in range(len(issue_list)):
            issue_df.loc[i, 'Issue ID'] = issue_list[i].key
            issue_df.loc[i, 'Issue Type'] = issue_list[i].fields.issuetype.name
            issue_df.loc[i, 'Summary'] = issue_list[i].fields.summary
            issue_df.loc[i, 'Description'] = issue_list[i].fields.description
            issue_df.loc[i, 'Status'] = issue_list[i].fields.status.name
            issue_df.loc[i, 'Assignee'] = issue_list[i].fields.assignee.displayName if issue_list[i].fields.assignee else None
            issue_df.loc[i, 'Reporter'] = issue_list[i].fields.reporter.displayName
            issue_df.loc[i, 'Created Date'] = issue_list[i].fields.created
            issue_df.loc[i, 'Updated Date'] = issue_list[i].fields.updated
            issue_df.loc[i, 'Parent'] = issue_list[i].fields.parent.fields.summary if hasattr(issue_list[i].fields, 'parent') else None
            issue_df.loc[i, 'Parent ID'] = issue_list[i].fields.parent.key if hasattr(issue_list[i].fields, 'parent') else None
            issue_df.loc[i, 'FixVersion'] = ', '.join([fv.name for fv in issue_list[i].fields.fixVersions]) if issue_list[i].fields.fixVersions else None
            issue_df.loc[i, 'FixVersion Date'] = ', '.join([fv.releaseDate for fv in issue_list[i].fields.fixVersions if hasattr(fv, 'releaseDate')]) if issue_list[i].fields.fixVersions else None
            
            # Safely handle environment with type checking
            try:
                environment = None
                if (isinstance(self.custom_fields, dict) and 
                    self.project in self.custom_fields and 
                    isinstance(self.custom_fields[self.project], dict) and
                    'Environment' in self.custom_fields[self.project]):
                    environment_field = self.custom_fields[self.project]['Environment']
                    environment = issue_list[i].fields.__getattribute__(environment_field)
                issue_df.loc[i, 'Environment'] = environment
            except (AttributeError, TypeError, KeyError):
                issue_df.loc[i, 'Environment'] = None
        return issue_df
    
    def get_issues_in_sprint(self, sprint_id=[None], cust_fields=None,type=['Bug', 'Story'], maxResults=False):
        """
        Function that will return all issues in a given sprint
        
        :param sprint_id: ID of the sprint you would like to get issues for.
        :param type: Pass it the issue types you would like to return. If only one type provided pass as a single string. For multiple values pass as a list of strings. If no type is passed it will default to stories and bugs.
        :return: Dataframe of issues"""
        issue_list = self.jira.search_issues(f"project in ('{self.project}') and Sprint in ('{sprint_id}') and type in {tuple(type)} ", maxResults=maxResults)
        issue_df = pd.DataFrame(columns=['Issue ID', 'Issue Type', 'Summary', 'Description', 'Status', 'Assignee', 'Reporter', 'Created Date', 'Updated Date', 'Parent', 'Parent ID', 'Story Points'], index=range(len(issue_list)))
        for i in range(len(issue_list)):
            issue_df.loc[i, 'Issue ID'] = issue_list[i].key
            issue_df.loc[i, 'Issue Type'] = issue_list[i].fields.issuetype.name
            issue_df.loc[i, 'Summary'] = issue_list[i].fields.summary
            issue_df.loc[i, 'Description'] = issue_list[i].fields.description
            issue_df.loc[i, 'Jira Board'] = issue_list[i].fields.project.name
            issue_df.loc[i, 'Status'] = issue_list[i].fields.status.name
            issue_df.loc[i, 'Assignee'] = issue_list[i].fields.assignee.displayName if issue_list[i].fields.assignee else None
            issue_df.loc[i, 'Fix Version'] = ', '.join([fv.name for fv in issue_list[i].fields.fixVersions]) if issue_list[i].fields.fixVersions else None
            issue_df.loc[i, 'Reporter'] = issue_list[i].fields.reporter.displayName
            issue_df.loc[i, 'Created Date'] = issue_list[i].fields.created
            issue_df.loc[i, 'Updated Date'] = issue_list[i].fields.updated
            issue_df.loc[i, 'Parent'] = issue_list[i].fields.parent.fields.summary if hasattr(issue_list[i].fields, 'parent') else None
            issue_df.loc[i, 'Parent ID'] = issue_list[i].fields.parent.key if hasattr(issue_list[i].fields, 'parent') else None
            issue_df.loc[i, 'Sprint'] = sprint_id
            
            # Safely handle story points with type checking
            try:
                story_points = None
                if (isinstance(self.custom_fields, dict) and 
                    self.project in self.custom_fields and 
                    isinstance(self.custom_fields[self.project], dict) and
                    'Story Points' in self.custom_fields[self.project]):
                    story_points_field = self.custom_fields[self.project]['Story Points']
                    story_points = issue_list[i].fields.__getattribute__(story_points_field)
                    print(f"DEBUG: Issue {issue_list[i].key} - Story Points: {story_points}")
                else:
                    print(f"DEBUG: Issue {issue_list[i].key} - Story Points field not found in custom_fields")
                issue_df.loc[i, 'Story Points'] = story_points
            except (AttributeError, TypeError, KeyError) as e:
                print(f"DEBUG: Issue {issue_list[i].key} - Story Points error: {e}")
                issue_df.loc[i, 'Story Points'] = None
            
            # Safely handle environment with type checking
            try:
                environment = None
                if (isinstance(self.custom_fields, dict) and 
                    self.project in self.custom_fields and 
                    isinstance(self.custom_fields[self.project], dict) and
                    'Environment' in self.custom_fields[self.project]):
                    environment_field = self.custom_fields[self.project]['Environment']
                    environment = issue_list[i].fields.__getattribute__(environment_field)
                issue_df.loc[i, 'Environment'] = environment
            except (AttributeError, TypeError, KeyError):
                issue_df.loc[i, 'Environment'] = None
        
        # Debug the final DataFrame structure
        print(f"DEBUG: get_issues_in_sprint - Final DataFrame columns: {list(issue_df.columns)}")
        print(f"DEBUG: get_issues_in_sprint - DataFrame shape: {issue_df.shape}")
        if len(issue_df) > 0:
            print(f"DEBUG: get_issues_in_sprint - Story Points column sample: {issue_df['Story Points'].head()}")
            print(f"DEBUG: get_issues_in_sprint - Story Points null count: {issue_df['Story Points'].isnull().sum()}")
        
        return issue_df

    def monthly_completed_items(self, start_date, end_date, status_list=['Done'], maxResults=False, cust_fields=None, type=['Story', 'Bug']):
        """Function that can be leveraged for Executive level discussions on work completed in a month. 
        This can help with reviewing staff month allocations by summing up all points of work done in a given month time period
        
        :param start_date: the first date of a time range to pull issues from
        :param end_date: the last date of a time range to pull issues from
        :param status_list: Status of issue changed to for it to be included in query"""
        if len(type) == 1:
            if len(status_list) == 1:
                jql = f"project in ('{self.project}') and status in {status_list[0]} AND status CHANGED DURING {start_date, end_date} and issuetype = {type[0]}"
            else:
                statuses = "'" + "', '".join(status_list) + "'"
                jql = f"project in ('{self.project}') and status in ({statuses}) AND status CHANGED DURING {start_date, end_date} and issuetype = {type[0]}"
        else:
            issue_type = ",".join(type)
            if len(status_list) == 1:
                jql = f"project in ('{self.project}') and status in {status_list[0]} AND status CHANGED DURING {start_date, end_date} and issuetype in ({issue_type})"
            else:
                statuses = "'" + "', '".join(status_list) + "'"
                jql = f"project in ('{self.project}') and status in ({statuses}) AND status CHANGED DURING {start_date, end_date} and issuetype in ({issue_type})"
        
        issues = self.jira.search_issues(jql, maxResults=maxResults)
        issue_dict = {}
        for issue in issues:
            try:
                issue_dict[issue.key] = {
                    'Month': start_date,
                    'Summary': issue.fields.summary,
                    'Description': issue.fields.description,
                    'Jira Board': issue.fields.project.name,
                    'Status': issue.fields.status.name,
                    'Due Date': issue.fields.duedate if issue.fields.duedate else None,
                    'Issue Type': issue.fields.issuetype.name,
                    'Issue ID': issue.key,
                    'Parent':  issue.fields.parent.fields.summary if  issue.fields.parent else None, 
                    'Parent ID':  issue.fields.parent.key if issue.fields.parent else None,
                    'Assignee': issue.fields.assignee.displayName if issue.fields.assignee else None,
                    'Fix Versions': ', '.join([fv.name for fv in issue.fields.fixVersions]) if issue.fields.fixVersions else None, 
                    'Reporter': issue.fields.reporter.displayName if issue.fields.reporter else None,
                    'Created Date': issue.fields.created,
                    'Updated Date': issue.fields.updated,
                }
                
                # Add custom fields if specified
                if cust_fields is not None:
                    for x in cust_fields[self.project]:
                        field_name = x
                        field_value = cust_fields[self.project][x]
                        field_data = issue.fields.__getattribute__(field_value)
                        if hasattr(field_data, 'value'):
                            issue_dict[issue.key][field_name] = field_data.value
                        else:
                            issue_dict[issue.key][field_name] = field_data
            except:
                continue

        return pd.DataFrame.from_dict(issue_dict, orient='index').reset_index(drop=True)


    def automated_release_email(self, current_items=None, future_items=None, email_list=None):
        """ 
        Function to send out automated release notes email to stakeholders

        :param project: str: Name of the JIRA project
        :param current_items: pd.DataFrame: DataFrame of items in the current release window
        :param future_items: pd.DataFrame: DataFrame of items in the upcoming release window
        :param email_list: list: List of email addresses to send the release notes to
        """
        styler = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'files', 'html_styles.html')
        print('Current Items:', current_items)
        current_items.sort_values(by=['Release Date', 'Initiative Name', 'Issue Type'], inplace=True)
        future_items.sort_values(by=['Release Date', 'Initiative Name', 'Issue Type'], inplace=True)
        uniquenames = current_items['Initiative Name'].unique()
        contact1 = current_items.groupby(['Initiative Name'])['Contact'].min()
        contact2 = current_items.groupby(['Initiative Name'])['Contact'].max()
        pd.set_option('display.max_colwidth', None)
        contact_dict = {}

        for i in uniquenames: 
            if contact1[i] == contact2[i]:
                contact_dict[i] = contact1[i]
            else:
                contact_dict[i] = contact1[i] + ' & ' + contact2[i]

        uniquenames2 = future_items['Initiative Name'].unique()
        contact1_future = future_items.groupby(['Initiative Name'])['Contact'].min()
        contact2_future = future_items.groupby(['Initiative Name'])['Contact'].max()
        pd.set_option('display.max_colwidth', None)
        contact_dict2 = {}

        for i in uniquenames2: 
            if contact1_future[i] == contact2_future[i]:
                contact_dict2[i] = contact1_future[i]
            else:
                contact_dict2[i] = contact1_future[i] + ' & ' + contact2_future[i]
        
        html_current_sprint = ''
        for i in uniquenames:
            x = pd.DataFrame(current_items[current_items['Initiative Name'] == i][['Issue Type', 'Environment', 'Summary', 'Description']])
            html_current_sprint += "<p>" + i + ' | ' + contact_dict[i] + "</p>" + x.to_html(index=False)

        html_future_sprint = ''
        for i in uniquenames2:
            x = pd.DataFrame(future_items[future_items['Initiative Name'] == i][['Issue Type', 'Environment', 'Summary', 'Description']])
            html_future_sprint += "<div><p>" + i + ' | ' + contact_dict2[i] + "</p></div>" + x.to_html(index=False) 

        template_file = open(styler, "r")
        index = template_file.read()
        template_file = Soup(index, 'html.parser')

        div_current_sprint = template_file.find(id = "current_sprint_text")
        div_current_sprint['class'] = 'font20'

        div_future_sprint = template_file.find(id = "future_sprint_text")
        div_future_sprint['class'] = 'font20'

        table_html_current = Soup(html_current_sprint,'lxml')
        table_html_future = Soup(html_future_sprint, 'lxml')

        for tag in table_html_current():
            for attribute in ["class", "id", "name", "style","border"]:
                del tag[attribute]

        table_current = table_html_current.findAll('table')
        po_current = table_html_current.findAll('p')
        table_future = table_html_future.findAll('table')
        po_future = table_html_future.findAll('p')

            # for i in range(len(po_current)):
            #     div_current_sprint.insert_after(po_current[i])
            #     print(div_current_sprint)
                #div_current_sprint.insert_after(table_current[i])
        template_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'files', 'template_email.html')
        final_html = open(template_path, "w", encoding="utf-8")
        template_file = (template_file.prettify())
        final_html.write(str(template_file))
        final_html.close()
        f = open(template_path, 'r')
        context = f.read()
        s = Soup(context, 'html.parser')
        if len(po_current) == 0:
            s.find(id = 'current_sprint_text').insert_after('No Releases deployed this week.')
            print('1: No items deployed this sprint')
        else:
            for i in range(len(po_current)):
                if i == 0:
                    s.find(id = 'current_sprint_text').insert_after(po_current[i])
                    s.find_all("p")[i+2].insert_after(table_current[i]) 
                else:
                    s.find_all("table")[i-1].insert_after(po_current[i])
                    s.find_all("p")[i+2].insert_after(table_current[i])
                i+=1
        print('5: Stories added to Current Week Section of Email')
        if len(po_future) == 0: 
            s.find(id = 'future_sprint_text').insert_after('No Releases currently planned for next week.')
        else:
            for i in range(len(po_future)):
                if i == 0:
                    s.find(id = 'future_sprint_text').insert_after(po_future[i])
                    s.find_all("p")[3 + len(uniquenames)].insert_after(table_future[i]) 
                else:
                    s.find_all("table")[len(uniquenames)+i-1].insert_after(po_future[i]) #5
                    s.find_all("p")[3+len(uniquenames)+i].insert_after(table_future[i])
                i+=1
        print('6: Stories added to Next Week Section of Email')

        email = MIMEText(str(s), 'html')
        email['From'] = 'releasenotes@gmail.com'
        email['To'] = ','.join(email_list)
        email['Subject'] = "{p} Analytics Release ({t} - {e})".format(p = self.project, e = date.today().strftime("%#m/%d"), t = (date.today()-timedelta(days=7)).strftime("%#m/%d"))

        try:
            #masked by removing company SMTP and just replaced with gmail for understanding purposes
            server = smtplib.SMTP('smtp.gmail.com', 25)
            server.connect("smtp.gmail.com", 25)      
            server.send_message(email)
            server.quit()
            print('7: Email Successfully sent to distribution list')
        except Exception as e:
            print("failed to login to server and send email, Error Message: {0}".format(e))

class TableauExtract:
    """
    Class created for activities interacting with Tableau. When initializing, the class requires:

    :param: extract_name: The name of how you would like the extract to appear on the tableau server
    :param: url: The tableau domain you would like to publish the extrac to.
    :param: site: If your tableau has different sites, which you would like to publish to
    :param: project: If you need your extract to live in a specific folder, this is where you would define that.
    """
    def __init__(self, extract_name, server, site, project, user, pw):
        self.extract_name = extract_name
        self.server = server
        if site == "default":
            self.site = "" 
        else:
            self.site = site
        self.project = project
        self.user = user
        self.pw = pw
        self.headers = {'accept': 'application/json', 'content-type': 'application/json'}

    def clean_dataframe_for_tableau(self, df):
        """
        Clean DataFrame for Tableau compatibility - handles numeric columns properly
        """
        import pandas as pd
        
        # Create a copy to avoid modifying the original
        cleaned_df = df.copy()
        
        # Define columns that should be numeric (story point columns)
        numeric_columns = ['Done', 'In Progress', 'In Review', 'Ready for Development', 'Ready for Prod', 'Story Points']
        
        for col in cleaned_df.columns:
            if col in numeric_columns:
                # Force these columns to be numeric, convert any strings to 0
                cleaned_df[col] = pd.to_numeric(cleaned_df[col], errors='coerce').fillna(0).astype(int)
            elif cleaned_df[col].dtype == 'object':
                # Keep as string, replace NaN with empty string
                cleaned_df[col] = cleaned_df[col].fillna('')
            elif cleaned_df[col].dtype in ['int64', 'float64']:
                # Already numeric, just fill NaN with 0
                cleaned_df[col] = cleaned_df[col].fillna(0)
        
        return cleaned_df
        
    def publish_extract(self, dataframe):
        """
        Converts dataframe into a Tableau Extract on a specfied location on your tableau server
        
        :param self: Description
        """
        # Clean the dataframe before converting to hyper format
        cleaned_df = self.clean_dataframe_for_tableau(dataframe)
        
        file_path = self.extract_name + ".hyper"
        pt.frame_to_hyper(cleaned_df, file_path, table="Extract")
        tableau_auth = TSC.TableauAuth(self.user, self.pw, site_id=self.site)
        server_obj = TSC.Server(self.server)

        version_url = f'{self.server}/api/2.8/serverinfo'
        try:
            server_version = requests.get(version_url, headers=self.headers)
            print('Successfully fetching version: ', server_version.status_code)
            server_json = json.loads(server_version.content)
            api_version = server_json['serverInfo']['restApiVersion']
            print('API Version: ', api_version)
        except requests.exceptions.RequestException as e:
            print(e)
            api_version = '2.8'
            print(f'Using default API version: {api_version}')
            
        signin_url = f'{self.server}/api/{api_version}/auth/signin'
        print(f"Signin url: {signin_url}")
        if self.site:
            print(f"Site : {self.site}")
            payload = {"credentials": {"name": self.user, "password": self.pw, "site": {"contentUrl": self.site }}}
        else:
            payload = {"credentials": {"name": self.user, "password": self.pw}}
        # send request
        try:
            req = requests.post(signin_url, json=payload, headers=self.headers)
        except requests.exceptions.RequestException as e:
            print(e)
        else:
            res = json.loads(req.content)

        site_id = self.site

        with server_obj.auth.sign_in(tableau_auth):
            print('Signed into Tableau Server')
            new_datasource = TSC.DatasourceItem(self.project)
            new_extract = server_obj.datasources.publish(new_datasource, file_path, 'Overwrite')
            print("Successfully publish datasource to {project_name}".format(project_name=self.extract_name))

class AhaProject:
    """
    Class for interacting with Aha API and fetching the relevant information required
    """
    def __init__(self, base_url, access_token):
        self.base_url = base_url
        self.access_token = access_token
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }
    
    def get_all_features(self, per_page=100):
        """Get all features with efficient pagination"""
        url = f"{self.base_url}/api/v1/features"
        all_features = []
        page = 1
        
        while True:
            try:
                params = {'page': page, 'per_page': per_page}
                result = requests.get(url, headers=self.headers, params=params, timeout=30)
                result.raise_for_status()
                
                data = result.json()
                features = data.get('features', [])
                
                if not features:
                    break
                    
                all_features.extend(features)
                print(f"Fetched {len(features)} features from page {page}")
                
                if len(features) < per_page:
                    break
                    
                page += 1
                time.sleep(0.1)  # Rate limiting
                
            except requests.exceptions.RequestException as e:
                print(f"Error fetching page {page}: {e}")
                break
        
        return pd.DataFrame(all_features)
    
    def get_feature_details():
        pass

class ConfluenceManager:
    def __init__(self, confluence_url, confluence_user, confluence_token):
        """Initialize Confluence connection"""
        self.confluence_url = confluence_url # e.g., 'https://yourcompany.atlassian.net/wiki'
        self.confluence_user = confluence_user  # Your email
        self.confluence_token = confluence_token  # API token
        
        # Initialize Confluence client
        self.confluence = Confluence(
            url=self.confluence_url,
            username=self.confluence_user,
            password=self.confluence_token,
            cloud=True  # Set to False for Confluence Server
        )
    
    def create_page(self, space_key, title, content, parent_id=None):
        """
        Create a new Confluence page
        
        Args:
            space_key (str): The space key where to create the page
            title (str): Page title
            content (str): Page content in Confluence Storage Format (HTML-like)
            parent_id (int, optional): Parent page ID if creating sub-page
            
        Returns:
            dict: Created page information
        """
        try:
            page = self.confluence.create_page(
                space=space_key,
                title=title,
                body=content,
                parent_id=parent_id,
                type='page',
                representation='storage'
            )
            print(f"✅ Page created successfully: {page['_links']['webui']}")
            return page
        except Exception as e:
            print(f"❌ Error creating page: {e}")
            return None
    
    def update_page(self, page_id, title, content, version_number=None):
        """
        Update an existing Confluence page
        
        Args:
            page_id (int): ID of the page to update
            title (str): New page title
            content (str): New page content
            version_number (int, optional): Current version number (will be auto-detected if not provided)
            
        Returns:
            dict: Updated page information
        """
        try:
            # Get current page info if version not provided
            if version_number is None:
                current_page = self.confluence.get_page_by_id(page_id, expand='version')
                version_number = current_page['version']['number']
            
            # Update the page
            page = self.confluence.update_page(
                page_id=page_id,
                title=title,
                body=content,
                version=version_number + 1,
                representation='storage'
            )
            print(f"✅ Page updated successfully: {page['_links']['webui']}")
            return page
        except Exception as e:
            print(f"❌ Error updating page: {e}")
            return None
    
    def find_page_by_title(self, space_key, title):
        """Find a page by title in a specific space"""
        try:
            pages = self.confluence.get_all_pages_from_space(
                space=space_key,
                start=0,
                limit=100,
                expand='version'
            )
            
            for page in pages:
                if page['title'] == title:
                    return page
            
            return None
        except Exception as e:
            print(f"❌ Error finding page: {e}")
            return None
    
    def create_or_update_page(self, space_key, title, content, parent_id=None):
        """
        Create a new page or update existing one
        
        Args:
            space_key (str): The space key
            title (str): Page title
            content (str): Page content
            parent_id (int, optional): Parent page ID for new pages
            
        Returns:
            dict: Page information
        """
        existing_page = self.find_page_by_title(space_key, title)
        
        if existing_page:
            print(f"📝 Updating existing page: {title}")
            return self.update_page(
                existing_page['id'], 
                title, 
                content, 
                existing_page['version']['number']
            )
        else:
            print(f"📄 Creating new page: {title}")
            return self.create_page(space_key, title, content, parent_id)
    
    def create_table_from_dataframe(self, df, table_class="confluenceTable"):
        """
        Convert pandas DataFrame to Confluence table HTML
        
        Args:
            df (pd.DataFrame): DataFrame to convert
            table_class (str): CSS class for the table
            
        Returns:
            str: HTML table string for Confluence
        """
        # Start table
        html = f'<table class="{table_class}"><colgroup>'
        
        # Create column groups
        for _ in df.columns:
            html += '<col/>'
        html += '</colgroup>'
        
        # Create header
        html += '<thead><tr>'
        for col in df.columns:
            html += f'<th><p><strong>{col}</strong></p></th>'
        html += '</tr></thead>'
        
        # Create body
        html += '<tbody>'
        for _, row in df.iterrows():
            html += '<tr>'
            for value in row:
                # Handle NaN values
                display_value = str(value) if pd.notna(value) else ''
                html += f'<td><p>{display_value}</p></td>'
            html += '</tr>'
        html += '</tbody></table>'
        
        return html


# Example usage functions
def create_release_notes_page(confluence_manager, release_data, space_key="TECH"):
    """Create a release notes page from Jira release data"""
    
    # Generate content
    current_date = datetime.now().strftime("%B %d, %Y")
    
    content = f"""
    <h1>Release Notes - {current_date}</h1>
    
    <h2>📋 Release Summary</h2>
    <p>This page contains the release notes for features and fixes deployed.</p>
    
    <h2>🚀 Released Items</h2>
    """
    
    if isinstance(release_data, pd.DataFrame) and not release_data.empty:
        # Convert DataFrame to Confluence table
        table_html = confluence_manager.create_table_from_dataframe(release_data)
        content += table_html
    else:
        content += "<p><em>No items released in this period.</em></p>"
    
    content += f"""
    
    <h2>📊 Release Metrics</h2>
    <ul>
        <li>Total Items Released: {len(release_data) if hasattr(release_data, '__len__') else 0}</li>
        <li>Release Date: {current_date}</li>
    </ul>
    
    <h2>🔗 Related Links</h2>
    <ul>
        <li><a href="{os.getenv('JIRA_URL')}/projects">Jira Project Dashboard</a></li>
    </ul>
    
    <p><em>Last updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</em></p>
    """
    
    # Create or update the page
    page_title = f"Release Notes - {datetime.now().strftime('%Y-%m-%d')}"
    
    return confluence_manager.create_or_update_page(
        space_key=space_key,
        title=page_title,
        content=content
    )


if __name__ == "__main__":
    # Example usage
    try:
        # Initialize Confluence manager
        cm = ConfluenceManager()
        
        # Example: Create a simple page
        sample_content = """
        <h1>Test Page</h1>
        <p>This is a test page created via API.</p>
        <h2>Features</h2>
        <ul>
            <li>Feature 1</li>
            <li>Feature 2</li>
        </ul>
        """
        
        # Create or update page
        page = cm.create_or_update_page(
            space_key="TECH",  # Replace with your space key
            title="API Test Page",
            content=sample_content
        )
        
        if page:
            print(f"🎉 Success! Page URL: https://yourcompany.atlassian.net/wiki{page['_links']['webui']}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("💡 Make sure to set CONFLUENCE_URL, CONFLUENCE_USER, and CONFLUENCE_TOKEN in your .env file")
