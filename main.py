# -*- coding: utf-8 -*-
import requests
import json
from functools import reduce

import os
from dotenv import load_dotenv
from pathlib import Path
import re
import datetime

load_dotenv()

class GithubData:
        
    def __init__(self, username):
        self.username =  username
        
        self.orgs = [
        'all-but-dissertation',
        'cornell-cdses',
        'minimod-nutrition',
        'staaars-plus',
        'uganda-rideshare-projects'
    ]
    
    @property
    def headers(self):
        return {"Authorization": "token " + os.getenv('GITHUB_KEY'), 
                                          "Accept": "application/vnd.github.v3+json"}
        
    def _get_user_repos(self):
        return requests.get(f'https://api.github.com/users/{self.username}/repos',
                               headers = self.headers).json()
        
    def _get_org_repos(self, org):
        return requests.get(f'https://api.github.com/orgs/{org}/repos',
                               headers = self.headers).json()
    
    def _get_repo_name(self, repo):
        return repo['name']
    
    def _get_repo_url(self, repo):
        return repo['url']
    
    def _get_issues_url(self, repo_url):
        return f"{self._get_repo_url(repo_url)}/issues"
        
    def get_all_issue_urls(self):
        
        # request each org
        repo_list = []
        
        for org in self.orgs:
            repo_list.append(self._get_org_repos(org))
            
        repo_list.append(self._get_user_repos())
            
        flattened_repo_list = reduce(lambda x,y : x + y, repo_list)
        
        orgs = [i['owner']['login'] for i in flattened_repo_list]

        return  {self._get_repo_name(r) : self._get_issues_url(r) for r in flattened_repo_list}, orgs
    
    def request_issue(self, url):
        
        params = {'state' : "all", "per_page" : 100}
        
        return requests.get(url,
                            params = params,
                            headers = self.headers)
        
    def update_issue(self, owner, repo, issue_number):
        pass

#TODO: methods that create the table if it doesn't exist (it's empty if it isn't equivalent or something?)
class NotionDatabase:
    
    def __init__(self, database_id=None):
        if database_id is None:
            self.database_id = os.getenv("NOTION_DATABASE")
        else:
            self.database_id = database_id
            
        self.headers = {"Authorization": "Bearer " + os.getenv('NOTION_KEY'), 
                                        "Content-Type": "application/json",
                                        "Notion-Version" : "2021-08-16"}
            
    def request_database(self):
        return requests.get(f'https://api.notion.com/v1/databases/{self.database_id}/', 
                            headers = self.headers)
    
    def post_page(self, data):
        return requests.post(f'https://api.notion.com/v1/pages',
                                headers=self.headers,
                                data = json.dumps(data)
                                )
    def title(self, row):
        return {"title" : [{ "text" : {"content" : row['title']}}]}
    
    def state(self, row):
        return {"rich_text" : [{"text" : { "content" : row['state']}}]}
    
    def body(self, row):
        # limit size of body text if goes over 2000
        if row['body'] and len(row['body']) > 2000:
            row['body'] = row['body'][0:2000]
            
        return {"rich_text" : [{"text" : { "content" : row['body']}}]} if row['body'] is not None else {"rich_text" : [{"text" : { "content" : ""}}]}
        
    def org(self, organization):
        return {"rich_text" : [{"text" : { "content" : organization}}]} 
    
    def repo(self, name):
        return {"rich_text" : [{"text" : { "content" : name}}]} 
            
    def label(self, row):
        return {"multi_select" : [{"name" : lab['name'] } for lab in row['labels']]} \
            if row['labels'] else {"multi_select" : [{"name" : lab['name'] } for lab in row['labels']]}
            
    def url(self, row):
        return {"url" : row['html_url']} 
    
    def api_url(self, row):
        return {"url" : row['url']} 
    
    def issue_number(self, row):
        return {"number" : row['number']}
    
    @property
    def github_type(self):
        return {'multi_select' : [{'name' : 'github'}]}
    
            
    def upload_issues(self, row, name, organization):
        
        github_issues = {}
        
        github_issues["title"] = self.title(row)
        github_issues["state"] = self.state(row)
        github_issues["body"] = self.body(row)
        github_issues["org"] = self.org(organization)
        github_issues['repo'] = self.repo(name)
        github_issues['labels'] = self.label(row)
        github_issues['url'] = self.url(row)
        github_issues['api_url'] = self.api_url(row)
        github_issues['issue_number'] = self.issue_number(row)
        
        notion_dict = {"parent" : {"database_id" : os.getenv("NOTION_DATABASE")}, 
            "properties" : {"Title" : github_issues['title'],
                            "State" : github_issues['state'],
                            "Body" : github_issues['body'],
                            "Organization" : github_issues['org'],
                            "Repo" : github_issues["repo"],
                            "Labels" : github_issues['labels'],
                            "URL" : github_issues['url'],
                            "Github Issue Number" : github_issues['issue_number'],
                            "Github API URL" : github_issues['api_url'],
                            "Type" : self.github_type
                            } 
            }
        
        return self.post_page(data = notion_dict)
    
        
# Get list of all my repositories
github = GithubData("amichuda")
notion = NotionDatabase()   
    
def upload_all_issues(cache=True, since=None, cache_glob=None):
    
    issues_urls, orgs = github.get_all_issue_urls()
    
    for (name, url), org in zip(issues_urls.items(), orgs):
        
        github_data = github.request_issue(url)
        
        if github_data.status_code != 200:
            raise Exception(f'Response Status: {github_data.status_code}')
        else:
            github_data = github_data.json()
                
            
            # Check that database exists
            notion_request = notion.request_database()
                    
            if notion_request.status_code != 200:
                raise Exception(notion_request.status_code)
            else:
                notion_request = notion_request.json()
                
            print(
                f"""
                **************************
                Checking {org}/{name}
                **************************
                """
            )
                
            for i in github_data:
                
                # check if updated_at `since`
                if since is not None and datetime.datetime.strptime(i['updated_at'], "%Y-%m-%dT%H:%M:%SZ") <= since:
                    
                    print(f"{org}/{name}/{i['number']} created before {since}, skipping")
                    continue
                
                # Now check if that issue exists in the cache
                if since is not None and Path(f"cache/{org}_{name}_{i['number']}.json").is_file():
                    print("Issue already exists in cache, skipping")
                    continue
                
                print(f"Adding {i['title']}")
                
                notion_post = notion.upload_issues(i, name, org)
                
                print(notion_post.json())
                
                if cache:
                    with open(f"cache/{org}_{name}_{i['number']}.json", 'w') as f:
                        f.write(notion_to_json(notion_post.json()))
                
                if notion_post.json()['object'] == "error":
                    raise Exception(f"Error {notion_post.json()['status']}, {notion_post.json()['message']}")
                
def check_notion_changes(file_json, org, repo, issue_number, notion_headers, github_headers):
        
    query_dict = {
        'filter' : {
            "and" : [
                {
                    "property" : "Organization",
                    "rich_text" : {
                        "equals" : f"{org}"
                    } 
                },
                {
                    "property" : "Repo",
                    "rich_text" : {
                        "equals" : f"{repo}"
                    }
                },
                {
                    "property" : "Github Issue Number",
                    "number" : {
                        "equals" : issue_number
                    }
                }
            ]
        }
    }
    
    # now query notion database for that info and find that page
    query_database = requests.post(f"https://api.notion.com/v1/databases/{os.getenv('NOTION_DATABASE')}/query",
                                   headers=notion_headers,
                                   data = json.dumps(query_dict))
    
    if query_database.status_code != 200:
        raise Exception(query_database.content)
    
    query_database = query_database.json()
    
    # Now compare file and query
    database_info = query_database['results'][0]['properties']
    
    # Now we get the information from the github and check for changes there
    gh_issue = requests.get(database_info['Github API URL']['url'],
                       headers=github_headers)
    
    if gh_issue.status_code != 200:
        raise Exception(gh_issue.content)
    
    gh_issue = gh_issue.json()
    
    # parse state, labels, body and title and check for differences between that and `file_json`
    
    # title
    title_comp = gh_issue['title'] == database_info['Title']['title'][0]['plain_text']
    
    # state
    state_comp = gh_issue['state'] == database_info['State']['rich_text'][0]['plain_text']
    
    # labels
    label_comp = sorted([i['name'] for i in gh_issue['labels']]) == sorted([i['name'] for i in database_info['Labels']['multi_select']])
    
    # body
    notion_body = database_info['Body']['rich_text'][0]['plain_text']
    
    gh_body = gh_issue['body']
    
    try:
        gh_body = gh_body[0:2000]
    except TypeError:
        gh_body = gh_body
    
    gh_body = gh_body if gh_body is not None else ""
    
    body_comp = gh_body == notion_body
    
    if not all([title_comp, state_comp, label_comp, body_comp]):
        print("Changes made in Github, updating Notion...")
        
        # First, get page_id
        page_id = query_database['results'][0]['id']
        
        # Create json to send
        notion_dict = {
            "properties" : {
                "Body" :  {
                    'rich_text' : [{"text" : {'content' : gh_body}}]
                },
                'Title' : {
                    'title' : [{"text" : {'content' : gh_issue['title'] }}]
                },
                "State" : {
                    'rich_text' : [{ "text" : {'content' : gh_issue['state']}}]
                },
                'Labels' : {
                    'multi_select' : [{'name' : i['name']} for i in gh_issue['labels']]
                }
            }
        }
        
        # Now patch with changes
        patch_notion = requests.patch(f"https://api.notion.com/v1/pages/{page_id}",
                                      headers = notion_headers,
                                      data = json.dumps(notion_dict))
        
        if patch_notion.status_code != 200:
            raise Exception(patch_notion.content)
        
        # now query notion database again to get updated table, in case things updated
        query_database = requests.post(f"https://api.notion.com/v1/databases/{os.getenv('NOTION_DATABASE')}/query",
                                   headers=notion_headers,
                                   data = json.dumps(query_dict))
    
        if query_database.status_code != 200:
            raise Exception(query_database.content)
        
        query_database = query_database.json()
        
        # Now compare file and query
        database_info = query_database['results'][0]['properties']
            
        with open(f"cache/{org}_{repo}_{issue_number}.json", 'w') as f:
            print("Updating cache with changes from Github...")
            f.write(json.dumps(query_database['results'][0]))
    
    # Now check properties to see if something changed
    if file_json != database_info:
        # Find difference between them
        # difference = set(file_json).difference(set(database_info)) #TODO: not sure if this might slow things down if I make it recursive?
        print(f"Found a Change!")
        # Post to github
        # only keep what github can take!
        github_post_dict = {
            'title' : database_info['Title']['title'][0]['text']['content'],
            'body' : database_info['Body']['rich_text'][0]['text']['content'],
            'state' : database_info['State']['rich_text'][0]['text']['content'],
            'labels' : database_info['Labels']['multi_select']
        }
        
        patch = requests.patch(database_info['Github API URL']['url'],
                       headers=github_headers,
                       data = json.dumps(github_post_dict))
        
        if patch.status_code != 200:
            raise Exception(patch.content)
        
        with open(f"cache/{org}_{repo}_{issue_number}.json", 'w') as f:
            print("Updating cache...")
            f.write(json.dumps(query_database['results'][0]))

#TODO: If file in cache doesn't exist, then that means notion is trying to make a new issue, so post a new issue in the repository
#TODO: Still need to figure out how best to poll github to check for new issues there
#TODO: Possibly put comments into each row's page? 
#TODO: make more descriptive message when changes are found

def notion_to_json(notion_dict):
    
    d = {
        'title' : notion_dict['properties']['Title']['title'][0]['plain_text'],
        'url' : notion_dict['properties']['URL']['url'],
        'state' : notion_dict['properties']['State']['rich_text'][0]['plain_text'],
        'labels' : [i['name'] for i in notion_dict['properties']['Labels']['multi_select']],
        'organization' : notion_dict['properties']['Organization']['rich_text'][0]['plain_text'],
        'repo' : notion_dict['properties']['Repo']['rich_text'][0]['plain_text'],
        'issue_number' : notion_dict['properties']['Github Issue Number']['number'],
        'body' : notion_dict['properties']['Body']['rich_text'][0]['plain_text'],
        'api_url' : notion_dict['properties']['Github API URL']['url'],
        'page_id' : notion_dict['id']
    }
    
    return json.dumps(d)

def github_to_json(github_dict, org, repo):
    
    github_dict['body'] = github_dict['body'] if github_dict['body'] is not None else ""
    if len(github_dict['body']) >= 2000:
        github_dict['body'] = github_dict['body'][0:2000]
    
    d = {
        'title' : github_dict['title'],
        'url' : github_dict['html_url'],
        'state' : github_dict['state'],
        'labels' : [i['name'] for i in github_dict['labels']],
        'organization' : org,
        'repo' : repo,
        'issue_number' : github_dict['number'],
        'body' : github_dict['body'],
        'api_url' : github_dict['url'],
        'page_id' : ""
    }
    
    return json.dumps(d)

def json_to_github(json_dict):
    
    github_dict=  {
        'title' : json_dict['title'],
        'state' : json_dict['state'],
        'labels' : [{'name' : i} for i in json_dict['labels']],
        'body' : json_dict['body'],
    }
    
    return json.dumps(github_dict)
    
def json_to_notion(json_dict):
    
    notion_dict = {
            "properties" : {
                "Body" :  {
                    'rich_text' : [{"text" : {'content' : json_dict['body']}}]
                },
                'Title' : {
                    'title' : [{"text" : {'content' : json_dict['title'] }}]
                },
                "State" : {
                    'rich_text' : [{ "text" : {'content' : json_dict['state']}}]
                },
                'Labels' : {
                    'multi_select' : [{'name' : i} for i in json_dict['labels']]
                },
                'Type' : {
                    'multi_select' : [{'name' : 'github'}]
                }
            }
        }
    
    return json.dumps(notion_dict), json_dict['page_id']
    

def notion_command(file_json, notion_headers, org, repo, issue_number):
    
    query_dict = {
        'filter' : {
            "and" : [
                {
                    "property" : "Organization",
                    "rich_text" : {
                        "equals" : f"{org}"
                    } 
                },
                {
                    "property" : "Repo",
                    "rich_text" : {
                        "equals" : f"{repo}"
                    }
                },
                {
                    "property" : "Github Issue Number",
                    "number" : {
                        "equals" : issue_number
                    }
                }
            ]
        }
    }
    
    # now query notion database for that info and find that page
    query_database = requests.post(f"https://api.notion.com/v1/databases/{os.getenv('NOTION_DATABASE')}/query",
                                   headers=notion_headers,
                                   data = json.dumps(query_dict))
    
    if query_database.status_code != 200:
        raise Exception(query_database.content)
    
    query_database = query_database.json()
    
    # Now compare file and query
    database_info = notion_to_json(query_database['results'][0])
    
    if file_json != json.loads(database_info):
        
        print(f"Found a Change! Saving to notion_command/{org}_{repo}_{issue_number}.json")
        
        # save command
        with open(f"cache/notion_commands/{org}_{repo}_{issue_number}.json", 'w') as f:
            f.write(database_info)
        
    return query_database

def patch_github_issue(notion_command, github_headers):
        
    # read notion_command
    with open(notion_command, 'r') as f:
        notion_command_instructions = f.read()
    
    notion_command_instructions = json.loads(notion_command_instructions)
    
    # convert our json to github json for send
    github_patch = json_to_github(notion_command_instructions)
    
    # patch github
    patch = requests.patch(notion_command_instructions['api_url'],
                    headers=github_headers,
                    data = github_patch)
    
    if patch.status_code != 200:
        raise Exception(patch.content)
    
    # If all goes well, rewrite json file 
    with open(f"cache/{notion_command.stem}.json", 'w') as f:
        f.write(json.dumps(notion_command_instructions))
        
    # ... and delete the notion_command
    notion_command.unlink()
        

def github_command(file_json, github_headers, org, repo, issue_number):
    
    gh_issue = requests.get(file_json['api_url'],
                       headers=github_headers)
    
    if gh_issue.status_code != 200:
        raise Exception(gh_issue.content)
    
    gh_issue = github_to_json(gh_issue.json(), org, repo)
    
    gh_issue = json.loads(gh_issue)

    gh_issue['page_id'] = file_json['page_id']
    
    if file_json != gh_issue:
        
        print(f"Found a Change! Saving to github_commands/{org}_{repo}_{issue_number}.json")
        
        with open(f"cache/github_commands/{org}_{repo}_{issue_number}.json", 'w') as f:
            f.write(json.dumps(gh_issue))
        
    return f"cache/github_commands/{org}_{repo}_{issue_number}.json"

def patch_notion_database(github_command, notion_headers):
    
    # read github_command
    with open(github_command, 'r') as f:
        github_command_instructions = f.read()
    
    github_command_instructions = json.loads(github_command_instructions)
    
    #if body is over 2000, cut the body down
    if len(github_command_instructions['body']) >= 2000:
        github_command_instructions['body'] = github_command_instructions['body'][0:2000]
    
    # convert our json to notion json for send
    notion_patch, page_id = json_to_notion(github_command_instructions)
    
    # Now patch with changes
    patch_notion = requests.patch(f"https://api.notion.com/v1/pages/{page_id}",
                                    headers = notion_headers,
                                    data = notion_patch)
    
    if patch_notion.status_code != 200:
        raise Exception(patch_notion.content)
    
    # If all goes well, rewrite json file 
    with open(f"cache/{github_command.stem}.json", 'w') as f:
        f.write(json.dumps(github_command_instructions))
        
    # ... and delete the github_command
    github_command.unlink()
    
# Create your views here.
if __name__ == "__main__":
    # Run this if you want to re-create the table from scratch
    # upload_all_issues(cache=True, since=None)
    
    # Get all cache files
    cache = Path("cache")
    
    now_minus_twenty = datetime.datetime.now() + datetime.timedelta(minutes=-20)
    
    while True:
        print(f"Check issues since {now_minus_twenty}")
        # check for any new issues created since NOW (using ISO 8601 format)
        # upload_all_issues(cache=True, since = now_minus_twenty)
        
        # # get glob of master json files
        for path in cache.glob("*.json"):
            print(f"Checking cache: {path.stem}")
            
            # get pertinent information from command
            match = re.match(r"(amichuda|all-but-dissertation|minimod-nutrition|cornell-cdses|uganda-rideshare-projects|staaars-plus)_(.*)_([0-9]+)", path.stem)
            org = match[1]
            repo  = match[2]
            issue_number = match[3]
            
            # read file in cache
            with open(path, 'rb') as f:
                file = f.read()
            
            file_json = json.loads(file.decode('utf-8'))
            
            # Check for changes in github to be patched to notion
            github_command(file_json, github.headers, org, repo, int(issue_number))
            
            # check for change in notion to be patched to github
            notion_command(file_json, notion.headers, org, repo, int(issue_number))
            
        # Get glob of notion commands
        notion_commands = Path("cache/notion_commands/").glob("*.json")

        print("Beginning to implement commands...")
        for command in notion_commands:
            print(f"patching github issue with {command.stem}")
                
            patch_github_issue(command, github.headers)           
        
        # Get glob of github commands
        github_commands = Path("cache/github_commands/").glob("*.json")
        
        for command in github_commands:
            print(f"patching notion database with {command.stem}")
            
            patch_notion_database(command, notion.headers)