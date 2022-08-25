def most_recent_campaign_per_advertiser():
  import types, datetime
  from google.cloud import bigquery
  import requests
  querystr = '''
  with keys as (
    select distinct campaign_id, advertiser_id, platform, timestamp
  from `standard_digest.campaign_digest`
  ),
  x as (
    select
      *,
      [campaign_id, platform] as campaign_x_platform
      from keys
  ),
  y as (
    select
      *,
      last_value(campaign_x_platform) over (partition by advertiser_id order by timestamp asc rows between UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_val
    from x
  )
  select distinct last_val[OFFSET(1)] as platform, advertiser_id,  last_val[OFFSET(0)] as campaign
  from y
  '''
  import types, datetime, os
  from google.cloud import bigquery
  import requests
  client = bigquery.client.Client(project='focal-elf-631',
                                  default_query_job_config=bigquery.job.QueryJobConfig(
                                                                        use_legacy_sql=False,
                                                                        maximum_bytes_billed=9223372036854775806
                                                                    ))
  job = client.query(querystr)
  rowidx = 0
  # You can get token via https://github.com/moloco/marvel2/blob/3fa703a7e41ec00f030970e51c9987a13a14e5ba/go/src/alfred/README.md#issue-an-iap-token
  token = os.environ['TOKEN']
  
  with open('most_recent_campaign_per_advertiser.json', 'w') as f:
    f.write('[\n')
    first = True 
    prevFail = False
    
    for row in job:
      if first:
        first = False
      elif not prevFail:
        f.write(',\n')
      platform_name = row[0]
      advertiser_name = row[1]
      campaign_name = row[2]
      rowidx += 1
      url = f"https://alfred-prod.adsmoloco.com/rtb/v1/rtb-campaigns/{campaign_name}?platform_id={platform_name}&advertiser_id={advertiser_name}"
      resp = requests.get(url, headers={"Authorization": f"Bearer {token}"})
      if (resp.status_code == 200):
        print(f'Writing {rowidx} th row : {platform_name}, {advertiser_name}, {campaign_name}')
        f.write(resp.text)
        prevFail = False
      else:
        prevFail = True
    f.write('\n]\n')
  



def run_query(): # finished up to row 8160th
  import types, datetime
  from google.cloud import bigquery
  import requests
  #today = datetime.datetime.today().strftime('%Y%m%d')
  today = '20220821'  # XXX
  client = bigquery.client.Client(project='focal-elf-631',
                                  default_query_job_config=bigquery.job.QueryJobConfig(
                                                                        use_legacy_sql=False,
                                                                        maximum_bytes_billed=9223372036854775806
                                                                    ))
  job = client.query(f'''select platform_name, advertiser_name, campaign_name from `focal-elf-631.prod.campaign_digest_merged_{today}`''')
  rowidx = 0
  # You can get token via https://github.com/moloco/marvel2/blob/3fa703a7e41ec00f030970e51c9987a13a14e5ba/go/src/alfred/README.md#issue-an-iap-token
  token = 'eyJhbGciOiJSUzI1NiIsImtpZCI6IjQwMmYzMDViNzA1ODEzMjlmZjI4OWI1YjNhNjcyODM4MDZlY2E4OTMiLCJ0eXAiOiJKV1QifQ.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJhenAiOiI4MzkyMzM0MDI3MDItZWkyMWJpMzFqNXF0bDZtMHVxMG9ndTY3bWxwdGVnczcuYXBwcy5nb29nbGV1c2VyY29udGVudC5jb20iLCJhdWQiOiI4MzkyMzM0MDI3MDItZTlmcjczZTdndW1lZHExYjFjNjVtaHZ2aGRwa2U4aWYuYXBwcy5nb29nbGV1c2VyY29udGVudC5jb20iLCJzdWIiOiIxMTIwMjM3MjU2Mzc4MjQ1ODcxNDAiLCJoZCI6Im1vbG9jby5jb20iLCJlbWFpbCI6ImRvbmd5b3VuZy5raW1AbW9sb2NvLmNvbSIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJhdF9oYXNoIjoiOFNLcVZZNUl5QWM5Y1duS0V2SHlodyIsImlhdCI6MTY2MTQ0MjY2MSwiZXhwIjoxNjYxNDQ2MjYxfQ.ipeVKoDtw9EI5rki8gWk8cXtWsYt45JwO-UA_SpcLOdOFk7LN3LCDI0oc5yDxExlTP-HjwnJT6uYSh5cdN7RaajllcOpjcBkrfXXnF-0NhV94qqpNiijWlaAp3Is7ZWdFLYz1u1Gn2-uhH7LirSeg_91hHD2MCZx7FzKxKvQl2mxmFEalL6QfsxgDLp7xchICIA9ELfk52r8r96QYbq4e8maDyFR1HsKZSZKlpft1E4toGi9R9Nr6hx4IWC-IfEna_5LUnQ-8t_LSnhmU9Y5-FU1VMbfbVbdouGruqoRA3PUws_EEJxQcvAI8i736EazKJlo8AAeogPEne_EovJ7jQ'
  
  with open('output.json', 'w') as f:
    f.write('[\n')
    first = True 
    
    for row in job:
      if first:
        first = False
      else:
        f.write(',\n')
      platform_name = row[0]
      advertiser_name = row[1]
      campaign_name = row[2]
      rowidx += 1
      url = f"https://alfred-prod.adsmoloco.com/rtb/v1/rtb-campaigns/{campaign_name}?platform_id={platform_name}&advertiser_id={advertiser_name}"
      resp = requests.get(url, headers={"Authorization": f"Bearer {token}"})
      if (resp.status_code == 200):
        print(f'Writing {rowidx} th row : {platform_name}, {advertiser_name}, {campaign_name}')
        f.write(resp.text)
    f.write('\n]\n')

if __name__=='__main__': most_recent_campaign_per_advertiser()




# OAuth2.0 token guide : https://support.google.com/googleapi/answer/6158849?hl=en
# how to use : https://cloud.google.com/endpoints/docs/frameworks/python/access_from_python
# pip3 install google-api-python-client
# pip3 install httplib2
# pip3 install oauth2client
# https://google-auth.readthedocs.io/en/master/user-guide.html
# https://developers.google.com/google-ads/api/docs/first-call/refresh-token#python






#alfred = 'https://alfred-prod.adsmoloco.com/rtb/v1/rtb-campaigns/VzWWdMct8XfsfgZW?platform_id=EA&advertiser_id=urXS2T72VM1BWBYX'
#
#
#  
#def run_query():
#  import types, datetime
#  from google.cloud import bigquery
#  import requests
#  today = datetime.datetime.today().strftime('%Y%m%d')
#  client = bigquery.client.Client(project='focal-elf-631',
#                                  default_query_job_config=bigquery.job.QueryJobConfig(
#                                                                        use_legacy_sql=False,
#                                                                        maximum_bytes_billed=9223372036854775806
#                                                                    ))
#  job = client.query(f'''select platform_name, advertiser_name, campaign_name from `focal-elf-631.prod.campaign_digest_merged_{today}` limit 1''')
#  for row in job:
#    platform_name = row[0]
#    advertiser_name = row[1]
#    campaign_name = row[2]
#    api = f"https://alfred-prod.adsmoloco.com/rtb/v1/rtb-campaigns/{campaign_name}?platform_id={platform_name}&advertiser_id={advertiser_name}"
#    print(api)
#    #r = requests.get(api)
#    #print(r.text)
#
#def temp():
#  from google.oauth2 import id_token
#  from google.oauth2 import service_account
#  import google.auth
#  import google.auth.transport.requests
#  from google.oauth2 import credentials
#  from google.auth.transport.requests import AuthorizedSession
#
#  api = 'https://alfred-prod.adsmoloco.com/rtb/v1/rtb-campaigns/VzWWdMct8XfsfgZW?platform_id=EA&advertiser_id=urXS2T72VM1BWBYX'
#
#  target_audience = api
#  url = api
#
#  creds = google.oauth2.credentials.Credentials(None, refresh_token='1//04X_WIkQwN80wCgYIARAAGAQSNwF-L9IrqNW88-FUFGuibjqYMgwy9mdNxH4lZkPePzj9CY6FiGCpl43ASn2tTEwQ1BOGP2aE9fg', client_id='839233402702-ei21bi31j5qtl6m0uq0ogu67mlptegs7.apps.googleusercontent.com', client_secret='Vnc3saR6pz6iivByf_ZtNr1o', token_uri='https://oauth2.googleapis.com/token')
#
#  authed_session = AuthorizedSession(creds)
#
#
#  # make authenticated request and print the response, status_code
#  resp = authed_session.get(api)
#
#
#
#  print(resp.status_code)
#  print(resp.text)
#
#def get_credentials():
#  # The best doc from user perspective
#  # https://gcloud.readthedocs.io/en/latest/google-cloud-auth.html#user-accounts-3-legged-oauth-2-0-with-a-refresh-token
#  from oauth2client.client import GoogleCredentials
#  credentials = GoogleCredentials.get_application_default()
#  print(credentials.to_json())
#  '''
#  {"access_token": null, "client_id": "839233402702-ei21bi31j5qtl6m0uq0ogu67mlptegs7.apps.googleusercontent.com", "client_secret": "Vnc3saR6pz6iivByf_ZtNr1o", "refresh_token": "1//04X_WIkQwN80wCgYIARAAGAQSNwF-L9IrqNW88-FUFGuibjqYMgwy9mdNxH4lZkPePzj9CY6FiGCpl43ASn2tTEwQ1BOGP2aE9fg", "token_expiry": null, "token_uri": "https://oauth2.googleapis.com/token", "user_agent": "Python client library", "revoke_uri": "https://oauth2.googleapis.com/revoke", "id_token": null, "id_token_jwt": null, "token_response": null, "scopes": [], "token_info_uri": null, "invalid": false, "_class": "GoogleCredentials", "_module": "oauth2client.client"}
#  '''
#
#def fail():
#  # tried https://requests-oauthlib.readthedocs.io/en/latest/ but this seems to be more for web apps, since it requires a domain
#  from requests_oauthlib import OAuth2Session
#  session = OAuth2Session(client_id='839233402702-ei21bi31j5qtl6m0uq0ogu67mlptegs7.apps.googleusercontent.com')
#  token = session.fetch_token('https://oauth2.googleapis.com/token', client_secret='Vnc3saR6pz6iivByf_ZtNr1o', authorization_response='https://alfred-prod.adsmoloco.com/rtb/v1/rtb-campaigns/{campaign_name}?platform_id={platform_name}&advertiser_id={advertiser_name}')
#  print(session.get('https://alfred-prod.adsmoloco.com/rtb/v1/rtb-campaigns/{campaign_name}?platform_id={platform_name}&advertiser_id={advertiser_name}').json())
#
#def retry():
#  # follow https://google-auth.readthedocs.io/en/master/user-guide.html
#  from google.oauth2 import id_token
#  from google.oauth2 import service_account
#  import google.auth
#  import google.auth.transport.requests
#  from google.auth.transport.requests import AuthorizedSession
#
#  creds = service_account.IDTokenCredentials.from_service_account_file(
#          'cred.json')
#
#  authed_session = AuthorizedSession(creds)
#
#  # make authenticated request and print the response, status_code
#  resp = authed_session.get(alfred)
#  print(resp.status_code)
#  print(resp.text)
#
#def bigquery_1():
#  '''
#  ACCESS_TOKEN="$(gcloud auth application-default print-access-token)"
#  curl -H "Authorization: Bearer $ACCESS_TOKEN" "https://alfred-prod.adsmoloco.com/rtb/v1/rtb-campaigns/VzWWdMct8XfsfgZW?platform_id=EA&advertiser_id=urXS2T72VM1BWBYX"
#
#  >  Invalid IAP credentials: Unable to parse JWT%
#  '''
#
#def cloudendpoint():
#  # https://google-auth.readthedocs.io/en/master/user-guide.html
#  from google.oauth2 import id_token
#  from google.oauth2 import service_account
#  import google.auth
#  import google.auth.transport.requests
#  from google.auth.transport.requests import AuthorizedSession
#
#  target_audience = 'https://alfred-prod.adsmoloco.com'
#  url = alfred
#
#  'cp /Users/dongyoungkim/.config/gcloud/application_default_credentials.json /Users/dongyoungkim/src/tools'
#  creds = service_account.IDTokenCredentials.from_service_account_file(
#          'cred.json', target_audience=target_audience)
#
#  authed_session = AuthorizedSession(creds)
#
#  # make authenticated request and print the response, status_code
#  resp = authed_session.get(url)
#  print(resp.status_code)
#  print(resp.text)
#
#  # to verify an ID Token
#  request = google.auth.transport.requests.Request()
#  token = creds.token
#  print(token)
#  print(id_token.verify_token(token,request))
#  
#  
#
#if __name__=='__main__': fail()
