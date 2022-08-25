import types, datetime
from google.cloud import bigquery
import requests

# QUERY MUST RETURN PLATFORM, ADVERTISER, CAMPAIGN
  
def active():
  today =  datetime.datetime.today().strftime('%Y%m%d')
  querystr = f'''
select platform_name, advertiser_name, campaign_name
from `focal-elf-631.prod.campaign_digest_merged_{today}` where inactive_since is null
  '''
  helper(querystr)
  

def most_recent_campaign_per_advertiser():
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
  helper(querystr)

def helper(querystr):
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
  
  with open('output.json', 'w') as f:
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
        print(f'Failed on {rowidx} th row : {platform_name}, {advertiser_name}, {campaign_name}')
        prevFail = True
    f.write('\n]\n')
  
if __name__=='__main__': active()
