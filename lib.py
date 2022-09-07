# sync with /content/gdrive/My Drive/Colab Notebooks/lib/lib.py
import types, datetime
import pandas as pd
import functools
from google.cloud import bigquery
import hashlib

project = 'focal-elf-631'
mydataset = f'{project}.dongyoung_ttl30'

import os
os.environ["GOOGLE_CLOUD_PROJECT"] = project

def hash(x):
  return hashlib.md5(str(x).encode('utf-8')).hexdigest()

def today():
  return datetime.datetime.today().strftime('%Y%m%d')

class Query:
  registry = dict()

  # x = Query(myArgs)
  # behind the scenes, python sequentially calls
  # x = Query.__new__(myArgs)
  # x.__init__(myArgs)
  def __new__(cls, query_str_encoded):
    hashed = hash(query_str_encoded)
    if hashed in cls.registry:
      ret = cls.registry[hashed]
      print(f'Destination : {ret.destination}')
      return ret
    else:
      return object.__new__(cls)
  
  def __init__(self, query_str_encoded):
    if hasattr(self, 'done'):
      return
    self.query_str_encoded = query_str_encoded
    hashed = hash(query_str_encoded)
    assert hashed not in Query.registry
    self.hashed = hashed
    table_id = f'{today()}_{hashed}'
    destination = f'{mydataset}.{table_id}'
    self.destination = destination
    print(f'Destination : {self.destination}')
    
    existing_tables = [x.table_id for x in bigquery.client.Client(project=project).list_tables(mydataset)]
    if table_id in existing_tables:
      destination = destination + '_' + datetime.datetime.now().strftime('%H%M%S')
      query_to_run = self.fetch_statement()
    else:
      query_to_run = self.build_optimized_query()
    client = bigquery.client.Client(project=project,
                                    default_query_job_config=bigquery.job.QueryJobConfig(
                                        use_legacy_sql=False,
                                        maximum_bytes_billed=9223372036854775806,
                                        destination = destination
                                    ))
    job = client.query(query_to_run)
    self.job = job # only use this for to_dataframe()
    self.job.result()

    self.done = True # marker
    Query.registry[hashed] = self
  
  def fetch_statement(self):
    return f'''
      select * from {self.destination}
    '''

  def encode(self):
    return f'_beginencode_{self.hashed}_endencode_'
  
  def __repr__(self):
    return self.encode()

  def build_optimized_query(self):
    return self.query_string_factory(lambda x: x.fetch_statement(), lambda x: None)
  
  def show(self):
    self.query_string_factory(lambda x: x.query_str_encoded, lambda x: print(x))
  
  def query_string_factory(self, f, g):
    n = len('_beginencode_')
    m = len('_endencode_')
    x = self.query_str_encoded
    while '_beginencode_' in x:
      idx = x.index('_beginencode_')
      prefix = x[ : idx]
      suffix = x[ idx+n+m+32 : ]
      hashed = x[ idx+n : idx+n+32 ]
      query_obj = Query.registry[hashed]
      x = prefix + '(' + f(query_obj) + ')' + suffix
    g(x)
    return x



# The classes below have nothing to do with Query
# These are just tools to generate a query statement string "( select * from XXX where SOME_DATE_RANGE )"
# Because they are big and wasteful to store in cache
class unpartitioned:
  def __init__(self, project, dataset:str, table:str):
    self.project = project
    self.dataset = dataset
    self.table = table

  def range(self, begin, end=None) -> str:
    if end is None:
      end = begin
    begin = str(begin)[2:]
    end = str(end)[2:]

    query = f'''(
      select *
      from `{self}20*`
      where _TABLE_SUFFIX between "{begin}" and "{end}"
    )
    '''
    return query
  
  @functools.lru_cache()
  def schema(self):
    table_today = self.table + datetime.datetime.today().strftime('%Y%m%d')
    q = f'''
    select *
    from {self.project}.{self.dataset}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
    where
      table_name ="{table_today}"
        '''
    query = Query(q)
    ret = query.job.to_dataframe()
    ret.drop(ret.columns[:3], axis=1, inplace=True)
    return ret

  # e.g. focal-elf-631.prod.bid
  def __repr__(self):
    return '.'.join([self.project, self.dataset, self.table])

class partitioned:
  def __init__(self, project, dataset:str, table:str, timestamp_column='timestamp'):
    self.project = project
    self.dataset = dataset
    self.table = table
    self.timestamp_column = timestamp_column

  def range(self, begin, end=None) -> str:
    if end is None:
      end = begin
    begin = str(begin)
    end = str(end)
    
    begin = datetime.datetime.strptime(begin, "%Y%m%d").strftime("%Y-%m-%d")
    end = datetime.datetime.strptime(end, "%Y%m%d").strftime("%Y-%m-%d")

    query = f'''(
      select *
      from `{self}`
      where DATE({self.timestamp_column}) BETWEEN  "{begin}" AND "{end}"
    )
    '''

    return query
  
  @functools.lru_cache()
  def schema(self):
    q = f'''
    select
    *
    from
      {self.project}.{self.dataset}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
    where
      table_name ="{self.table}"
        '''
    query = Query(q)
    ret = query.job.to_dataframe()
    ret.drop(ret.columns[:3], axis=1, inplace=True)
    return ret

  # e.g. focal-elf-631.prod.bid
  def __repr__(self):
    return '.'.join([self.project, self.dataset, self.table])

class logical:
  def __init__(self, base, *clauses):
    self.base = base
    self.clauses = clauses

  def range(self, begin, end=None) -> str:
    query = f'''
    (
      select *
      from {self.base.range(begin,end)}
      where true
    '''

    for clause in self.clauses:
      query = query + f'''
      AND {clause}
      '''
    
    query = query + '''
    )
    '''
    return query
  
  @functools.lru_cache()
  def schema(self):
    return self.base.schema()

prod = types.SimpleNamespace()
prod.bid = unpartitioned(project, 'prod','bid')
prod.bid.rendezvous = logical(prod.bid, "array_length(rendezvous.partition.submissions) > 1")

prod_stream = types.SimpleNamespace()
prod_stream.imp = partitioned(project, 'prod_stream', 'imp')
prod_stream.cv = partitioned(project, 'prod_stream', 'cv')
prod_stream.bid  = partitioned(project, 'prod_stream', 'bid')
prod_stream.cv.install = logical(prod_stream.cv, "cv.event = 'INSTALL'")

explab_profile = types.SimpleNamespace()
explab_profile.dsp = partitioned('explab-298609', 'raw_profile', 'dsp_profile', 'created_at')
