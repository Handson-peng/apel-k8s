from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

host = 'es-atlas7.cern.ch'
port = 9203
eslogin = 'panda-analytics'
espasswd = 'mnfnmxuFSc78sadjkc-sx'
use_ssl = True
verify_certs = False
timeout=30
max_retries=10
retry_on_timeout=True

connection = Elasticsearch(
                [{'host': host, 'port': int(port)}],
                http_auth=(eslogin, espasswd),
                use_ssl=use_ssl,
                verify_certs=verify_certs,
                timeout=timeout,
                max_retries=max_retries,
                retry_on_timeout=retry_on_timeout
            )

def query_elasticsearch(index='atlas_harvesterworkers-*', computingsite, from_time, to_time):
    s = Search(using=connection, index=index)
    s = s.filter('term', **{'computingsite.keyword': computingsite})
    s = s.query('range',  endtime={'gte': from_time,'lte': to_time, 'format': 'yyyy-MM-dd HH:mm:ss'})
    s.aggs.metric('max_endtime', 'max', field='endtime')\
          .metric('min_endtime', 'min', field='endtime')\
          .metric('total_jobs', 'sum', field="njobs")\
          .pipeline('total_walltime', 'sum', script={'source':"(doc['endtime'].value.millis - doc['starttime'].value.millis)/1000"})
    res = s.execute()
    return res.aggregations.to_dict()

