
import os, fnmatch, json
from elasticsearch.helpers import bulk
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient

es = Elasticsearch()
ic = IndicesClient(es)
root_dir = 'data'
file_pat = 'doc_*'
url = 'localhost:9200/_bulk'
dirs = os.listdir(root_dir)
templates = ['security-analytics-packetbeat.json', 'security-analytics-winlogbeat.json']

def put_template(template):
    template_name = template.split('.')[0]
    with open(template, 'r') as f:
        template_body = json.load(f)
    resp = ic.put_template(template_name, template_body)

def find_files(file_pat, root_dir):
    for path, dirlist, filelist in os.walk(root_dir):
        for name in fnmatch.filter(filelist, file_pat):
            yield os.path.join(path,name)

def make_actions(files):
    for full_path in files:
        index_name = full_path.split('/')[-3]
        with open(full_path) as f:
            for _id, doc in enumerate(f):
                action =  { "_index": index_name, "_type": "doc", "_id": _id }
                action["_source"] = doc
                yield action

for template in templates:
    put_template(template)

actions = make_actions(find_files(file_pat, root_dir))
b = bulk(es, actions)

