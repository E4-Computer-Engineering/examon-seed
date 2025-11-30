import re
import sys
import json
import codecs

import subprocess


def sanitize_json_payload(raw_json):

    def escape_quotes_in_values(json_string):
        pattern = r'(":")((?:(?!":").)*?)("(?:,|}))'
        replacement = lambda m: m.group(1) + m.group(2).replace('"', r'\"') + m.group(3)
        return re.sub(pattern, replacement, json_string)

    try:
        

        raw_json = re.sub(r'(?<!\\)(\\\\)+', '\\\\', raw_json)
        
        raw_json = escape_quotes_in_values(raw_json)

        raw_json = re.sub(r'\\(?!["\\/bfnrt])', '', raw_json)
        
        raw_json = codecs.decode(raw_json, 'unicode_escape')

        json_obj = json.loads(raw_json.strip(), strict=False)
        
    except ValueError as e:
        print "Error decoding JSON: {}".format(e)
        line_number = int(str(e).split('line ')[1].split(' ')[0])
        json_lines = raw_json.splitlines()
        if line_number <= len(json_lines):
            print "Problematic JSON line {}: {}".format(line_number, repr(json_lines[line_number+1]))
        else:
            print "Line number {} is out of range.".format(line_number)
        
    return json_obj


def parse_concatenated_json(file_content, obj_separator="### EOF ###"):
    
    def escape_quotes_in_values(json_string):
        pattern = r'(":")((?:(?!":").)*?)("(?:,|}))'
        replacement = lambda m: m.group(1) + m.group(2).replace('"', r'\"') + m.group(3)
        return re.sub(pattern, replacement, json_string)
    
    merged_json = {}
    
    raw_jsons = file_content.split(obj_separator)

    for raw_json in raw_jsons:
        if not raw_json.strip():
            continue
        
        try:
           
            raw_json = re.sub(r'(?<!\\)(\\\\)+', '\\\\', raw_json)
            
            raw_json = escape_quotes_in_values(raw_json)

            raw_json = re.sub(r'\\(?!["\\/bfnrt])', '', raw_json)
            
            raw_json = codecs.decode(raw_json, 'unicode_escape')

            json_obj = json.loads(raw_json.strip(), strict=False)
            
            if not merged_json:
                merged_json = json_obj
            else:
                if "Jobs" in json_obj:
                    if "Jobs" not in merged_json:
                        merged_json["Jobs"] = {}
                    merged_json["Jobs"].update(json_obj["Jobs"])
                
        except ValueError as e:
            print "Error decoding JSON: {}".format(e)
            line_number = int(str(e).split('line ')[1].split(' ')[0])
            json_lines = raw_json.splitlines()
            if line_number <= len(json_lines):
                print "Problematic JSON line {}: {}".format(line_number, repr(json_lines[line_number+1]))
            else:
                print "Line number {} is out of range.".format(line_number)
    
    return merged_json


class PBSRd(object):
    
    def __init__(self):
        self.data = []
        self.key = ''
        self.name_field = ''
        
    def node(self):
        self.data = json.loads(subprocess.check_output('pbsnodes -avjL -F json', shell=True))
        self.key = 'nodes'
        self.name_field = 'hostname'
        return self
    
    def job(self):
        self.data = json.loads(subprocess.check_output('qselect -tm.gt.$(date -d "5 minutes ago" "+%Y%m%d%H%M") -s QRF | xargs qstat -xfF json -J', shell=True))
        self.key = 'Jobs'
        self.name_field = 'job_id'
        return self
    
    def get(self):
        """Get all slurm node information.
            :returns: Dictionary of dictionaries whose key is the node name.
            :rtype: `dict`
        """
        d = self._add_key(self.data, self.key)
        return d[self.key]
        
    def _groupby_key(self, d, key):
        """Return a grouped dictionary d by the given key.
            
            :The group key is not removed from the group itself.
        """        
        return  dict([(x[key], x) for x in d])
    
    def _add_key(self, d, key):
        if key == 'Jobs':
            for k,v in d[key].items():
                v['Job_Id'] = k.split('.')[0]
        if key == 'nodes':
               for k,v in d[key].items():
                v['node'] = k         
        return d
    
    def get_version(self):
        return self.data['pbs_version']
    
    
    def get_server(self):
        return self.data['pbs_server']
    

if __name__ == "__main__":
    
    pypbs = PBSRd()
    
    try:
        if sys.argv[1] == '-i':  # qselect
            ret = pypbs.node()
        elif sys.argv[1] == '-q':  # pbsnodes
            ret = pypbs.job()
        else:
            ret = []
    except Exception as e:
        print("Query failed - {0}".format(e))
        sys.exit(1)

    print(json.dumps(ret.get()))