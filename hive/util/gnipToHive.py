
from datetime import timedelta
from datetime import datetime
import json
import re
import sys

dir = 'wisconsin_politics_to_rt'

##get the Gnip rules file
jsonObj = json.loads(open('../../gnip/rules/wisconsin_politics_users_top_@_to_from_retweets_of.json', 'r').read())

## Convert datetimes
## TK: Add something here which automatically rounds fromDate down and toDate by day
fromDate = datetime.strptime(jsonObj['fromDate'], "%Y%m%d%H%M%S")
toDate   = datetime.strptime(jsonObj['toDate'], "%Y%m%d%H%M%S") + timedelta(hours = 1)

values = { 'words': [], 'to': [], 'from': [], 'retweets_of': [] }

## sort into the right bin
for s in jsonObj['rules']:
	s = s['value'].lower()
	if re.match('contains:', s):
		## hackish way to do this but whatevs
		s = re.sub("contains:", "", s)
		s = re.sub("\\\\", "", s)
		s = re.sub('"', "", s)
		values['words'].append(s)
	elif re.match('(from|to|retweets_of):', s):
		m = re.match('(from|to|retweets_of):', s)
		g = m.group(1)
		values[g].append( re.sub(g + ":", "", s) )
	else:
		values['words'].append( s )

## set the max in the right column
upper = ( max( [ len(values[x]) for x in values.keys() ] ) / 50 ) + 2

for m in range(0, upper):
	pdir = dir + "-" + str(m)
	s    = m*50
	e    = (m+1)*50 

	## username values
	if ( values["from"][s:e] ):
		fr = " or ".join(map(lambda x: "lower(user.screen_name) = '%s'" % x, values['from'][s:e] ))
	else:
		fr = "0"
	
	if ( values["to"][s:e] ):
		to = " or ".join(map(lambda x: "lower(in_reply_to_screen_name) = '%s'" % x, values['to'][s:e] ))
	else:
		to = "0"

	if ( values["retweets_of"][s:e] ):
		rt = " or ".join(map(lambda x: "lower(retweeted_status.user.screen_name) = '%s'" % x, values['retweets_of'][s:e] ))
	else:
		rt = "0"

	## text values
	if ( values["words"][s:e] ):
		text    = " or ".join(map(lambda x: "lower(text) like '%" + x + "%'", values['words'][s:e] )) 
		rt_text = " or ".join(map(lambda x: "lower(retweeted_status.text) like '%" + x + "%'", values['words'][s:e] )) 
	else:
		text = "0"
		rt_text = "0"

	## TK: Need to come up with a more general date boundary
	query = """
insert overwrite local directory '""" + pdir + """' 
row format delimited 
fields terminated by "\\t"
select id_str, created_at, regexp_replace(text, "[ \\t\\r\\n]+", " "), user.id_str, regexp_replace(user.name, "[ \\t\\r\\n]+", " "), 
user.screen_name, retweeted_status.id_str, retweeted_status.created_at, regexp_replace(retweeted_status.text, "[ \\t\\r\\n]+", " "), 
retweeted_status.user.id_str, regexp_replace(retweeted_status.user.name, "[ \\t\\r\\n]+", " "), retweeted_status.user.screen_name 
from gh_rc
where year = %d and ( (month = %d and day >= %d) or month = %d or (month = %d and day <= %d) ) and (%s or %s or %s or %s or %s)""" % (fromDate.year, fromDate.month, fromDate.day, 6, toDate.month, toDate.day, fr, to, rt, text, rt_text)

	print(query)
