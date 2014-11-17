insert overwrite local directory 'french-debate'
row format delimited 
fields terminated by "\t"
select id_str, created_at, regexp_replace(text, "[ \t\r\n]+", " "), user.id_str, regexp_replace(user.name, "[ \t\r\n]+", " "), user.screen_name, retweeted_status.id_str, retweeted_status.created_at, regexp_replace(retweeted_status.text, "[ \t\r\n]+", " "), retweeted_status.user.id_str, regexp_replace(retweeted_status.user.name, "[ \t\r\n]+", " "), retweeted_status.user.screen_name from gh_rc 
where year = 2012 and month = 5 and (day = 2 or day = 3) and 
(lower(text) like '%sarkozy%' or lower(text) like '%holland%' or lower(retweeted_status.text) like '%sarkozy%' or lower(retweeted_status.text) like '%holland%');