use hive.tmp;
drop table if exists sai_test;
create table sai_test(col varchar) with (external_location='s3://dev-issp-basic/tmp/sai_test');
insert into sai_test values ('a'), ('b');
select * from sai_test;
