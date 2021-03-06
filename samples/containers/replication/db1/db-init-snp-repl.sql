set nocount on;

use master;
go

-- removed database if exists
if db_id('publisher_db') is not null 
begin
	exec sp_removedbreplication @dbname = 'publisher_db'
	alter database publisher_db set single_user with rollback immediate;
	drop database publisher_db;
	print convert(varchar(30), getdate(), 121) + ' - droped database publisher_db'
end
go

-- removed all jobs 
declare @job_name varchar(1000);

while exists (select 1 from msdb.dbo.sysjobs_view) 
begin
	select @job_name = name from msdb.dbo.sysjobs_view;
	exec msdb.dbo.sp_delete_job @job_name = @job_name, @delete_unused_schedule=1;
	print convert(varchar(30), getdate(), 121) + ' - deleted job ' + @job_name;
end

create database publisher_db;
go
alter database publisher_db set recovery simple;
go
print convert(varchar(30), getdate(), 121) + ' - created database publisher_db'
go

use publisher_db;
go

create schema snp;
go
print convert(varchar(30), getdate(), 121) + ' - created schema snp';
go

-- create function for generate mock stmt
create or alter function dbo.get_mock_stmt 
(
	@table_name varchar(128) -- target table name
,	@operation varchar(10) -- insert, delete, update, select_cnt
,	@quantity int -- numbers of rows
)
returns nvarchar(max)
as
begin

	return (
		select case
				when @operation = 'insert' then 
				';with n1(c) as (select 0 union all select 0)
				,	   n2(c) as (select 0 from n1 as t1 cross join n1 as t2)
				,	   n3(c) as (select 0 from n2 as t1 cross join n2 as t2)
				,	   n4(c) as (select 0 from n3 as t1 cross join n3 as t2)
				,	   n5(c) as (select 0 from n4 as t1 cross join n4 as t2)
				,	   n6(c) as (select 0 from n5 as t1 cross join n1 as t2)
				,	  dt(context) as (select newid() from n6)
				insert into ' +  @table_name + '(context)
				select top(' + cast(@quantity as varchar(10)) + ') context from dt;
				'
				when @operation = 'update' then
				'update	src
				set		src.context = newid()
					,	src.mod_date = getdate()
				from	(select top(' + cast(@quantity as varchar(10)) + ') context, mod_date from ' + @table_name + ' order by context) src;'
				when @operation = 'delete' then
				'delete src from	( select top(' + cast(@quantity as varchar(10)) + ') id from ' + @table_name + ') src;'
				when @operation = 'select_cnt' then 
				'select count(*) from ' + @table_name
			   end
		)	
end;
go
print convert(varchar(30), getdate(), 121) + ' - created function dbo.get_mock_stmt';
go

-- create mock_stmt_log table
create table dbo.mock_stmt_log
(
	id bigint identity(1,1) not null primary key
,	msg nvarchar(max)
,	create_date datetime default getdate()
)
go
print convert(varchar(30), getdate(), 121) + ' - created table dbo.mock_stmt_log';
go

-- crate mock_stmt_caller for call mock_stmt with pseudo random operation and data quantity
create or alter procedure dbo.mock_stmt_caller
(
	@table_name varchar(128) -- target table name
,	@operation varchar(10) = null -- insert, delete, update, select_cnt, if null then random
,	@quantity int = null -- numbers of rows, if null then random
,	@max_random_quantity int = 1000 -- maximum number of rows for random
	
)
as
begin
	set nocount on;
	set xact_abort on;

	declare @stmt nvarchar(max);
	declare @row_cnt int;
	declare @log_msg nvarchar(max);
	declare @operations table ( operation varchar(10));

	set @max_random_quantity =  iif(@max_random_quantity is null, 100, abs(@max_random_quantity));
	
	insert into @operations
	select 'insert' union all select 'delete' union all select 'update' union all select 'select_cnt';

	-- set random value
	select @operation = ( select top(1) operation from @operations order by newid()	) where @operation is null
	select @quantity = ( select abs(cast(crypt_gen_random(4, cast(@operation as varbinary(20))) as int)) % @max_random_quantity ) where @quantity is null;

	set @stmt = dbo.get_mock_stmt(@table_name, @operation, @quantity);

	begin try
		exec sp_executesql @stmt;
		set @row_cnt = @@rowcount;
		insert into dbo.mock_stmt_log(msg) select @table_name + ': Successfull ' + @operation + isnull(cast(@row_cnt as varchar(10)),' null') + ' of ' + cast(@quantity as varchar(10)); 
	end try
	begin catch
		insert into dbo.mock_stmt_log(msg) select @table_name + ': Unsuccessfull ' + @operation + ' err_msg: ' + error_message(); 
	end catch
	
	return 0;

end
go
print convert(varchar(30), getdate(), 121) + ' - created procedure dbo.mock_stmt_caller';
go

-- create procedure to exec mock_stmt_caller in loop with waitfor delay
create or alter procedure dbo.mock_stmt_caller_loop
(
	@table_name varchar(128) -- target table name
,	@delay_sec int = null -- waitfor delay in second (max is 60), if null then random from 1 to 5 sec
,	@operation varchar(10) = null -- insert, delete, update, select_cnt, if null then random
,	@quantity int = null -- numbers of rows, if null then random
,	@max_random_quantity int = 1000 -- maximum number of rows for random
	
)
as
begin
	set nocount on;
	set xact_abort on;

	declare @waitfor_delay varchar(10);
	set @delay_sec = iif(@delay_sec is null, -999, @delay_sec);

	while 1 = 1
	begin
		
		select @delay_sec = (select abs(cast(crypt_gen_random(4, null) as int)) % 6) where @delay_sec = -999;
		set @waitfor_delay = '00:00:' + right('0' + cast(iif(@delay_sec = 0, 1, @delay_sec) as varchar(10)), 2);
	
		exec dbo.mock_stmt_caller @table_name, @operation, @quantity, @max_random_quantity;
		waitfor delay @waitfor_delay

	end

	return 0
end
go
print convert(varchar(30), getdate(), 121) + ' - created procedure dbo.mock_stmt_caller_loop';
go

use master
go
-- creating replication objects
declare @distributor sysname = @@servername;
declare @distributor_login sysname = N'sa';
declare @distributor_password sysname = N'MssqlPass123';
-- check is distributor exists
declare @sp_get_distributor table (is_installed int,distribution_server_name varchar(500),is_distribution_db_installed int,is_distribution_publisher int,has_remote_distribution_publisher int);
insert @sp_get_distributor exec master.sys.sp_get_distributor
-- drop distributor
if exists (select 1 from @sp_get_distributor where is_installed = 1)
exec sp_dropdistributor @no_checks = 1, @ignore_distributor=1;
-- create distributor
exec sp_adddistributor @distributor = @distributor;
-- create distributor database
exec sp_adddistributiondb 
	 @database = N'distributor_db'
	,@log_file_size = 2
	,@deletebatchsize_xact = 5000
	,@deletebatchsize_cmd = 2000
	,@security_mode = 0
	,@login = @distributor_login
	,@password = @distributor_password;
go
print convert(varchar(30), getdate(), 121) + ' - created distributor database distributor_db';
go

use publisher_db
go
-- adding the distrubution publisher
declare @publisher sysname = @@servername;
declare @distributor_login sysname = N'sa';
declare @distributor_password sysname = N'MssqlPass123';
exec sp_adddistpublisher @publisher         = @publisher
,                        @distribution_db   = N'distributor_db'
,                        @security_mode     = 0
,                        @login             = @distributor_login
,                        @password          = @distributor_password
,                        @working_directory = N'/var/opt/mssql/ReplData'
,                        @trusted           = N'false'
,                        @thirdparty_flag   = 0
,                        @publisher_type    = N'MSSQLSERVER'
go
print convert(varchar(30), getdate(), 121) + ' - added distributor to publisher';
go

use publisher_db
go
-- adding the snapshot replication
declare @distributor sysname = @@servername;
declare @publisher_login sysname = N'sa';
declare @publisher_password sysname = N'MssqlPass123';

exec sp_replicationdboption @dbname = N'publisher_db', @optname = N'publish', @value = N'true'

exec sp_addpublication
	@publication = N'snp_repl_publisher_db', 
	@description = N'Snapshot publication of database ''publisher_db'' from Publisher ''<PUBLISHER HOSTNAME>''.',
	@retention = 0, 
	@allow_push = N'true', 
	@repl_freq = N'snapshot', 
	@status = N'active', 
	@independent_agent = N'true'

exec sp_addpublication_snapshot 
	@publication = N'snp_repl_publisher_db', 
	@frequency_type = 128, 
	@frequency_interval = 8, 
	@frequency_relative_interval = 1, 
	@frequency_recurrence_factor = 0, 
	@frequency_subday = 4, 
	@frequency_subday_interval = 2, 
	@active_start_time_of_day = 0,
	@active_end_time_of_day = 235959, 
	@active_start_date = 0, 
	@active_end_date = 0, 
	@publisher_security_mode = 0, 
	@publisher_login = @publisher_login, 
	@publisher_password = @publisher_password
go
print convert(varchar(30), getdate(), 121) + ' - added snapshot replication';
go

use publisher_db;
go
-- create objects 
declare @snp_repl_table_qty int = 3;
declare @snp_table_name_prefix varchar(100) = 'tab_';
declare @snp_table_name varchar(200);
declare @cnt int = 0;
declare @stmt nvarchar(max);
declare @job_name sysname;
declare @job_id binary(16)
declare @step_name sysname;
declare @publication varchar(100) = 'snp_repl_publisher_db';
declare @schema_option binary(8) = 0x000000000003409;
declare @table_schema_name sysname;
declare @table_name sysname;

set @cnt = 0
while ( @cnt < @snp_repl_table_qty )
begin
	
	set @snp_table_name = 'snp.' + @snp_table_name_prefix + cast(@cnt as varchar(10));
	-- create table
	set @stmt = 
	'create table ' + @snp_table_name + ' (id bigint identity(1,1), context uniqueidentifier default newid(), mod_date datetime default getdate());';
	if object_id(@snp_table_name) is null 
	begin
		exec (@stmt);
		print convert(varchar(30), getdate(), 121) + ' - created table ' + @snp_table_name;
	end
		
	set @table_schema_name = object_schema_name(object_id(@snp_table_name));
	set @table_name = object_name(object_id(@snp_table_name))
	
	-- insert data to table
	set @stmt =
	'
	;with n1(c) as (select 0 union all select 0)
	,	  n2(c) as (select 0 from n1 as t1 cross join n1 as t2)
	,	  n3(c) as (select 0 from n2 as t1 cross join n2 as t2)
	,	  n4(c) as (select 0 from n3 as t1 cross join n3 as t2)
	,	  n5(c) as (select 0 from n4 as t1 cross join n4 as t2)
	,	  n6(c) as (select 0 from n5 as t1 cross join n1 as t2)
	,	  dt(context) as (select newid() from n6)
	insert into ' +  @snp_table_name + '(context)
	select context from dt;
	'
	print convert(varchar(30), getdate(), 121) + ' - added data to table ' + @snp_table_name;
	
	exec (@stmt);	
	
	-- create jobs to mock stmt generator
	set @job_id = null;
	set @job_name = 'publisher_db:';
	set @job_name += @snp_table_name + '_mock_stmt_caller_loop';
	set @step_name = @snp_table_name + '_mock_stmt_caller_loop';
	set @stmt = 'exec dbo.mock_stmt_caller_loop @table_name = ' + '''' + @snp_table_name + '''';
	-- drop if exists
	if exists (select 1 from msdb.dbo.sysjobs_view where name = @job_name)
	exec msdb.dbo.sp_delete_job @job_name = @job_name, @delete_unused_schedule=1;
	-- create job
	exec msdb.dbo.sp_add_job @job_name=@job_name, 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', @job_id = @job_id output;
	-- add step to job
	exec msdb.dbo.sp_add_jobstep @job_id=@job_id, @step_name=@step_name, 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=@stmt, 
		@database_name=N'publisher_db', 
		@flags=0;	
	-- add job to server
	exec msdb.dbo.sp_add_jobserver @job_name = @job_name;
	-- start job 
	exec msdb.dbo.sp_start_job @job_name = @job_name;
	print convert(varchar(30), getdate(), 121) + ' - created job ' + @job_name;
	
	-- adding aricles to replication with generated tables
	exec publisher_db.sys.sp_addarticle 
		@publication = @publication, 
		@article = @snp_table_name, 
		@source_owner = @table_schema_name, 
		@source_object = @table_name, 
		@type = N'logbased', 
		@description = null, 
		@creation_script = null, 
		@pre_creation_cmd = N'drop', 
		@schema_option = @schema_option,
		@identityrangemanagementoption = N'manual', 
		@destination_table = @table_name, 
		@destination_owner = @table_schema_name, 
		@vertical_partition = N'false';
	print convert(varchar(30), getdate(), 121) + ' - added artice for table ' + @snp_table_name;
	
	set @cnt += 1;		 
end;
go

use publisher_db;
go

declare @subscriber sysname = N'db2';
declare @subscriber_db sysname = N'subscriber_db';
declare @subscriberLogin sysname = N'sa';
declare @subscriberPassword sysname =  N'MssqlPass123'
declare @publication varchar(100) = 'snp_repl_publisher_db';

exec sp_addsubscription 
	@publication = @publication, 
	@subscriber = @subscriber,
	@destination_db = @subscriber_db, 
	@subscription_type = N'Push', 
	@sync_type = N'automatic', 
	@article = N'all', 
	@update_mode = N'read only', 
	@subscriber_type = 0

exec sp_addpushsubscription_agent 
	@publication = @publication, 
	@subscriber = @subscriber,
	@subscriber_db = @subscriber_db, 
	@subscriber_security_mode = 0, 
	@subscriber_login =  @subscriberLogin,
	@subscriber_password =  @subscriberPassword,
	@frequency_type = 128, 
	@frequency_interval = 8, 
	@frequency_relative_interval = 0, 
	@frequency_recurrence_factor = 0, 
	@frequency_subday = 4, 
	@frequency_subday_interval = 2, 
	@active_start_time_of_day = 0, 
	@active_end_time_of_day = 0, 
	@active_start_date = 0, 
	@active_end_date = 19950101
go
print convert(varchar(30), getdate(), 121) + ' - created substriction';
go

declare @publication varchar(100) = 'snp_repl_publisher_db';
exec sp_startpublication_snapshot 
	@publication = @publication, 
	@publisher = NULL
go 
print convert(varchar(30), getdate(), 121) + ' - creating snapshot';
/*
select s.*
from msdb.dbo.sysjobs s inner join msdb.dbo.syscategories c on s.category_id = c.category_id
where c.name in ('REPL-Distribution')
*/