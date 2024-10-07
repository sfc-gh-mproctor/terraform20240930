/*

Snowflake provides a CLI (command line interface) as well as other client software (drivers, connectors, etc.) for connecting to Snowflake and using certain Snowflake features (e.g. Apache Kafka for loading data, Apache Hive metadata for external tables). The clients must be installed on each local workstation or system from which you wish to connect.
As needed, we release new versions of the clients to fix bugs, and introduce enhancements and new features. New versions are backward-compatible with existing Snowflake features, but we do not guarantee that earlier versions are forward-compatible. As such, we recommend actively monitoring and maintaining the versions of your installed clients; if they are not in-sync with the current version of Snowflake, you may encounter issues when connecting to and using Snowflake. 
https://docs.snowflake.com/release-notes/requirements


This query has to be run in a customer's account, not in snowhouse.

*/

------------------------------------------------------------------------------------------------------------

-- Filter only after a specific start_date
set start_date = dateadd(days, -10, current_date());
-------------------------------------------------------------------------------------------------------------

-- Determine which unsupported drivers have been used in connections to Snowflake

with sessions as (
select count(1) as session_count
, max(created_on) as latest_session_date
, client_application_id as driver_id
, client_application_version as driver_version
, split_part(client_application_id, ' ', 1) as driver_name
, split_part(client_application_version, '.', 1) as major
, split_part(client_application_version, '.', 2) as minor
, split_part(client_application_version, '.', 3) as patch
, user_name
, authentication_method as auth
from snowflake.account_usage.sessions
where client_environment <> '{"APPLICATION":"Snowflake Web App"}'
and created_on >= $start_date
group by all
),
base_drivers as (
select parse_json(system$client_version_info()) v
)
, minimums as (
select
f.value:clientId::string driver_name
, f.value:minimumSupportedVersion::string
, split_part(f.value:minimumSupportedVersion::string, '.', 1) major
, split_part(f.value:minimumSupportedVersion::string, '.', 2) minor
, split_part(f.value:minimumSupportedVersion::string, '.', -1) patch
from base_drivers c
, table(flatten(v)) f
)
select user_name
, auth
, session_count
, latest_session_date
, s.driver_name
, driver_version as unsupported_driver_version_used
, m.major||'.'||m.minor||'.'||m.patch as min_supported_version
from sessions s inner join minimums m on s.driver_name = m.driver_name
where (s.major < m.major)
or (s.major = m.major and s.minor < m.minor)
or (s.major = m.major and s.minor = m.minor and s.patch < m.patch)
order by user_name, driver_name, latest_session_date desc
;

