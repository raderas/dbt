creat table "hodinkee".public."data__dbt_tmp"
as(

with __dbt__CTE__data_ab1 as (
-- SQL MODEL to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
  jsonb_extract_path(_airbyte_data,'updated') as "updated",
  jsonb_extract_path(_airbyte_data,'list_name') as "list_name",
  _airbyte_emitted_at
from "hodinkee".public._airbyte_raw_data
-- data
), __dbt__CTE_data_ab2 as (

--SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
  cast("updated" as string) as "updated",
  cast("list_name" as string) as "list_name",
  _airbyte_emitted_at
from __dbt__CTE__data_ab1
-- data
) -- Final base SQL model
select
  "updated",
  "list_name",
  airbyte_emitted_at
from __dbt__CTE__data_ab2
-- data from "hodinkee".public.airbyte_raw_data
);
