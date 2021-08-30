with recursive columns as (
    select
		row_number() over () as column_id,
		t.table_name as table_name,
		t.table_schema as table_schema,
		c.column_name as column_name,
		c.data_type as column_data_type,
		c.ordinal_position as column_order
	from information_schema.tables t
	join information_schema.columns c using (table_schema, table_name)
), primary_keys as (
	select
		tc.table_name,
		tc.table_schema,
		ku.column_name
	from information_schema.table_constraints as tc
	join information_schema.key_column_usage ku using (constraint_schema, constraint_name)
	where tc.constraint_type = 'PRIMARY KEY'
), foreign_keys as (
	select
		tc.table_name,
		tc.table_schema,
		ku.column_name,
		c.column_id as references_column
	from information_schema.table_constraints as tc
	join information_schema.key_column_usage ku using (constraint_schema, constraint_name)
	join information_schema.constraint_column_usage as ccu using (constraint_schema, constraint_name)
	join columns c on c.table_schema = ccu.table_schema and c.table_name = ccu.table_name and c.column_name = ccu.column_name
	where tc.constraint_type = 'FOREIGN KEY'
)
select
	c.column_id as id,
	c.table_schema || '.' || c.table_name as table_name,
	c.column_name,
	c.column_data_type,
	c.column_order,
	pk.column_name is not null as primary_key,
	fk.references_column as column_reference
from columns as c
left join primary_keys pk using (table_schema, table_name, column_name)
left join foreign_keys fk using (table_schema, table_name, column_name)
where c.table_schema = 'public'
order by c.table_name, c.column_order
