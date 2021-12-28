select json_build_object(
	'public.t1', coalesce(json_agg((json_build_object('document', public.t1.document, 'id', public.t1.id))) FILTER (WHERE public.t1.id IS NOT NULL), '[]'),
	'public.t2', coalesce(json_agg((json_build_object('id', public.t2.id, 't1', public.t2.t1))) FILTER (WHERE public.t2.id IS NOT NULL), '[]'),
	'public.u', coalesce(json_agg((json_build_object('data', public.u.data, 'id', public.u.id, 't1', public.u.t1, 't2', public.u.t2))) FILTER (WHERE public.u.id IS NOT NULL), '[]')) as document
from public.t1
left join public.t2 on (public.t2.t1 = public.t1.id)
left join public.u on (public.u.t2 = public.t2.id)