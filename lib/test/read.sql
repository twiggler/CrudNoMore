select json_build_object(
	'public.t1', coalesce(json_agg((json_build_object('document', public.t1.document, 'id', public.t1.id))), '[]'),
	'public.t2', coalesce(json_agg((json_build_object('id', public.t2.id, 't1', public.t2.t1))), '[]'),
	'public.u', coalesce(json_agg((json_build_object('data', public.u.data, 'id', public.u.id, 't1', public.u.t1, 't2', public.u.t2))), '[]')) as document
from public.document
left join public.t1 on (public.t1.document = public.document.id)
left join public.t2 on (public.t2.t1 = public.t1.id)
left join public.u on (public.u.t2 = public.t2.id)
where public.t1.document = 1