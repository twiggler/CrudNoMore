insert into public.t1 (id, document) values (10, 1);
insert into public.t1 (id, document) values(11, 1);
insert into public.t2 (id, t1) values (20, 10);
insert into public.u (id, t1, t2, data) values (30, 10, 20, 'Data1');
insert into public.u (id, t1, t2, data) values (31, 10, 20, 'Data2');

insert into public.t1 (id, document) values (110, 2);
insert into public.t2 (id, t1) values (120, 110);
insert into public.u (id, t1, t2, data) values (130, 110, 120, 'Data21');
