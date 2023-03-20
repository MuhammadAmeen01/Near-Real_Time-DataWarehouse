select s.supplier_name, p.product_name, d.quater, d.month, sum(total_sale)
from dwh.facttable as t, dwh.date as d, dwh.product as p, dwh.supplier as s
where t.product_id=p.product_id and t.supplier_id=s.supplier_id and t.DATE=d.date and d.month=9 and d.year=2017
group by supplier_name, p.product_name, d.quater, d.month order by sum(Total_sale) DESC limit 3;

select s.store_name, p.product_name, sum(total_sale)
from dwh.facttable as t, dwh.product as p, dwh.store as s
where t.product_id=p.product_id and t.store_id=s.store_id
group by store_name, product_name order by sum(Total_sale) limit 10;

select su.SUPPLIER_NAME,d.quater,d.month,t.Total_sale from
dwh.facttable as t , dwh.supplier as su,dwh.date as d
where t.SUPPLIER_ID=su.SUPPLIER_ID and t.DATE=d.date
group by SUPPLIER_NAME, quater, month;

select su.STORE_NAME,p.PRODUCT_NAME,sum(t.Total_sale) from
dwh.facttable as t , dwh.store as su,dwh.product as p
where t.STORE_ID=su.STORE_ID and t.PRODUCT_ID=p.PRODUCT_ID
group by STORE_NAME,PRODUCT_NAME;


select p.PRODUCT_NAME,sum(t.Total_sale),d.day from
dwh.facttable as t , dwh.date as d,dwh.product as p
where t.PRODUCT_ID=p.PRODUCT_ID and t.DATE=d.date and d.day='Sunday' or d.day='Saturday'
group by PRODUCT_NAME order by sum(t.Total_sale) DESC;


select p.PRODUCT_NAME,sum(t.Total_sale),d.day from
dwh.facttable as t , dwh.date as d,dwh.product as p
where t.PRODUCT_ID=p.PRODUCT_ID and t.DATE=d.date and (d.day='Saturday' or d.day='Sunday')
group by PRODUCT_NAME order by sum(t.Total_sale) desc limit 5;

select su.STORE_NAME,p.PRODUCT_NAME, s.SUPPLIER_NAME,sum(t.Total_sale) as totalSale from
dwh.facttable as t , dwh.store as su,dwh.product as p,dwh.supplier as s
where t.STORE_ID=su.STORE_ID and t.PRODUCT_ID=p.PRODUCT_ID and s.SUPPLIER_ID=t.SUPPLIER_ID
group by STORE_NAME with rollup;


select p.PRODUCT_NAME,sum(t.Total_sale),d.year,d.quater from
dwh.facttable as t , dwh.date as d,dwh.product as p
where t.PRODUCT_ID=p.PRODUCT_ID and t.DATE=d.date and d.year=2017 and (d.quater=1 or  d.quater=2)
group by PRODUCT_NAME order by sum(t.Total_sale);



drop table dwh.“STORE_PRODUCT_ANALYSIS”;
create table dwh.“STORE_PRODUCT_ANALYSIS” select su.STORE_NAME,p.PRODUCT_NAME,t.Total_sale from
dwh.facttable as t , dwh.store as su,dwh.product as p
where t.STORE_ID=su.STORE_ID and t.PRODUCT_ID=p.PRODUCT_ID
group by STORE_NAME,PRODUCT_NAME;


select * from dwh.“STORE_PRODUCT_ANALYSIS”;