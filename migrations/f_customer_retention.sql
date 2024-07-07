delete from mart.f_customer_retention where f_customer_retention.period_id = (select week_of_year from mart.d_calendar where d_calendar.date_actual = '{{ds}}' ) ;

insert into mart.f_customer_retention (period_id, period_name, item_id,
                                       new_customers_count, returning_customers_count, refunded_customer_count,
                                       new_customers_revenue, returning_customers_revenue,
									   customers_refunded)
select
	period_id, period_name, item_id,
    new_customers_count, returning_customers_count, refunded_customer_count,
    new_customers_revenue, returning_customers_revenue,
    customers_refunded
from
	(
	select
		period_id,
		period_name,
		item_id,
		count(*) filter(where status = 'shipped' and orders = 1) as new_customers_count,
		count(*) filter(where status = 'shipped' and orders > 1) as returning_customers_count,
		count(*) filter(where status = 'refunded') as refunded_customer_count,
		sum(case when status = 'shipped' and orders = 1 then sum_payment_amount else 0 end) as new_customers_revenue,
		sum(case when status = 'shipped' and orders > 1 then sum_payment_amount else 0 end) as returning_customers_revenue,
		coalesce(sum(orders) filter(where status = 'refunded'), 0) as customers_refunded,
		MD5((period_id,period_name)::text) as uniq_id
	from 
		(
		select
		    period_id,
		    period_name,
		    item_id,
		    customer_id,
		    status,
		    count(*) as orders,
		    sum(payment_amount) as sum_payment_amount
		from
		    (
		    select uol.*, date_part('week', uol.date_time::timestamp) as period_id,
		    		to_char(uol.date_time::timestamp, 'YYYY-WW') as period_name
		    from staging.user_order_log uol
            where uol.date_time::Date = '{{ds}}'
		    ) as t1
		group by period_id, period_name, item_id, customer_id, status
		) as t2
	group by period_id, period_name, item_id
	) as t3
where uniq_id not in (select MD5((period_id,period_name)::text) as uniq_id from mart.f_customer_retention);

