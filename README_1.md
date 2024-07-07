# Проект 3-го спринта

### Скрипты изменения и создания объектов БД, миграции данных в новую структуру в migrations
### Итоговый DAG "project_sprint3.py" расположен в папке src/dags
### Были реализованы процедуры очистки и заполнения таблиц таким образом, чтобы было возможно независимое удаление и восстановление информации за отдельные дни без затрагивания информации за другие дни.

Добавлено новое поле status в staging.user_order_log и mart.f_sales Скрипт add_status_uol_f_sales.sql
--добавляем поле статус со значением по умолчанию shipped
ALTER TABLE staging.user_order_log ADD column status VARCHAR(20) default 'shipped' not null;

--добавляем поле status в витрину 
ALTER TABLE mart.f_sales ADD column status VARCHAR(20) default 'shipped';

Модифицируем файл, чтобы в витрине mart.f_sales учитывались статусы shipped и refunded. Значениям в полях quantity и payment_amount проставляется минус, если статус refunded.
Добавляем проверку на дубли и их удаление.

delete from mart.f_sales
where f_sales.date_id in
    (select date_id from mart.d_calendar where mart.d_calendar.date_actual = '{{ds}}');

insert into mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount, status)
select
    date_id, item_id, customer_id, city_id, quantity, payment_amount, status
from
    (
    select date_id, item_id, customer_id, city_id, quantity, payment_amount, status,    
        MD5((date_id,item_id,customer_id,city_id,quantity,payment_amount,status)::text) as uniq_id
    from
        (
        select
            dc.date_id as date_id, 
            uol.item_id as item_id,
            uol.customer_id as customer_id,
            uol.city_id as city_id,
            uol.quantity * (case when uol.status = 'refunded' then -1 else 1 end) quantity,
            uol.payment_amount * (case when uol.status = 'refunded' then -1 else 1 end) payment_amount,  
            uol.status as status
        from staging.user_order_log uol
        left join mart.d_calendar as dc
            on uol.date_time::Date = dc.date_actual
        where uol.date_time::Date = '{{ds}}'
        ) as t1
    ) as t2
where uniq_id not in (select MD5((date_id,item_id,customer_id,city_id,quantity,payment_amount,status)::text) as uniq_id
                      from mart.f_sales);

### Добавляю новую витрину скриптом create_f_customer_retention.sql
--создание новой витрины mart.f_customer_retention
drop table if exists mart.f_customer_retention;
create table mart.f_customer_retention (
	id serial4 PRIMARY KEY, 
    new_customers_count int4 not null, 
    returning_customers_count int4 not null, 
    refunded_customer_count int4 not null, 
    period_name VARCHAR(30) not null, 
    period_id int4 not null, 
    item_id int4 not null, 
    new_customers_revenue numeric(12,2) not null, 
    returning_customers_revenue numeric(12,2) not null,
    customers_refunded int4 not null);

comment on column mart.f_customer_retention.new_customers_count is 'кол-во новых клиентов (тех, которые сделали только один заказ за рассматриваемый промежуток времени)';
comment on column mart.f_customer_retention.returning_customers_count is 'кол-во вернувшихся клиентов (тех, которые сделали только несколько заказов за рассматриваемый промежуток времени).' ;
comment on column mart.f_customer_retention.refunded_customer_count is 'кол-во клиентов, оформивших возврат за рассматриваемый промежуток времени.' ;
comment on column mart.f_customer_retention.period_name is 'weekly' ;
comment on column mart.f_customer_retention.period_id is 'идентификатор периода (номер недели или номер месяца).' ;
comment on column mart.f_customer_retention.item_id is 'идентификатор категории товара.' ;
comment on column mart.f_customer_retention.new_customers_revenue is 'доход с новых клиентов' ;
comment on column mart.f_customer_retention.returning_customers_revenue is 'доход с вернувшихся клиентов';
comment on column mart.f_customer_retention.customers_refunded is 'количество возвратов клиентов';

### Реализован скрипт для инкрементов:
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


### В DAG модифицирована соответствующая задача и порядок выполнения:
   update_f_customer_retention = PostgresOperator(
        task_id='update_f_customer_retention',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_customer_retention.sql",
        parameters={"date": {business_dt}})

    (
            generate_report
            >> get_report
            >> get_increment
            >> upload_user_order_inc
            >> [update_d_item_table, update_d_city_table, update_d_customer_table]
            >> update_f_sales
            >> update_f_customer_retention
    )

