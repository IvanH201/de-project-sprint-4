
--добавляем поле статус со значением по умолчанию shipped
ALTER TABLE staging.user_order_log ADD column status VARCHAR(20) default 'shipped' not null;

--добавляем поле status в витрину 
ALTER TABLE mart.f_sales ADD column status VARCHAR(20) default 'shipped';