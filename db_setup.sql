create database source;
use source;
create table payment_otc(payment_type int NOT NULL, payment_name varchar(255) NOT NULL);

INSERT INTO payment_otc VALUES(1, 'Cash');
INSERT INTO payment_otc VALUES(2, 'Net Banking');
INSERT INTO payment_otc VALUES(3, 'UPI');
INSERT INTO payment_otc VALUES(4, 'Net Banking');
INSERT INTO payment_otc VALUES(5, 'Debit Card');

create database output;