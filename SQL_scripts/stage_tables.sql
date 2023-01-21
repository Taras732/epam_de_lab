create
or replace TABLE CUSTOMERS (
    CUSTOMERSK NUMBER(38, 0) NOT NULL autoincrement,
    CUSTOMERID VARCHAR(5) NOT NULL,
    COMPANYNAME VARCHAR(40) NOT NULL,
    CONTACTNAME VARCHAR(30),
    CONTACTTITLE VARCHAR(30),
    ADDRESS VARCHAR(60),
    CITY VARCHAR(15),
    REGION VARCHAR(15),
    POSTALCODE VARCHAR(10),
    COUNTRY VARCHAR(15),
    CRECORDSTARTDATE DATE DEFAULT CURRENT_DATE(),
    primary key (CUSTOMERSK)
);
create
or replace TABLE EMPLOYEES (
    EMPLOYEESK NUMBER(38, 0) NOT NULL autoincrement,
    EMPLOYEEID NUMBER(38, 0) NOT NULL,
    LASTNAME VARCHAR(20) NOT NULL,
    FIRSTNAME VARCHAR(10) NOT NULL,
    TITLE VARCHAR(30),
    TITLEOFCOURTESY VARCHAR(25),
    BIRTHDATE DATE,
    HIREDATE DATE,
    ADDRESS VARCHAR(60),
    CITY VARCHAR(15),
    REGION VARCHAR(15),
    POSTALCODE VARCHAR(10),
    COUNTRY VARCHAR(15),
    NOTES VARCHAR(16777216),
    REPORTSTO NUMBER(38, 0),
    ERECORDSTARTDATE DATE DEFAULT CURRENT_DATE(),
    primary key (EMPLOYEESK)
);
create
or replace TABLE SUPPLIERS (
    SUPPLIERSK NUMBER(38, 0) NOT NULL autoincrement,
    SUPPLIERID NUMBER(38, 0) NOT NULL,
    COMPANYNAME VARCHAR(40) NOT NULL,
    CONTACTNAME VARCHAR(30),
    CONTACTTITLE VARCHAR(30),
    ADDRESS VARCHAR(60),
    CITY VARCHAR(15),
    REGION VARCHAR(15),
    POSTALCODE VARCHAR(10),
    COUNTRY VARCHAR(15),
    SRECORDSTARTDATE DATE DEFAULT CURRENT_DATE(),
    primary key (SUPPLIERSK)
);


create or replace TABLE ORDERDETAILS (
  ORDERID NUMBER(38,0) NOT NULL,
  PRODUCTID NUMBER(38,0) NOT NULL,
  UNITPRICE NUMBER(38,0),
  QUANTITY NUMBER(38,0),
  DISCOUNT FLOAT
);

create or replace TABLE ORDERS (
  ORDERID NUMBER(38,0) NOT NULL,
  CUSTOMERID VARCHAR(50) NOT NULL,
  EMPLOYEEID NUMBER(38,0) NOT NULL,
  ORDERDATE DATE,
  REQUIREDDATE DATE,
  SHIPPEDDATE DATE,
  SHIPVIA NUMBER(38,0),
  FREIGHT NUMBER(38,0),
  SHIPNAME VARCHAR(50),
  SHIPADDRESS VARCHAR(50),
  SHIPCITY VARCHAR(50),
  SHIPREGION VARCHAR(50),
  SHIPPOSTALCODE VARCHAR(50),
  SHIPCOUNTRY VARCHAR(50)
);

create or replace TABLE CATEGORIES (
  CATEGORIESID NUMBER(38,0),
  CATEGORYNAME VARCHAR(40) NOT NULL,
  CATEGORYDESCRIPTION VARCHAR(100) NOT NULL
);

create or replace TABLE PRODUCTS (
  PRODUCTID NUMBER(38,0) NOT NULL,
  PRODUCTNAME VARCHAR(40) NOT NULL,
  SUPPLIERID NUMBER(38,0),
  CATEGORYID NUMBER(38,0),
  QUANTITYPERUNIT VARCHAR(20),
  UNITPRICE NUMBER(38,0),
  REORDERLEVEL NUMBER(38,0),
  DISCONTINUED BOOLEAN NOT NULL,
  PRECORDSTARTDATE TIMESTAMP_NTZ(9)
);