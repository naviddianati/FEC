/* The main outputs of this script are:
1-  the contributor_addresses_unique table
    which is all records in contributor_addresses which have a unique (TRAN_ID,CMTE_ID) combination.
    This is the table that will be joined with tables for various states later to generate i
    <state>_addresses tables. 
2-  the contributor_addresses_identifiable table
    which contains all the records in contributor_addresses which either
    a) have a unique (TRAN_ID,CMTE_ID) pair
    or
    b) have a (TRAN_ID,CMTE_ID) pair that is shared with some other records, but only with records that are identical to it in other fields.

    These are records that we call identifiable.
*/


/*create index TRAN_INDEX on contributor_addresses (TRAN_ID,CMTE_ID);*/
/*ALTER TABLE contributor_addresses ENGINE = MyISAM;*/

SET SESSION max_heap_table_size=75000000000;
SET SESSION tmp_table_size=75000000000;
SET SESSION max_tmp_tables=200;

DROP TABLE IF EXISTS contributor_addresses_unique;
CREATE TABLE contributor_addresses_unique 
    LIKE contributor_addresses;

/*  A subset of contributor_addresses that contains all records that are clearly attributable to one person.
    It contains records with a unique id pair, as well as those that have non-unique id pairs but within each
    equivalency class of records sharing the same id pair, all clearly belong to the same person.    
*/ 
DROP TABLE IF EXISTS contributor_addresses_identifiable;
CREATE TABLE contributor_addresses_identifiable 
    LIKE contributor_addresses;





/*  A marginal superset of uniques which also includes records that have duplicate (TRAN_ID,CMTE_ID) pairs,
    but all who share the same pair, share the same other fields as well, and are clearly attributable to 
    a single person.
*/
DROP TABLE IF EXISTS identifiables;
CREATE TABLE `identifiables` (
  `TRAN_ID` varchar(32) DEFAULT '',
  `CMTE_ID` varchar(9) DEFAULT '',
  KEY `TRAN_INDEX` (`TRAN_ID`,`CMTE_ID`)
) DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS t2;
CREATE TABLE `t2` (
  `TRAN_ID` varchar(32) DEFAULT '',
  `CMTE_ID` varchar(9) DEFAULT '',
  `c`  int DEFAULT 0,

  KEY `TRAN_INDEX` (`TRAN_ID`,`CMTE_ID`)
) DEFAULT CHARSET=latin1;




DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (
  `TRAN_ID` varchar(32) DEFAULT '',
  `CMTE_ID` varchar(9) DEFAULT '',
  `CONTRIBUTOR_ZIP` varchar(9) DEFAULT '',
  `CONTRIBUTOR_CITY` varchar(30) DEFAULT '',
  `CONTRIBUTOR_STATE` varchar(2) DEFAULT '',
  `CONTRIBUTOR_STREET_1` varchar(200) DEFAULT '',
  `c`  int DEFAULT 0,
  KEY `TRAN_INDEX` (`TRAN_ID`,`CMTE_ID`)
--  KEY `ALL_INDEX` (`TRAN_ID`,`CMTE_ID`,`CONTRIBUTOR_ZIP`,`CONTRIBUTOR_STATE`,`CONTRIBUTOR_CITY`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


/*LOCK TABLES t1 WRITE,
            t2 WRITE,
            identifiables WRITE,
            contributor_addresses WRITE,
            contributor_addresses_unique WRITE,
            contributor_addresses_identifiable WRITE,
            uniques WRITE;
*/  


          
DROP TABLE IF EXISTS uniques;
CREATE TABLE `uniques` (
  `TRAN_ID` varchar(32) DEFAULT '',
  `CMTE_ID` varchar(9) DEFAULT '',
  KEY `TRAN_INDEX` (`TRAN_ID`,`CMTE_ID`)
) DEFAULT CHARSET=latin1;




INSERT INTO uniques 
         SELECT TRAN_ID,CMTE_ID  
               FROM contributor_addresses 
               GROUP BY TRAN_ID,CMTE_ID 
               HAVING count(*) = 1;




/*  NOTE:   IF (TRAN_ID,CMTE_ID pair is unique)
                COOL!
            ELSE:
                IF (all records sharing the same (TRAN_IC,CMTE_ID) have the same exact address)
                    Collapse them into one record 
                ELSE:
                    Discount all of them
*/





/* SEQUENTIAL VERSION */
-- Records that are clearly due to the same person but have duplicate id pairs

-- LOCK TABLES  contributor_addresses READ;

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED ;


SELECT "Populating t1" AS "";
INSERT INTO t1
        SELECT TRAN_ID,CMTE_ID,CONTRIBUTOR_ZIP,CONTRIBUTOR_STATE,CONTRIBUTOR_CITY  ,CONTRIBUTOR_STREET_1,count(*) as c
            FROM contributor_addresses
            WHERE (TRAN_ID!='' AND CMTE_ID!='')
            GROUP BY TRAN_ID,CMTE_ID,CONTRIBUTOR_ZIP,CONTRIBUTOR_STATE,CONTRIBUTOR_CITY  ,CONTRIBUTOR_STREET_1
            HAVING c >1;

-- Filter out those id pairs that appear in more than one person's set of records with duplicate id pairs
SELECT "Populating t2" AS "";
INSERT INTO t2
    SELECT TRAN_ID,CMTE_ID,count(*) as c
        FROM t1
        GROUP BY TRAN_ID,CMTE_ID
        HAVING c=1;


SELECT "Populating identifiables" AS "";
INSERT INTO identifiables
    SELECT * FROM uniques
        UNION
    SELECT TRAN_ID,CMTE_ID FROM t2;

/* END*/


/* INLINE VERSION: MAY RUN OUT OF TMP STORAGE */
/* INSERT INTO identifiables
    SELECT * FROM
    uniques
    UNION
    -- I believe the rows of the following will all be distinct
    SELECT TRAN_ID,CMTE_ID 
         FROM(
            -- Filter out those id pairs that appear in more than one person's set of records with duplicate id pairs
            SELECT TRAN_ID,CMTE_ID,count(*) as c1
                FROM(
                    -- Records that are clearly due to the same person but have duplicate id pairs
                    SELECT *,count(*) as c
                        FROM contributor_addresses
                        WHERE (TRAN_ID!='' AND CMTE_ID!='')
                        GROUP BY TRAN_ID,CMTE_ID,CONTRIBUTOR_ZIP,CONTRIBUTOR_ZIP,CONTRIBUTOR_STATE,CONTRIBUTOR_CITY,CONTRIBUTOR_STREET_1
                        HAVING c >1
                    ) AS t
                GROUP BY TRAN_ID,CMTE_ID
                HAVING c1 = 1
            ) AS t1;
*/







/* Use t1.* so that the output has the same column number/order ast the target table */
SELECT "Populating contributor_addresses_unique" AS "";
INSERT INTO contributor_addresses_unique 
    SELECT t1.* FROM contributor_addresses t1 
        JOIN uniques 
        USING (TRAN_ID,CMTE_ID);

-- The last group by operation is to get rid of duplicates that might result from joining with identifiables
SELECT "Populating contributor_addresses_identifiable" AS "";
INSERT INTO contributor_addresses_identifiable
    SELECT t1.* FROM contributor_addresses t1
        JOIN identifiables
        USING (TRAN_ID,CMTE_ID)
        GROUP BY TRAN_ID,CMTE_ID,CONTRIBUTOR_ZIP,CONTRIBUTOR_STATE,CONTRIBUTOR_CITY,CONTRIBUTOR_STREET_1;



COMMIT;

-- UNLOCK TABLES;

-- DROP TABLE t1;
-- DROP TABLE t2;

-- DROP TABLE uniques;
-- DROP TABLE identifiables;















