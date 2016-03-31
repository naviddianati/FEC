USE FEC;
drop table if exists washington;
create table washington like individual_contributions_v2;
insert into washington select * from individual_contributions_v2 where STATE='WA';
delete from washington where TRAN_ID="" or CMTE_ID="";
delete from washington using washington join  (select TRAN_ID,CMTE_ID,count(*) as c  from washington  group by TRAN_ID,CMTE_ID  having c>1 ) t on (washington.TRAN_ID=t.TRAN_ID and washington.CMTE_ID=t.CMTE_ID);
drop table if exists washington_addresses_v2;
create table washington_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from washington as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists washington;

USE FEC;
drop table if exists wisconsin;
create table wisconsin like individual_contributions_v2;
insert into wisconsin select * from individual_contributions_v2 where STATE='WI';
delete from wisconsin where TRAN_ID="" or CMTE_ID="";
delete from wisconsin using wisconsin join  (select TRAN_ID,CMTE_ID,count(*) as c  from wisconsin  group by TRAN_ID,CMTE_ID  having c>1 ) t on (wisconsin.TRAN_ID=t.TRAN_ID and wisconsin.CMTE_ID=t.CMTE_ID);
drop table if exists wisconsin_addresses_v2;
create table wisconsin_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from wisconsin as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists wisconsin;

USE FEC;
drop table if exists westvirginia;
create table westvirginia like individual_contributions_v2;
insert into westvirginia select * from individual_contributions_v2 where STATE='WV';
delete from westvirginia where TRAN_ID="" or CMTE_ID="";
delete from westvirginia using westvirginia join  (select TRAN_ID,CMTE_ID,count(*) as c  from westvirginia  group by TRAN_ID,CMTE_ID  having c>1 ) t on (westvirginia.TRAN_ID=t.TRAN_ID and westvirginia.CMTE_ID=t.CMTE_ID);
drop table if exists westvirginia_addresses_v2;
create table westvirginia_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from westvirginia as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists westvirginia;

USE FEC;
drop table if exists florida;
create table florida like individual_contributions_v2;
insert into florida select * from individual_contributions_v2 where STATE='FL';
delete from florida where TRAN_ID="" or CMTE_ID="";
delete from florida using florida join  (select TRAN_ID,CMTE_ID,count(*) as c  from florida  group by TRAN_ID,CMTE_ID  having c>1 ) t on (florida.TRAN_ID=t.TRAN_ID and florida.CMTE_ID=t.CMTE_ID);
drop table if exists florida_addresses_v2;
create table florida_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from florida as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists florida;

USE FEC;
drop table if exists federatedstatesofmicronesia;
create table federatedstatesofmicronesia like individual_contributions_v2;
insert into federatedstatesofmicronesia select * from individual_contributions_v2 where STATE='FM';
delete from federatedstatesofmicronesia where TRAN_ID="" or CMTE_ID="";
delete from federatedstatesofmicronesia using federatedstatesofmicronesia join  (select TRAN_ID,CMTE_ID,count(*) as c  from federatedstatesofmicronesia  group by TRAN_ID,CMTE_ID  having c>1 ) t on (federatedstatesofmicronesia.TRAN_ID=t.TRAN_ID and federatedstatesofmicronesia.CMTE_ID=t.CMTE_ID);
drop table if exists federatedstatesofmicronesia_addresses_v2;
create table federatedstatesofmicronesia_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from federatedstatesofmicronesia as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists federatedstatesofmicronesia;

USE FEC;
drop table if exists wyoming;
create table wyoming like individual_contributions_v2;
insert into wyoming select * from individual_contributions_v2 where STATE='WY';
delete from wyoming where TRAN_ID="" or CMTE_ID="";
delete from wyoming using wyoming join  (select TRAN_ID,CMTE_ID,count(*) as c  from wyoming  group by TRAN_ID,CMTE_ID  having c>1 ) t on (wyoming.TRAN_ID=t.TRAN_ID and wyoming.CMTE_ID=t.CMTE_ID);
drop table if exists wyoming_addresses_v2;
create table wyoming_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from wyoming as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists wyoming;

USE FEC;
drop table if exists newhampshire;
create table newhampshire like individual_contributions_v2;
insert into newhampshire select * from individual_contributions_v2 where STATE='NH';
delete from newhampshire where TRAN_ID="" or CMTE_ID="";
delete from newhampshire using newhampshire join  (select TRAN_ID,CMTE_ID,count(*) as c  from newhampshire  group by TRAN_ID,CMTE_ID  having c>1 ) t on (newhampshire.TRAN_ID=t.TRAN_ID and newhampshire.CMTE_ID=t.CMTE_ID);
drop table if exists newhampshire_addresses_v2;
create table newhampshire_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from newhampshire as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists newhampshire;

USE FEC;
drop table if exists newjersey;
create table newjersey like individual_contributions_v2;
insert into newjersey select * from individual_contributions_v2 where STATE='NJ';
delete from newjersey where TRAN_ID="" or CMTE_ID="";
delete from newjersey using newjersey join  (select TRAN_ID,CMTE_ID,count(*) as c  from newjersey  group by TRAN_ID,CMTE_ID  having c>1 ) t on (newjersey.TRAN_ID=t.TRAN_ID and newjersey.CMTE_ID=t.CMTE_ID);
drop table if exists newjersey_addresses_v2;
create table newjersey_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from newjersey as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists newjersey;

USE FEC;
drop table if exists newmexico;
create table newmexico like individual_contributions_v2;
insert into newmexico select * from individual_contributions_v2 where STATE='NM';
delete from newmexico where TRAN_ID="" or CMTE_ID="";
delete from newmexico using newmexico join  (select TRAN_ID,CMTE_ID,count(*) as c  from newmexico  group by TRAN_ID,CMTE_ID  having c>1 ) t on (newmexico.TRAN_ID=t.TRAN_ID and newmexico.CMTE_ID=t.CMTE_ID);
drop table if exists newmexico_addresses_v2;
create table newmexico_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from newmexico as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists newmexico;

USE FEC;
drop table if exists northcarolina;
create table northcarolina like individual_contributions_v2;
insert into northcarolina select * from individual_contributions_v2 where STATE='NC';
delete from northcarolina where TRAN_ID="" or CMTE_ID="";
delete from northcarolina using northcarolina join  (select TRAN_ID,CMTE_ID,count(*) as c  from northcarolina  group by TRAN_ID,CMTE_ID  having c>1 ) t on (northcarolina.TRAN_ID=t.TRAN_ID and northcarolina.CMTE_ID=t.CMTE_ID);
drop table if exists northcarolina_addresses_v2;
create table northcarolina_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from northcarolina as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists northcarolina;

USE FEC;
drop table if exists northdakota;
create table northdakota like individual_contributions_v2;
insert into northdakota select * from individual_contributions_v2 where STATE='ND';
delete from northdakota where TRAN_ID="" or CMTE_ID="";
delete from northdakota using northdakota join  (select TRAN_ID,CMTE_ID,count(*) as c  from northdakota  group by TRAN_ID,CMTE_ID  having c>1 ) t on (northdakota.TRAN_ID=t.TRAN_ID and northdakota.CMTE_ID=t.CMTE_ID);
drop table if exists northdakota_addresses_v2;
create table northdakota_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from northdakota as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists northdakota;

USE FEC;
drop table if exists nebraska;
create table nebraska like individual_contributions_v2;
insert into nebraska select * from individual_contributions_v2 where STATE='NE';
delete from nebraska where TRAN_ID="" or CMTE_ID="";
delete from nebraska using nebraska join  (select TRAN_ID,CMTE_ID,count(*) as c  from nebraska  group by TRAN_ID,CMTE_ID  having c>1 ) t on (nebraska.TRAN_ID=t.TRAN_ID and nebraska.CMTE_ID=t.CMTE_ID);
drop table if exists nebraska_addresses_v2;
create table nebraska_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from nebraska as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists nebraska;

USE FEC;
drop table if exists newyork;
create table newyork like individual_contributions_v2;
insert into newyork select * from individual_contributions_v2 where STATE='NY';
delete from newyork where TRAN_ID="" or CMTE_ID="";
delete from newyork using newyork join  (select TRAN_ID,CMTE_ID,count(*) as c  from newyork  group by TRAN_ID,CMTE_ID  having c>1 ) t on (newyork.TRAN_ID=t.TRAN_ID and newyork.CMTE_ID=t.CMTE_ID);
drop table if exists newyork_addresses_v2;
create table newyork_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from newyork as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists newyork;

USE FEC;
drop table if exists rhodeisland;
create table rhodeisland like individual_contributions_v2;
insert into rhodeisland select * from individual_contributions_v2 where STATE='RI';
delete from rhodeisland where TRAN_ID="" or CMTE_ID="";
delete from rhodeisland using rhodeisland join  (select TRAN_ID,CMTE_ID,count(*) as c  from rhodeisland  group by TRAN_ID,CMTE_ID  having c>1 ) t on (rhodeisland.TRAN_ID=t.TRAN_ID and rhodeisland.CMTE_ID=t.CMTE_ID);
drop table if exists rhodeisland_addresses_v2;
create table rhodeisland_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from rhodeisland as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists rhodeisland;

USE FEC;
drop table if exists nevada;
create table nevada like individual_contributions_v2;
insert into nevada select * from individual_contributions_v2 where STATE='NV';
delete from nevada where TRAN_ID="" or CMTE_ID="";
delete from nevada using nevada join  (select TRAN_ID,CMTE_ID,count(*) as c  from nevada  group by TRAN_ID,CMTE_ID  having c>1 ) t on (nevada.TRAN_ID=t.TRAN_ID and nevada.CMTE_ID=t.CMTE_ID);
drop table if exists nevada_addresses_v2;
create table nevada_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from nevada as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists nevada;

USE FEC;
drop table if exists guam;
create table guam like individual_contributions_v2;
insert into guam select * from individual_contributions_v2 where STATE='GU';
delete from guam where TRAN_ID="" or CMTE_ID="";
delete from guam using guam join  (select TRAN_ID,CMTE_ID,count(*) as c  from guam  group by TRAN_ID,CMTE_ID  having c>1 ) t on (guam.TRAN_ID=t.TRAN_ID and guam.CMTE_ID=t.CMTE_ID);
drop table if exists guam_addresses_v2;
create table guam_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from guam as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists guam;

USE FEC;
drop table if exists colorado;
create table colorado like individual_contributions_v2;
insert into colorado select * from individual_contributions_v2 where STATE='CO';
delete from colorado where TRAN_ID="" or CMTE_ID="";
delete from colorado using colorado join  (select TRAN_ID,CMTE_ID,count(*) as c  from colorado  group by TRAN_ID,CMTE_ID  having c>1 ) t on (colorado.TRAN_ID=t.TRAN_ID and colorado.CMTE_ID=t.CMTE_ID);
drop table if exists colorado_addresses_v2;
create table colorado_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from colorado as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists colorado;

USE FEC;
drop table if exists california;
create table california like individual_contributions_v2;
insert into california select * from individual_contributions_v2 where STATE='CA';
delete from california where TRAN_ID="" or CMTE_ID="";
delete from california using california join  (select TRAN_ID,CMTE_ID,count(*) as c  from california  group by TRAN_ID,CMTE_ID  having c>1 ) t on (california.TRAN_ID=t.TRAN_ID and california.CMTE_ID=t.CMTE_ID);
drop table if exists california_addresses_v2;
create table california_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from california as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists california;

USE FEC;
drop table if exists georgia;
create table georgia like individual_contributions_v2;
insert into georgia select * from individual_contributions_v2 where STATE='GA';
delete from georgia where TRAN_ID="" or CMTE_ID="";
delete from georgia using georgia join  (select TRAN_ID,CMTE_ID,count(*) as c  from georgia  group by TRAN_ID,CMTE_ID  having c>1 ) t on (georgia.TRAN_ID=t.TRAN_ID and georgia.CMTE_ID=t.CMTE_ID);
drop table if exists georgia_addresses_v2;
create table georgia_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from georgia as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists georgia;

USE FEC;
drop table if exists connecticut;
create table connecticut like individual_contributions_v2;
insert into connecticut select * from individual_contributions_v2 where STATE='CT';
delete from connecticut where TRAN_ID="" or CMTE_ID="";
delete from connecticut using connecticut join  (select TRAN_ID,CMTE_ID,count(*) as c  from connecticut  group by TRAN_ID,CMTE_ID  having c>1 ) t on (connecticut.TRAN_ID=t.TRAN_ID and connecticut.CMTE_ID=t.CMTE_ID);
drop table if exists connecticut_addresses_v2;
create table connecticut_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from connecticut as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists connecticut;

USE FEC;
drop table if exists oklahoma;
create table oklahoma like individual_contributions_v2;
insert into oklahoma select * from individual_contributions_v2 where STATE='OK';
delete from oklahoma where TRAN_ID="" or CMTE_ID="";
delete from oklahoma using oklahoma join  (select TRAN_ID,CMTE_ID,count(*) as c  from oklahoma  group by TRAN_ID,CMTE_ID  having c>1 ) t on (oklahoma.TRAN_ID=t.TRAN_ID and oklahoma.CMTE_ID=t.CMTE_ID);
drop table if exists oklahoma_addresses_v2;
create table oklahoma_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from oklahoma as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists oklahoma;

USE FEC;
drop table if exists ohio;
create table ohio like individual_contributions_v2;
insert into ohio select * from individual_contributions_v2 where STATE='OH';
delete from ohio where TRAN_ID="" or CMTE_ID="";
delete from ohio using ohio join  (select TRAN_ID,CMTE_ID,count(*) as c  from ohio  group by TRAN_ID,CMTE_ID  having c>1 ) t on (ohio.TRAN_ID=t.TRAN_ID and ohio.CMTE_ID=t.CMTE_ID);
drop table if exists ohio_addresses_v2;
create table ohio_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from ohio as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists ohio;

USE FEC;
drop table if exists kansas;
create table kansas like individual_contributions_v2;
insert into kansas select * from individual_contributions_v2 where STATE='KS';
delete from kansas where TRAN_ID="" or CMTE_ID="";
delete from kansas using kansas join  (select TRAN_ID,CMTE_ID,count(*) as c  from kansas  group by TRAN_ID,CMTE_ID  having c>1 ) t on (kansas.TRAN_ID=t.TRAN_ID and kansas.CMTE_ID=t.CMTE_ID);
drop table if exists kansas_addresses_v2;
create table kansas_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from kansas as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists kansas;

USE FEC;
drop table if exists southcarolina;
create table southcarolina like individual_contributions_v2;
insert into southcarolina select * from individual_contributions_v2 where STATE='SC';
delete from southcarolina where TRAN_ID="" or CMTE_ID="";
delete from southcarolina using southcarolina join  (select TRAN_ID,CMTE_ID,count(*) as c  from southcarolina  group by TRAN_ID,CMTE_ID  having c>1 ) t on (southcarolina.TRAN_ID=t.TRAN_ID and southcarolina.CMTE_ID=t.CMTE_ID);
drop table if exists southcarolina_addresses_v2;
create table southcarolina_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from southcarolina as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists southcarolina;

USE FEC;
drop table if exists kentucky;
create table kentucky like individual_contributions_v2;
insert into kentucky select * from individual_contributions_v2 where STATE='KY';
delete from kentucky where TRAN_ID="" or CMTE_ID="";
delete from kentucky using kentucky join  (select TRAN_ID,CMTE_ID,count(*) as c  from kentucky  group by TRAN_ID,CMTE_ID  having c>1 ) t on (kentucky.TRAN_ID=t.TRAN_ID and kentucky.CMTE_ID=t.CMTE_ID);
drop table if exists kentucky_addresses_v2;
create table kentucky_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from kentucky as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists kentucky;

USE FEC;
drop table if exists oregon;
create table oregon like individual_contributions_v2;
insert into oregon select * from individual_contributions_v2 where STATE='OR';
delete from oregon where TRAN_ID="" or CMTE_ID="";
delete from oregon using oregon join  (select TRAN_ID,CMTE_ID,count(*) as c  from oregon  group by TRAN_ID,CMTE_ID  having c>1 ) t on (oregon.TRAN_ID=t.TRAN_ID and oregon.CMTE_ID=t.CMTE_ID);
drop table if exists oregon_addresses_v2;
create table oregon_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from oregon as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists oregon;

USE FEC;
drop table if exists southdakota;
create table southdakota like individual_contributions_v2;
insert into southdakota select * from individual_contributions_v2 where STATE='SD';
delete from southdakota where TRAN_ID="" or CMTE_ID="";
delete from southdakota using southdakota join  (select TRAN_ID,CMTE_ID,count(*) as c  from southdakota  group by TRAN_ID,CMTE_ID  having c>1 ) t on (southdakota.TRAN_ID=t.TRAN_ID and southdakota.CMTE_ID=t.CMTE_ID);
drop table if exists southdakota_addresses_v2;
create table southdakota_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from southdakota as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists southdakota;

USE FEC;
drop table if exists delaware;
create table delaware like individual_contributions_v2;
insert into delaware select * from individual_contributions_v2 where STATE='DE';
delete from delaware where TRAN_ID="" or CMTE_ID="";
delete from delaware using delaware join  (select TRAN_ID,CMTE_ID,count(*) as c  from delaware  group by TRAN_ID,CMTE_ID  having c>1 ) t on (delaware.TRAN_ID=t.TRAN_ID and delaware.CMTE_ID=t.CMTE_ID);
drop table if exists delaware_addresses_v2;
create table delaware_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from delaware as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists delaware;

USE FEC;
drop table if exists districtofcolumbia;
create table districtofcolumbia like individual_contributions_v2;
insert into districtofcolumbia select * from individual_contributions_v2 where STATE='DC';
delete from districtofcolumbia where TRAN_ID="" or CMTE_ID="";
delete from districtofcolumbia using districtofcolumbia join  (select TRAN_ID,CMTE_ID,count(*) as c  from districtofcolumbia  group by TRAN_ID,CMTE_ID  having c>1 ) t on (districtofcolumbia.TRAN_ID=t.TRAN_ID and districtofcolumbia.CMTE_ID=t.CMTE_ID);
drop table if exists districtofcolumbia_addresses_v2;
create table districtofcolumbia_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from districtofcolumbia as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists districtofcolumbia;

USE FEC;
drop table if exists hawaii;
create table hawaii like individual_contributions_v2;
insert into hawaii select * from individual_contributions_v2 where STATE='HI';
delete from hawaii where TRAN_ID="" or CMTE_ID="";
delete from hawaii using hawaii join  (select TRAN_ID,CMTE_ID,count(*) as c  from hawaii  group by TRAN_ID,CMTE_ID  having c>1 ) t on (hawaii.TRAN_ID=t.TRAN_ID and hawaii.CMTE_ID=t.CMTE_ID);
drop table if exists hawaii_addresses_v2;
create table hawaii_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from hawaii as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists hawaii;

USE FEC;
drop table if exists puertorico;
create table puertorico like individual_contributions_v2;
insert into puertorico select * from individual_contributions_v2 where STATE='PR';
delete from puertorico where TRAN_ID="" or CMTE_ID="";
delete from puertorico using puertorico join  (select TRAN_ID,CMTE_ID,count(*) as c  from puertorico  group by TRAN_ID,CMTE_ID  having c>1 ) t on (puertorico.TRAN_ID=t.TRAN_ID and puertorico.CMTE_ID=t.CMTE_ID);
drop table if exists puertorico_addresses_v2;
create table puertorico_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from puertorico as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists puertorico;

USE FEC;
drop table if exists palau;
create table palau like individual_contributions_v2;
insert into palau select * from individual_contributions_v2 where STATE='PW';
delete from palau where TRAN_ID="" or CMTE_ID="";
delete from palau using palau join  (select TRAN_ID,CMTE_ID,count(*) as c  from palau  group by TRAN_ID,CMTE_ID  having c>1 ) t on (palau.TRAN_ID=t.TRAN_ID and palau.CMTE_ID=t.CMTE_ID);
drop table if exists palau_addresses_v2;
create table palau_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from palau as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists palau;

USE FEC;
drop table if exists texas;
create table texas like individual_contributions_v2;
insert into texas select * from individual_contributions_v2 where STATE='TX';
delete from texas where TRAN_ID="" or CMTE_ID="";
delete from texas using texas join  (select TRAN_ID,CMTE_ID,count(*) as c  from texas  group by TRAN_ID,CMTE_ID  having c>1 ) t on (texas.TRAN_ID=t.TRAN_ID and texas.CMTE_ID=t.CMTE_ID);
drop table if exists texas_addresses_v2;
create table texas_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from texas as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists texas;

USE FEC;
drop table if exists louisiana;
create table louisiana like individual_contributions_v2;
insert into louisiana select * from individual_contributions_v2 where STATE='LA';
delete from louisiana where TRAN_ID="" or CMTE_ID="";
delete from louisiana using louisiana join  (select TRAN_ID,CMTE_ID,count(*) as c  from louisiana  group by TRAN_ID,CMTE_ID  having c>1 ) t on (louisiana.TRAN_ID=t.TRAN_ID and louisiana.CMTE_ID=t.CMTE_ID);
drop table if exists louisiana_addresses_v2;
create table louisiana_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from louisiana as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists louisiana;

USE FEC;
drop table if exists tennessee;
create table tennessee like individual_contributions_v2;
insert into tennessee select * from individual_contributions_v2 where STATE='TN';
delete from tennessee where TRAN_ID="" or CMTE_ID="";
delete from tennessee using tennessee join  (select TRAN_ID,CMTE_ID,count(*) as c  from tennessee  group by TRAN_ID,CMTE_ID  having c>1 ) t on (tennessee.TRAN_ID=t.TRAN_ID and tennessee.CMTE_ID=t.CMTE_ID);
drop table if exists tennessee_addresses_v2;
create table tennessee_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from tennessee as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists tennessee;

USE FEC;
drop table if exists pennsylvania;
create table pennsylvania like individual_contributions_v2;
insert into pennsylvania select * from individual_contributions_v2 where STATE='PA';
delete from pennsylvania where TRAN_ID="" or CMTE_ID="";
delete from pennsylvania using pennsylvania join  (select TRAN_ID,CMTE_ID,count(*) as c  from pennsylvania  group by TRAN_ID,CMTE_ID  having c>1 ) t on (pennsylvania.TRAN_ID=t.TRAN_ID and pennsylvania.CMTE_ID=t.CMTE_ID);
drop table if exists pennsylvania_addresses_v2;
create table pennsylvania_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from pennsylvania as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists pennsylvania;

USE FEC;
drop table if exists virginia;
create table virginia like individual_contributions_v2;
insert into virginia select * from individual_contributions_v2 where STATE='VA';
delete from virginia where TRAN_ID="" or CMTE_ID="";
delete from virginia using virginia join  (select TRAN_ID,CMTE_ID,count(*) as c  from virginia  group by TRAN_ID,CMTE_ID  having c>1 ) t on (virginia.TRAN_ID=t.TRAN_ID and virginia.CMTE_ID=t.CMTE_ID);
drop table if exists virginia_addresses_v2;
create table virginia_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from virginia as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists virginia;

USE FEC;
drop table if exists virginislands;
create table virginislands like individual_contributions_v2;
insert into virginislands select * from individual_contributions_v2 where STATE='VI';
delete from virginislands where TRAN_ID="" or CMTE_ID="";
delete from virginislands using virginislands join  (select TRAN_ID,CMTE_ID,count(*) as c  from virginislands  group by TRAN_ID,CMTE_ID  having c>1 ) t on (virginislands.TRAN_ID=t.TRAN_ID and virginislands.CMTE_ID=t.CMTE_ID);
drop table if exists virginislands_addresses_v2;
create table virginislands_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from virginislands as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists virginislands;

USE FEC;
drop table if exists alaska;
create table alaska like individual_contributions_v2;
insert into alaska select * from individual_contributions_v2 where STATE='AK';
delete from alaska where TRAN_ID="" or CMTE_ID="";
delete from alaska using alaska join  (select TRAN_ID,CMTE_ID,count(*) as c  from alaska  group by TRAN_ID,CMTE_ID  having c>1 ) t on (alaska.TRAN_ID=t.TRAN_ID and alaska.CMTE_ID=t.CMTE_ID);
drop table if exists alaska_addresses_v2;
create table alaska_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from alaska as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists alaska;

USE FEC;
drop table if exists alabama;
create table alabama like individual_contributions_v2;
insert into alabama select * from individual_contributions_v2 where STATE='AL';
delete from alabama where TRAN_ID="" or CMTE_ID="";
delete from alabama using alabama join  (select TRAN_ID,CMTE_ID,count(*) as c  from alabama  group by TRAN_ID,CMTE_ID  having c>1 ) t on (alabama.TRAN_ID=t.TRAN_ID and alabama.CMTE_ID=t.CMTE_ID);
drop table if exists alabama_addresses_v2;
create table alabama_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from alabama as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists alabama;

USE FEC;
drop table if exists americansamoa;
create table americansamoa like individual_contributions_v2;
insert into americansamoa select * from individual_contributions_v2 where STATE='AS';
delete from americansamoa where TRAN_ID="" or CMTE_ID="";
delete from americansamoa using americansamoa join  (select TRAN_ID,CMTE_ID,count(*) as c  from americansamoa  group by TRAN_ID,CMTE_ID  having c>1 ) t on (americansamoa.TRAN_ID=t.TRAN_ID and americansamoa.CMTE_ID=t.CMTE_ID);
drop table if exists americansamoa_addresses_v2;
create table americansamoa_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from americansamoa as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists americansamoa;

USE FEC;
drop table if exists arkansas;
create table arkansas like individual_contributions_v2;
insert into arkansas select * from individual_contributions_v2 where STATE='AR';
delete from arkansas where TRAN_ID="" or CMTE_ID="";
delete from arkansas using arkansas join  (select TRAN_ID,CMTE_ID,count(*) as c  from arkansas  group by TRAN_ID,CMTE_ID  having c>1 ) t on (arkansas.TRAN_ID=t.TRAN_ID and arkansas.CMTE_ID=t.CMTE_ID);
drop table if exists arkansas_addresses_v2;
create table arkansas_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from arkansas as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists arkansas;

USE FEC;
drop table if exists vermont;
create table vermont like individual_contributions_v2;
insert into vermont select * from individual_contributions_v2 where STATE='VT';
delete from vermont where TRAN_ID="" or CMTE_ID="";
delete from vermont using vermont join  (select TRAN_ID,CMTE_ID,count(*) as c  from vermont  group by TRAN_ID,CMTE_ID  having c>1 ) t on (vermont.TRAN_ID=t.TRAN_ID and vermont.CMTE_ID=t.CMTE_ID);
drop table if exists vermont_addresses_v2;
create table vermont_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from vermont as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists vermont;

USE FEC;
drop table if exists illinois;
create table illinois like individual_contributions_v2;
insert into illinois select * from individual_contributions_v2 where STATE='IL';
delete from illinois where TRAN_ID="" or CMTE_ID="";
delete from illinois using illinois join  (select TRAN_ID,CMTE_ID,count(*) as c  from illinois  group by TRAN_ID,CMTE_ID  having c>1 ) t on (illinois.TRAN_ID=t.TRAN_ID and illinois.CMTE_ID=t.CMTE_ID);
drop table if exists illinois_addresses_v2;
create table illinois_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from illinois as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists illinois;

USE FEC;
drop table if exists indiana;
create table indiana like individual_contributions_v2;
insert into indiana select * from individual_contributions_v2 where STATE='IN';
delete from indiana where TRAN_ID="" or CMTE_ID="";
delete from indiana using indiana join  (select TRAN_ID,CMTE_ID,count(*) as c  from indiana  group by TRAN_ID,CMTE_ID  having c>1 ) t on (indiana.TRAN_ID=t.TRAN_ID and indiana.CMTE_ID=t.CMTE_ID);
drop table if exists indiana_addresses_v2;
create table indiana_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from indiana as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists indiana;

USE FEC;
drop table if exists iowa;
create table iowa like individual_contributions_v2;
insert into iowa select * from individual_contributions_v2 where STATE='IA';
delete from iowa where TRAN_ID="" or CMTE_ID="";
delete from iowa using iowa join  (select TRAN_ID,CMTE_ID,count(*) as c  from iowa  group by TRAN_ID,CMTE_ID  having c>1 ) t on (iowa.TRAN_ID=t.TRAN_ID and iowa.CMTE_ID=t.CMTE_ID);
drop table if exists iowa_addresses_v2;
create table iowa_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from iowa as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists iowa;

USE FEC;
drop table if exists arizona;
create table arizona like individual_contributions_v2;
insert into arizona select * from individual_contributions_v2 where STATE='AZ';
delete from arizona where TRAN_ID="" or CMTE_ID="";
delete from arizona using arizona join  (select TRAN_ID,CMTE_ID,count(*) as c  from arizona  group by TRAN_ID,CMTE_ID  having c>1 ) t on (arizona.TRAN_ID=t.TRAN_ID and arizona.CMTE_ID=t.CMTE_ID);
drop table if exists arizona_addresses_v2;
create table arizona_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from arizona as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists arizona;

USE FEC;
drop table if exists idaho;
create table idaho like individual_contributions_v2;
insert into idaho select * from individual_contributions_v2 where STATE='ID';
delete from idaho where TRAN_ID="" or CMTE_ID="";
delete from idaho using idaho join  (select TRAN_ID,CMTE_ID,count(*) as c  from idaho  group by TRAN_ID,CMTE_ID  having c>1 ) t on (idaho.TRAN_ID=t.TRAN_ID and idaho.CMTE_ID=t.CMTE_ID);
drop table if exists idaho_addresses_v2;
create table idaho_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from idaho as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists idaho;

USE FEC;
drop table if exists maine;
create table maine like individual_contributions_v2;
insert into maine select * from individual_contributions_v2 where STATE='ME';
delete from maine where TRAN_ID="" or CMTE_ID="";
delete from maine using maine join  (select TRAN_ID,CMTE_ID,count(*) as c  from maine  group by TRAN_ID,CMTE_ID  having c>1 ) t on (maine.TRAN_ID=t.TRAN_ID and maine.CMTE_ID=t.CMTE_ID);
drop table if exists maine_addresses_v2;
create table maine_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from maine as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists maine;

USE FEC;
drop table if exists maryland;
create table maryland like individual_contributions_v2;
insert into maryland select * from individual_contributions_v2 where STATE='MD';
delete from maryland where TRAN_ID="" or CMTE_ID="";
delete from maryland using maryland join  (select TRAN_ID,CMTE_ID,count(*) as c  from maryland  group by TRAN_ID,CMTE_ID  having c>1 ) t on (maryland.TRAN_ID=t.TRAN_ID and maryland.CMTE_ID=t.CMTE_ID);
drop table if exists maryland_addresses_v2;
create table maryland_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from maryland as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists maryland;

USE FEC;
drop table if exists massachusetts;
create table massachusetts like individual_contributions_v2;
insert into massachusetts select * from individual_contributions_v2 where STATE='MA';
delete from massachusetts where TRAN_ID="" or CMTE_ID="";
delete from massachusetts using massachusetts join  (select TRAN_ID,CMTE_ID,count(*) as c  from massachusetts  group by TRAN_ID,CMTE_ID  having c>1 ) t on (massachusetts.TRAN_ID=t.TRAN_ID and massachusetts.CMTE_ID=t.CMTE_ID);
drop table if exists massachusetts_addresses_v2;
create table massachusetts_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from massachusetts as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists massachusetts;

USE FEC;
drop table if exists utah;
create table utah like individual_contributions_v2;
insert into utah select * from individual_contributions_v2 where STATE='UT';
delete from utah where TRAN_ID="" or CMTE_ID="";
delete from utah using utah join  (select TRAN_ID,CMTE_ID,count(*) as c  from utah  group by TRAN_ID,CMTE_ID  having c>1 ) t on (utah.TRAN_ID=t.TRAN_ID and utah.CMTE_ID=t.CMTE_ID);
drop table if exists utah_addresses_v2;
create table utah_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from utah as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists utah;

USE FEC;
drop table if exists missouri;
create table missouri like individual_contributions_v2;
insert into missouri select * from individual_contributions_v2 where STATE='MO';
delete from missouri where TRAN_ID="" or CMTE_ID="";
delete from missouri using missouri join  (select TRAN_ID,CMTE_ID,count(*) as c  from missouri  group by TRAN_ID,CMTE_ID  having c>1 ) t on (missouri.TRAN_ID=t.TRAN_ID and missouri.CMTE_ID=t.CMTE_ID);
drop table if exists missouri_addresses_v2;
create table missouri_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from missouri as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists missouri;

USE FEC;
drop table if exists minnesota;
create table minnesota like individual_contributions_v2;
insert into minnesota select * from individual_contributions_v2 where STATE='MN';
delete from minnesota where TRAN_ID="" or CMTE_ID="";
delete from minnesota using minnesota join  (select TRAN_ID,CMTE_ID,count(*) as c  from minnesota  group by TRAN_ID,CMTE_ID  having c>1 ) t on (minnesota.TRAN_ID=t.TRAN_ID and minnesota.CMTE_ID=t.CMTE_ID);
drop table if exists minnesota_addresses_v2;
create table minnesota_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from minnesota as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists minnesota;

USE FEC;
drop table if exists michigan;
create table michigan like individual_contributions_v2;
insert into michigan select * from individual_contributions_v2 where STATE='MI';
delete from michigan where TRAN_ID="" or CMTE_ID="";
delete from michigan using michigan join  (select TRAN_ID,CMTE_ID,count(*) as c  from michigan  group by TRAN_ID,CMTE_ID  having c>1 ) t on (michigan.TRAN_ID=t.TRAN_ID and michigan.CMTE_ID=t.CMTE_ID);
drop table if exists michigan_addresses_v2;
create table michigan_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from michigan as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists michigan;

USE FEC;
drop table if exists marshallislands;
create table marshallislands like individual_contributions_v2;
insert into marshallislands select * from individual_contributions_v2 where STATE='MH';
delete from marshallislands where TRAN_ID="" or CMTE_ID="";
delete from marshallislands using marshallislands join  (select TRAN_ID,CMTE_ID,count(*) as c  from marshallislands  group by TRAN_ID,CMTE_ID  having c>1 ) t on (marshallislands.TRAN_ID=t.TRAN_ID and marshallislands.CMTE_ID=t.CMTE_ID);
drop table if exists marshallislands_addresses_v2;
create table marshallislands_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from marshallislands as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists marshallislands;

USE FEC;
drop table if exists montana;
create table montana like individual_contributions_v2;
insert into montana select * from individual_contributions_v2 where STATE='MT';
delete from montana where TRAN_ID="" or CMTE_ID="";
delete from montana using montana join  (select TRAN_ID,CMTE_ID,count(*) as c  from montana  group by TRAN_ID,CMTE_ID  having c>1 ) t on (montana.TRAN_ID=t.TRAN_ID and montana.CMTE_ID=t.CMTE_ID);
drop table if exists montana_addresses_v2;
create table montana_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from montana as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists montana;

USE FEC;
drop table if exists northernmarianaislands;
create table northernmarianaislands like individual_contributions_v2;
insert into northernmarianaislands select * from individual_contributions_v2 where STATE='MP';
delete from northernmarianaislands where TRAN_ID="" or CMTE_ID="";
delete from northernmarianaislands using northernmarianaislands join  (select TRAN_ID,CMTE_ID,count(*) as c  from northernmarianaislands  group by TRAN_ID,CMTE_ID  having c>1 ) t on (northernmarianaislands.TRAN_ID=t.TRAN_ID and northernmarianaislands.CMTE_ID=t.CMTE_ID);
drop table if exists northernmarianaislands_addresses_v2;
create table northernmarianaislands_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from northernmarianaislands as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists northernmarianaislands;

USE FEC;
drop table if exists mississippi;
create table mississippi like individual_contributions_v2;
insert into mississippi select * from individual_contributions_v2 where STATE='MS';
delete from mississippi where TRAN_ID="" or CMTE_ID="";
delete from mississippi using mississippi join  (select TRAN_ID,CMTE_ID,count(*) as c  from mississippi  group by TRAN_ID,CMTE_ID  having c>1 ) t on (mississippi.TRAN_ID=t.TRAN_ID and mississippi.CMTE_ID=t.CMTE_ID);
drop table if exists mississippi_addresses_v2;
create table mississippi_addresses_v2 (unique key TRAN_INDEX(TRAN_ID,CMTE_ID)) select * from mississippi as t1  join contributor_addresses_identifiable as t2 using (CMTE_ID,TRAN_ID);
drop table if exists mississippi;

