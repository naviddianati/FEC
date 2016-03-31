USE FEC;
drop table if exists washington_full_v2;
create table washington_full_v2 like individual_contributions_v2;
insert into washington_full_v2 select * from individual_contributions_v2 where STATE='WA';

USE FEC;
drop table if exists wisconsin_full_v2;
create table wisconsin_full_v2 like individual_contributions_v2;
insert into wisconsin_full_v2 select * from individual_contributions_v2 where STATE='WI';

USE FEC;
drop table if exists westvirginia_full_v2;
create table westvirginia_full_v2 like individual_contributions_v2;
insert into westvirginia_full_v2 select * from individual_contributions_v2 where STATE='WV';

USE FEC;
drop table if exists florida_full_v2;
create table florida_full_v2 like individual_contributions_v2;
insert into florida_full_v2 select * from individual_contributions_v2 where STATE='FL';

USE FEC;
drop table if exists federatedstatesofmicronesia_full_v2;
create table federatedstatesofmicronesia_full_v2 like individual_contributions_v2;
insert into federatedstatesofmicronesia_full_v2 select * from individual_contributions_v2 where STATE='FM';

USE FEC;
drop table if exists wyoming_full_v2;
create table wyoming_full_v2 like individual_contributions_v2;
insert into wyoming_full_v2 select * from individual_contributions_v2 where STATE='WY';

USE FEC;
drop table if exists newhampshire_full_v2;
create table newhampshire_full_v2 like individual_contributions_v2;
insert into newhampshire_full_v2 select * from individual_contributions_v2 where STATE='NH';

USE FEC;
drop table if exists newjersey_full_v2;
create table newjersey_full_v2 like individual_contributions_v2;
insert into newjersey_full_v2 select * from individual_contributions_v2 where STATE='NJ';

USE FEC;
drop table if exists newmexico_full_v2;
create table newmexico_full_v2 like individual_contributions_v2;
insert into newmexico_full_v2 select * from individual_contributions_v2 where STATE='NM';

USE FEC;
drop table if exists northcarolina_full_v2;
create table northcarolina_full_v2 like individual_contributions_v2;
insert into northcarolina_full_v2 select * from individual_contributions_v2 where STATE='NC';

USE FEC;
drop table if exists northdakota_full_v2;
create table northdakota_full_v2 like individual_contributions_v2;
insert into northdakota_full_v2 select * from individual_contributions_v2 where STATE='ND';

USE FEC;
drop table if exists nebraska_full_v2;
create table nebraska_full_v2 like individual_contributions_v2;
insert into nebraska_full_v2 select * from individual_contributions_v2 where STATE='NE';

USE FEC;
drop table if exists newyork_full_v2;
create table newyork_full_v2 like individual_contributions_v2;
insert into newyork_full_v2 select * from individual_contributions_v2 where STATE='NY';

USE FEC;
drop table if exists rhodeisland_full_v2;
create table rhodeisland_full_v2 like individual_contributions_v2;
insert into rhodeisland_full_v2 select * from individual_contributions_v2 where STATE='RI';

USE FEC;
drop table if exists nevada_full_v2;
create table nevada_full_v2 like individual_contributions_v2;
insert into nevada_full_v2 select * from individual_contributions_v2 where STATE='NV';

USE FEC;
drop table if exists guam_full_v2;
create table guam_full_v2 like individual_contributions_v2;
insert into guam_full_v2 select * from individual_contributions_v2 where STATE='GU';

USE FEC;
drop table if exists colorado_full_v2;
create table colorado_full_v2 like individual_contributions_v2;
insert into colorado_full_v2 select * from individual_contributions_v2 where STATE='CO';

USE FEC;
drop table if exists california_full_v2;
create table california_full_v2 like individual_contributions_v2;
insert into california_full_v2 select * from individual_contributions_v2 where STATE='CA';

USE FEC;
drop table if exists georgia_full_v2;
create table georgia_full_v2 like individual_contributions_v2;
insert into georgia_full_v2 select * from individual_contributions_v2 where STATE='GA';

USE FEC;
drop table if exists connecticut_full_v2;
create table connecticut_full_v2 like individual_contributions_v2;
insert into connecticut_full_v2 select * from individual_contributions_v2 where STATE='CT';

USE FEC;
drop table if exists oklahoma_full_v2;
create table oklahoma_full_v2 like individual_contributions_v2;
insert into oklahoma_full_v2 select * from individual_contributions_v2 where STATE='OK';

USE FEC;
drop table if exists ohio_full_v2;
create table ohio_full_v2 like individual_contributions_v2;
insert into ohio_full_v2 select * from individual_contributions_v2 where STATE='OH';

USE FEC;
drop table if exists kansas_full_v2;
create table kansas_full_v2 like individual_contributions_v2;
insert into kansas_full_v2 select * from individual_contributions_v2 where STATE='KS';

USE FEC;
drop table if exists southcarolina_full_v2;
create table southcarolina_full_v2 like individual_contributions_v2;
insert into southcarolina_full_v2 select * from individual_contributions_v2 where STATE='SC';

USE FEC;
drop table if exists kentucky_full_v2;
create table kentucky_full_v2 like individual_contributions_v2;
insert into kentucky_full_v2 select * from individual_contributions_v2 where STATE='KY';

USE FEC;
drop table if exists oregon_full_v2;
create table oregon_full_v2 like individual_contributions_v2;
insert into oregon_full_v2 select * from individual_contributions_v2 where STATE='OR';

USE FEC;
drop table if exists southdakota_full_v2;
create table southdakota_full_v2 like individual_contributions_v2;
insert into southdakota_full_v2 select * from individual_contributions_v2 where STATE='SD';

USE FEC;
drop table if exists delaware_full_v2;
create table delaware_full_v2 like individual_contributions_v2;
insert into delaware_full_v2 select * from individual_contributions_v2 where STATE='DE';

USE FEC;
drop table if exists districtofcolumbia_full_v2;
create table districtofcolumbia_full_v2 like individual_contributions_v2;
insert into districtofcolumbia_full_v2 select * from individual_contributions_v2 where STATE='DC';

USE FEC;
drop table if exists hawaii_full_v2;
create table hawaii_full_v2 like individual_contributions_v2;
insert into hawaii_full_v2 select * from individual_contributions_v2 where STATE='HI';

USE FEC;
drop table if exists puertorico_full_v2;
create table puertorico_full_v2 like individual_contributions_v2;
insert into puertorico_full_v2 select * from individual_contributions_v2 where STATE='PR';

USE FEC;
drop table if exists palau_full_v2;
create table palau_full_v2 like individual_contributions_v2;
insert into palau_full_v2 select * from individual_contributions_v2 where STATE='PW';

USE FEC;
drop table if exists texas_full_v2;
create table texas_full_v2 like individual_contributions_v2;
insert into texas_full_v2 select * from individual_contributions_v2 where STATE='TX';

USE FEC;
drop table if exists louisiana_full_v2;
create table louisiana_full_v2 like individual_contributions_v2;
insert into louisiana_full_v2 select * from individual_contributions_v2 where STATE='LA';

USE FEC;
drop table if exists tennessee_full_v2;
create table tennessee_full_v2 like individual_contributions_v2;
insert into tennessee_full_v2 select * from individual_contributions_v2 where STATE='TN';

USE FEC;
drop table if exists pennsylvania_full_v2;
create table pennsylvania_full_v2 like individual_contributions_v2;
insert into pennsylvania_full_v2 select * from individual_contributions_v2 where STATE='PA';

USE FEC;
drop table if exists virginia_full_v2;
create table virginia_full_v2 like individual_contributions_v2;
insert into virginia_full_v2 select * from individual_contributions_v2 where STATE='VA';

USE FEC;
drop table if exists virginislands_full_v2;
create table virginislands_full_v2 like individual_contributions_v2;
insert into virginislands_full_v2 select * from individual_contributions_v2 where STATE='VI';

USE FEC;
drop table if exists alaska_full_v2;
create table alaska_full_v2 like individual_contributions_v2;
insert into alaska_full_v2 select * from individual_contributions_v2 where STATE='AK';

USE FEC;
drop table if exists alabama_full_v2;
create table alabama_full_v2 like individual_contributions_v2;
insert into alabama_full_v2 select * from individual_contributions_v2 where STATE='AL';

USE FEC;
drop table if exists americansamoa_full_v2;
create table americansamoa_full_v2 like individual_contributions_v2;
insert into americansamoa_full_v2 select * from individual_contributions_v2 where STATE='AS';

USE FEC;
drop table if exists arkansas_full_v2;
create table arkansas_full_v2 like individual_contributions_v2;
insert into arkansas_full_v2 select * from individual_contributions_v2 where STATE='AR';

USE FEC;
drop table if exists vermont_full_v2;
create table vermont_full_v2 like individual_contributions_v2;
insert into vermont_full_v2 select * from individual_contributions_v2 where STATE='VT';

USE FEC;
drop table if exists illinois_full_v2;
create table illinois_full_v2 like individual_contributions_v2;
insert into illinois_full_v2 select * from individual_contributions_v2 where STATE='IL';

USE FEC;
drop table if exists indiana_full_v2;
create table indiana_full_v2 like individual_contributions_v2;
insert into indiana_full_v2 select * from individual_contributions_v2 where STATE='IN';

USE FEC;
drop table if exists iowa_full_v2;
create table iowa_full_v2 like individual_contributions_v2;
insert into iowa_full_v2 select * from individual_contributions_v2 where STATE='IA';

USE FEC;
drop table if exists arizona_full_v2;
create table arizona_full_v2 like individual_contributions_v2;
insert into arizona_full_v2 select * from individual_contributions_v2 where STATE='AZ';

USE FEC;
drop table if exists idaho_full_v2;
create table idaho_full_v2 like individual_contributions_v2;
insert into idaho_full_v2 select * from individual_contributions_v2 where STATE='ID';

USE FEC;
drop table if exists maine_full_v2;
create table maine_full_v2 like individual_contributions_v2;
insert into maine_full_v2 select * from individual_contributions_v2 where STATE='ME';

USE FEC;
drop table if exists maryland_full_v2;
create table maryland_full_v2 like individual_contributions_v2;
insert into maryland_full_v2 select * from individual_contributions_v2 where STATE='MD';

USE FEC;
drop table if exists massachusetts_full_v2;
create table massachusetts_full_v2 like individual_contributions_v2;
insert into massachusetts_full_v2 select * from individual_contributions_v2 where STATE='MA';

USE FEC;
drop table if exists utah_full_v2;
create table utah_full_v2 like individual_contributions_v2;
insert into utah_full_v2 select * from individual_contributions_v2 where STATE='UT';

USE FEC;
drop table if exists missouri_full_v2;
create table missouri_full_v2 like individual_contributions_v2;
insert into missouri_full_v2 select * from individual_contributions_v2 where STATE='MO';

USE FEC;
drop table if exists minnesota_full_v2;
create table minnesota_full_v2 like individual_contributions_v2;
insert into minnesota_full_v2 select * from individual_contributions_v2 where STATE='MN';

USE FEC;
drop table if exists michigan_full_v2;
create table michigan_full_v2 like individual_contributions_v2;
insert into michigan_full_v2 select * from individual_contributions_v2 where STATE='MI';

USE FEC;
drop table if exists marshallislands_full_v2;
create table marshallislands_full_v2 like individual_contributions_v2;
insert into marshallislands_full_v2 select * from individual_contributions_v2 where STATE='MH';

USE FEC;
drop table if exists montana_full_v2;
create table montana_full_v2 like individual_contributions_v2;
insert into montana_full_v2 select * from individual_contributions_v2 where STATE='MT';

USE FEC;
drop table if exists northernmarianaislands_full_v2;
create table northernmarianaislands_full_v2 like individual_contributions_v2;
insert into northernmarianaislands_full_v2 select * from individual_contributions_v2 where STATE='MP';

USE FEC;
drop table if exists mississippi_full_v2;
create table mississippi_full_v2 like individual_contributions_v2;
insert into mississippi_full_v2 select * from individual_contributions_v2 where STATE='MS';

