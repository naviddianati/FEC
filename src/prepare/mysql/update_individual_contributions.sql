-- Add id column to individual_contributions_v2
 ALTER TABLE individual_contributions_v2 add column id INT NOT NULL AUTO_INCREMENT FIRST, ADD primary KEY ID_INDEX(id);

