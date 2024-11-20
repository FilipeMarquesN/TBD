SELECT * 
FROM BOOKS b 
WHERE b.publisher = 'Penguin Books' AND b.'Year-Of-Publication' > 2000;
-- note: year of publication can have a different name, all of these can and should have different names and we handle that in the dataframe