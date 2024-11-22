SELECT COUNT(id), AVG(original_publication_year), authors
FROM books b
GROUP BY b.authors;

SELECT * 
FROM BOOKS b 
WHERE b.language_code = 'eng' AND b.'Year-Of-Publication' > 2000;

SELECT *
FROM books b
WHERE b.average_rating <= 3 AND b.original_title LIKE '%Harry%'