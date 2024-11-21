SELECT COUNT(ISBN), AVG(Year_Of_Publication), Publisher
FROM BOOKS b
GROUP BY b.Publisher;
