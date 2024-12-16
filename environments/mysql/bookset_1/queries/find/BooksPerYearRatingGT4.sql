SELECT COUNT(tp.Title) AS TOTAL_BOOKS, tp.PublicationYear AS YEAR
FROM (SELECT b.Title, r.Rating, b.PublicationYear
FROM books b JOIN (
    SELECT BookId, Rating FROM ratings WHERE Rating >= 4
) r 
ON b.Id = r.BookId) tp
GROUP BY tp.PublicationYear
ORDER BY tp.PublicationYear ASC