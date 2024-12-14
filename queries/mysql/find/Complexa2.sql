INSERT INTO authorStatus(Authors, TotalBooks, AvgRating, YearWithMostBooks, BooksInYearWithMost)
SELECT 
    b.Authors,
    COUNT(*) AS TotalBooks,
    AVG(r.Rating) AS AvgRating, 
    (SELECT PublicationYear
     FROM books b2
     WHERE b2.Authors = b.Authors
     GROUP BY b2.PublicationYear
     ORDER BY COUNT(*) DESC
     LIMIT 1
    ) AS YearWithMostBooks,
    (SELECT COUNT(*)
     FROM books b3
     WHERE b3.Authors = b.Authors AND b3.PublicationYear = 
      (SELECT PublicationYear
       FROM books b2
       WHERE b2.Authors = b.Authors
       GROUP BY b2.PublicationYear
       ORDER BY COUNT(*) DESC
       LIMIT 1
             )
    ) AS BooksInYearWithMost
FROM books b
JOIN ratings r ON b.Id = r.BookId  
GROUP BY b.Authors
ORDER BY AvgRating DESC
LIMIT 10;
