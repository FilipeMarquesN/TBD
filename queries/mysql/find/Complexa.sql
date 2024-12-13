INSERT INTO books (Id, BookId, BestBook, BookTitleId, BooksCount, Isbn, Isbn13, Authors, PublicationYear, OriginalTitle, Title, LanguageCode, Rating, 
    RatingCount, BookTitleRatingCount, BookTitleReviewsCount, Ratings1, Ratings2, Ratings3, Ratings4, Ratings5, ImageURL, SmallImageURL
)
SELECT 
    999999 AS Id,                       
    999999 AS BookId,                   
    999999 AS BestBook,                 
    999999 AS BookTitleId,              
    100 AS BooksCount,                  
    '999999' AS Isbn,                   
    '9999999999999' AS Isbn13,          
    CONCAT(mf.UserName, ' ', mf.UserSurname) AS Authors, 
    2024 AS PublicationYear,            
    'Harry Potter Fan Book' AS OriginalTitle, 
    'Harry Potter Fan Book' AS Title,          
    'eng' AS LanguageCode,              
    0 AS Rating,                        
    0 AS RatingCount,                   
    0 AS BookTitleRatingCount,          
    0 AS BookTitleReviewsCount,         
    0 AS Ratings1,                      
    0 AS Ratings2,                      
    0 AS Ratings3,                      
    0 AS Ratings4,                      
    0 AS Ratings5,                      
    'None' AS ImageURL,                 
    'None' AS SmallImageURL             
FROM 
    (SELECT 
        u.UserId, 
        u.UserName,
        u.UserSurname,
        COUNT(r.UserId) AS amount
     FROM users u
     JOIN ratings r ON u.UserId = r.UserId
     JOIN books b ON r.BookId = b.id
     WHERE b.Title LIKE '%Harry Potter%'
     GROUP BY u.UserId, u.UserName, u.UserSurname
     ORDER BY amount DESC
     LIMIT 1
    ) AS mf;
