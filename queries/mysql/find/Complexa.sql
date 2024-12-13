WITH MaiorFa AS (
    SELECT 
        u.UserId, 
        b.id AS BookId, 
        COUNT(r.UserId) AS amount
    FROM users u
    JOIN ratings r ON u.UserId = r.UserId
    JOIN books b ON r.BookId = b.id
    WHERE b.id IN (
        SELECT BookId
        FROM books
        WHERE Title LIKE '%Harry Potter%'
    )
    GROUP BY u.UserId, b.id
    ORDER BY amount DESC
)
SELECT *
FROM MaiorFa
LIMIT 1;

INSERT INTO books (
    id, book_id, best_book_id, work_id, books_count, isbn, isbn13, authors, 
    original_publication_year, original_title, title, language_code, 
    average_rating, ratings_count, work_ratings_count, work_text_reviews_count, 
    ratings_1, ratings_2, ratings_3, ratings_4, ratings_5, image_url, small_image_url
)
SELECT 
    999999 AS id,                      
    999999 AS book_id,                 
    999999 AS best_book_id,            
    999999 AS work_id,                 
    100 AS books_count,                
    '999999' AS isbn,                  
    '9999999999999' AS isbn13,         
    mf.UserId AS authors,             
    2024 AS original_publication_year, 
    'Harry Potter Fan Book' AS original_title, 
    'Harry Potter Fan Book' AS title,          
    'eng' AS language_code,            
    0 AS average_rating,               
    0 AS ratings_count,                
    0 AS work_ratings_count,           
    0 AS work_text_reviews_count,      
    0 AS ratings_1,                    
    0 AS ratings_2,                    
    0 AS ratings_3,                    
    0 AS ratings_4,                    
    0 AS ratings_5,                    
    'None' AS image_url,               
    'None' AS small_image_url          
FROM 
    MaiorFa mf;
WHERE 
    RowNum = 1;