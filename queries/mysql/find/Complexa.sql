WITH MaiorFa AS (
    SELECT u.UserId, b.id AS BookId, COUNT(r.UserId) AS ammount
    FROM users u
    JOIN rating r ON u.UserId = r.UserId
    JOIN books b ON r.BookId = b.id
    WHERE b.id IN (
            SELECT BookId 
            FROM Titles 
            WHERE Title LIKE '%Harry Potter%'
        )
    GROUP BY u.UserId, b.id
    ORDER BY ammount
    LIMIT 1

)

INSERT INTO books (
    id, book_id, best_book_id, work_id, books_count, isbn, isbn13, authors, original_publication_year,
    original_title, title, language_code, average_rating, ratings_count, work_ratings_count, 
    work_text_reviews_count, ratings_1, ratings_2, ratings_3, ratings_4, ratings_5, image_url, small_image_url
)
SELECT 
    1 AS id,                       
    b.id AS book_id,               
    b.best_book_id,                
    b.work_id,                     
    100 AS books_count,            
    b.isbn,                        
    b.isbn13,                      
    b.authors,                     
    b.original_publication_year,   
    b.original_title,              
    b.title,                       
    b.language_code,               
    b.average_rating,              
    b.ratings_count,               
    b.work_ratings_count,         
    b.work_text_reviews_count,    
    b.ratings_1,                   
    b.ratings_2,                   
    b.ratings_3,                   
    b.ratings_4,                   
    b.ratings_5,                   
    b.image_url,                   
    b.small_image_url             
FROM 
    books b
JOIN 
    MaiorFa mf ON b.id = mf.UserId;

