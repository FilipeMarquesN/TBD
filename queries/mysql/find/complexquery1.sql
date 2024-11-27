query1 =
[
INSERT INTO books (
    Id, 
    BookId, 
    BestBook, 
    BookTitleId, 
    BooksCount, 
    Isbn, 
    Isbn13, 
    Authors, 
    PublicationYear, 
    OriginalTitle, 
    Title, 
    LanguageCode, 
    Rating, 
    RatingCount, 
    BookTitleRatingCount, 
    BookTitleReviewsCount, 
    Ratings1, 
    Ratings2, 
    Ratings3, 
    Ratings4, 
    Ratings5, 
    ImageURL, 
    SmallImageURL
) VALUES (
    10001, 
    204929010, 
    204929010, 
    204927599, 
    1, 
    9789897878, 
    9788535937817, 
    'Yuval Noah Harari', 
    2024, 
    'Nexus', 
    'Nexus', 
    'eng', 
    5.00, 
    10000, 
    10000, 
    10000, 
    0, 
    0, 
    0, 
    500, 
    1000, 
    'http://example.com/nexus-cover.jpg', 
    'http://example.com/small-nexus-cover.jpg'
)
]

query2 =
[
    SELECT * 
    FROM books
    ORDER BY Rating DESC
    LIMIT 10;
]