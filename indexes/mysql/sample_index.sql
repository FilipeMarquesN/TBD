CREATE INDEX books_ratings ON books (Id,Rating,Authors);
CREATE INDEX books_Rating ON ratings(Rating);

CREATE INDEX idx_rating ON books (Rating);
CREATE INDEX idx_authors_year ON books (Authors(191), PublicationYear);

