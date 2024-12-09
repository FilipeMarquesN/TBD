CREATE TABLE IF NOT EXISTS books(
    Id int PRIMARY KEY,
    BookId int UNIQUE, -- Unique: candidate Key
    BestBook int UNIQUE, -- Unique: candidate Key
    BookTitleId int,
    BooksCount int,
    Isbn varchar(10), -- Should be unique because candidate key but isn't because of null values
    Isbn13 varchar(13), -- Should be unique because candidate key but isn't because of null values
    Authors varchar(1000), -- Line 6202 books.csv
    PublicationYear int,
    OriginalTitle varchar(255),
    Title varchar(255),
    LanguageCode varchar(20),
    Rating float(2),
    RatingCount int,
    BookTitleRatingCount int, 
    BookTitleReviewsCount int,
    Ratings1 int,
    Ratings2 int,
    Ratings3 int,
    Ratings4 int,
    Ratings5 int,
    ImageURL varchar(255),
    SmallImageURL varchar(255)
);

CREATE TABLE IF NOT EXISTS users (
    UserId int,
    PRIMARY KEY(UserId)
);

CREATE TABLE IF NOT EXISTS tags(
    Id int PRIMARY KEY,
    TagName varchar(255)
);

CREATE TABLE IF NOT EXISTS ratings(
    BookId int NOT NULL,
    UserId int NOT NULL,
    Rating DECIMAL(2,1) NOT NULL,
    PRIMARY KEY (UserId, BookId),
    FOREIGN KEY (BookId) REFERENCES books(Id),
    FOREIGH KEY (UserId) REFERENCES users(UserId)
);

CREATE TABLE IF NOT EXISTS book_tags(
    GoodreadsBookId int NOT NULL,
    TagId int NOT NULL,
    Count int,
    PRIMARY KEY(GoodreadsBookId,TagId),
    FOREIGN KEY (GoodreadsBookId) REFERENCES books(BookId),
    FOREIGN KEY (TagId) REFERENCES tags(Id)
);

CREATE TABLE IF NOT EXISTS to_read (
    UserId int,
    BookId int,
    PRIMARY KEY (UserId, BookId),
    FOREIGN KEY (BookId) REFERENCES books(Id),
    FOREIGH KEY (UserId) REFERENCES users(UserId)
);

