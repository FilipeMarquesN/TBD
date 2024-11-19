-- Create for table books
CREATE TABLE books (
    ISBN varchar(13) NOT NULL,
    Book-Title varchar(255) NOT NULL ,
    Book-Author varchar(255) NOT NULL ,
    Year-Of-Publication int,
    Publisher varchar(255),
    Image-URL-S varchar(255),
    Image-URL-M varchar(255),
    Image-URL-L varchar(255),
    PRIMARY KEY (ISBN)
);
-- Create for table  users
CREATE TABLE users(
    User-ID int NOT NULL,
    Location varchar(255),
    Age int,
    PRIMARY KEY (User-ID)
);
-- Create for table  ratings
CREATE TABLE ratings(
    User_ID INT NOT NULL,
    ISBN VARCHAR(13) NOT NULL,
    Book_Rating INT NOT NULL CHECK (Book_Rating BETWEEN 0 AND 10),
    PRIMARY KEY (User_ID, ISBN),
    FOREIGN KEY (User_ID) REFERENCES users(User_ID),
    FOREIGN KEY (ISBN) REFERENCES books(ISBN)
);
