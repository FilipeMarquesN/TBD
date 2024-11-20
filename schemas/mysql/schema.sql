-- Create for table books
CREATE TABLE IF NOT EXISTS Books (
    ISBN varchar(13) PRIMARY KEY, -- note for future. VARCHAR primary keys are VERY innefficient and bad
    Title varchar(255) NOT NULL ,
    Author varchar(255) NOT NULL ,
    YearOfPublication int,
    Publisher varchar(255),
    ImageSmall varchar(255),
    ImageMedium varchar(255),
    ImageLarge varchar(255)
);
-- Create for table  users
CREATE TABLE IF NOT EXISTS Users(
    ID int PRIMARY KEY,
    Locale varchar(255),
    Age int
);
-- Create for table  ratings
CREATE TABLE IF NOT EXISTS Ratings(
    User INT NOT NULL,
    ISBN VARCHAR(13) NOT NULL,
    Rating INT NOT NULL CHECK (Rating BETWEEN 0 AND 10),
    PRIMARY KEY (User, ISBN),
    FOREIGN KEY (User) REFERENCES users(ID),
    FOREIGN KEY (ISBN) REFERENCES books(ISBN)
);
