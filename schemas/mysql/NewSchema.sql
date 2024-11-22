CREATE TABLE books(

id int NOT NULL,
book_id int,
best_book_id int,
work_id int,
books_count int,
isbn varchar(10),
isbn13 varchar(13),
authors varchar(100),
original_publication_year int,
original_title varchar(200),
title varchar(200),
language_code varchar(20),
average_rating int,
ratings_count int,
work_ratings_count int, 
work_text_reviews_count int,
ratings_1 int,
ratings_2 int,
ratings_3 int,
ratings_4 int,
ratings_5 int,
image_url varchar(255),
small_image_url varchar(255),
PRIMARY KEY (id)
);

CREATE TABLE tags(
tag_id int NOT NULL,
tag_name varchar(30),
PRIMARY KEY(tag_id)
);

CREATE TABLE reviews(
book_id int NOT NULL,
user_id int NOT NULL,
rating int NOT NULL
PRIMARY KEY (user_id, book_id),
FOREIGN KEY (book_id) REFERENCES books(book_id)

);

CREATE TABLE booktags(
goodreads_book_id int NOT NULL,
tag_id int NOT NULL,
count int,
PRIMARY KEY(goodreads_book_id,tag_id),
FOREIGN KEY (goodreads_book_id) REFERENCES books(book_id),
FOREIGN KEY (tag_id) REFERENCES tags(tag_id)
);

CREATE TABLE ToRead (
user_id int,
book_id int,
    PRIMARY KEY (user_id, book_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id), # idk about this!
    FOREIGN KEY (book_id) REFERENCES books(book_id)
);