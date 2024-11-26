/*Write the speciations for two fairly complex data operations that
are able to showcase the differences between relational and NoSQL
databases 

This is a complex operation because it includes multiple
queries, includes write and read operations, and includes
heavy queries (sort by, group by, range queries)




Atualizar o livro adicionar a tag 01_best-books que tiver mais reviews = 4 */

UPDATE books /* adicionado */
SET tags = CONCAT(tags, ',01_best-books') /* adicionado */
WHERE id = ( /* adicionado */
	SELECT b.id, COUNT(r.ratings) AS totalRatings
	FROM books b
	JOIN ratings r ON b.id =r.book_id
	Where r.ratings = 4
	GROUP BY b.id
	ORDER BY totalRatings DESC 
	LIMIT 1;
); /* adicionado */